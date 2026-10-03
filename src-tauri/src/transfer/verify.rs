//! Checksum verification of finished transfers (Plan 24 Phase 6), behind
//! the `transferVerify` setting.
//!
//! Size and range coverage are always checked elsewhere. With verification
//! on, a backend that can produce a checksum is asked for one:
//! - SFTP: `sha256sum` (or `shasum -a 256`) over an exec channel, against a
//!   SHA-256 computed locally (for downloads, by the writer as bytes land).
//! - S3-style object stores: the upload's ETag against the local MD5 (a
//!   single PUT) or the `md5-of-md5s-N` form (multipart).
//! - Faro Agent: the daemon's BLAKE3 whole-file hash.
//!
//! Backends without one keep the size check only.

use crate::session::{AgentSession, SshSession};
use anyhow::Result;
use md5::{Digest as _, Md5};
use std::path::Path;
use std::sync::Arc;

/// The finished file doesn't match the server's checksum. Never retried;
/// a download's temp is kept for inspection and never moved into place.
#[derive(Debug)]
pub struct VerifyFailed(pub String);

impl std::fmt::Display for VerifyFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "verification failed: {}", self.0)
    }
}

impl std::error::Error for VerifyFailed {}

pub fn mismatch(what: &str, local: &str, remote: &str) -> anyhow::Error {
    anyhow::Error::new(VerifyFailed(format!(
        "{what} differs (local {local}, server {remote})"
    )))
}

/// `'it''s'`-style single quoting for a POSIX shell.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// SHA-256 of a remote file via an exec channel, or `None` when the host
/// can't run either tool (no exec, BusyBox without them, Windows).
pub async fn remote_sha256(ssh: &Arc<SshSession>, path: &str) -> Option<String> {
    let q = shell_quote(path);
    let cmd = format!("sha256sum -- {q} 2>/dev/null || shasum -a 256 -- {q} 2>/dev/null");
    let out = ssh.exec(&cmd).await.ok()?;
    let hash = out.stdout.split_whitespace().next()?.to_ascii_lowercase();
    (hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit())).then_some(hash)
}

/// SHA-256 of a local file (hex), read on the blocking pool.
pub async fn local_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let p = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<String> {
        let mut h = Sha256::new();
        let mut f = std::fs::File::open(&p)?;
        std::io::copy(&mut f, &mut h)?;
        Ok(hex(&h.finalize()))
    })
    .await?
}

/// The daemon's BLAKE3 whole-file hash of `path`.
pub async fn agent_hash(session: &Arc<AgentSession>, path: &str) -> Result<String> {
    use faro_agent_proto::msg::{Request, Response};
    match session
        .request(Request::Signature {
            path: path.to_string(),
        })
        .await?
    {
        Response::Signature { whole_hash, .. } => Ok(whole_hash),
        Response::Error { message, .. } => anyhow::bail!("signature {path}: {message}"),
        other => anyhow::bail!("signature {path}: unexpected {other:?}"),
    }
}

/// The same BLAKE3 whole-file hash, of a local file.
pub async fn local_agent_hash(path: &Path) -> Result<String> {
    let p = path.to_path_buf();
    tokio::task::spawn_blocking(move || faro_agent_proto::delta::signature_of_file(&p))
        .await?
        .map(|s| s.whole_hash)
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn md5(data: &[u8]) -> [u8; 16] {
    Md5::digest(data).into()
}

/// The ETag S3 gives a multipart upload: MD5 of the parts' MD5s, `-N`.
pub fn multipart_etag(parts: &[[u8; 16]]) -> String {
    let mut h = Md5::new();
    for p in parts {
        h.update(p);
    }
    format!("{}-{}", hex(&h.finalize()), parts.len())
}

/// Compare an object-store ETag with what we expect, when the ETag is the
/// S3 MD5 form at all (32 hex digits, optionally `-N`). Other stores (Azure,
/// GCS, encrypted S3 objects) use opaque ETags: `None` = can't tell.
pub fn etag_matches(etag: &str, expected: &str) -> Option<bool> {
    let etag = etag.trim().trim_matches('"').to_ascii_lowercase();
    let (hash, parts) = match etag.split_once('-') {
        Some((h, n)) => (h, Some(n)),
        None => (etag.as_str(), None),
    };
    let md5_shaped = hash.len() == 32
        && hash.bytes().all(|b| b.is_ascii_hexdigit())
        && parts.is_none_or(|n| n.bytes().all(|b| b.is_ascii_digit()));
    md5_shaped.then(|| etag == expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_etag_forms() {
        let a = md5(b"hello");
        assert_eq!(hex(&a), "5d41402abc4b2a76b9719d911017c592");
        assert_eq!(etag_matches("\"5d41402abc4b2a76b9719d911017c592\"", &hex(&a)), Some(true));
        assert_eq!(etag_matches("\"00000000000000000000000000000000\"", &hex(&a)), Some(false));
        // Multipart: md5 of the concatenated part digests, then "-N".
        let parts = [md5(b"part one"), md5(b"part two")];
        let want = multipart_etag(&parts);
        assert!(want.ends_with("-2"));
        assert_eq!(etag_matches(&format!("\"{want}\""), &want), Some(true));
        // Azure / GCS style opaque tags can't be checked.
        assert_eq!(etag_matches("\"0x8DC2F0B5E5C1A4B\"", &want), None);
    }

    #[test]
    fn quoting_survives_awkward_names() {
        assert_eq!(shell_quote("/a b/it's.txt"), r"'/a b/it'\''s.txt'");
    }
}
