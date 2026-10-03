//! Certificates the user chose to trust for FTPS servers the OS trust store
//! rejects — self-signed certs on NAS boxes and shared hosts, mostly. The SSH
//! equivalent is `known_hosts`; X.509 has no standard per-user file, so pins
//! live in `trusted_certs.json` in Faro's data dir as `"host:port" ->
//! SHA-256 fingerprint`.

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

/// Serialises read-modify-write of the file across concurrent connects.
static LOCK: Mutex<()> = Mutex::new(());

/// Same directory the GUI (`app_data_dir`) and the CLI resolve to.
fn path() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("FARO_DATA_DIR") {
        if !dir.is_empty() {
            return Some(PathBuf::from(dir).join("trusted_certs.json"));
        }
    }
    Some(dirs::data_dir()?.join("com.juandenis.faro").join("trusted_certs.json"))
}

fn key(host: &str, port: u16) -> String {
    format!("{}:{port}", host.to_ascii_lowercase())
}

fn load() -> BTreeMap<String, String> {
    path()
        .and_then(|p| std::fs::read(p).ok())
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

/// The fingerprint previously trusted for `host:port`, if any.
pub fn lookup(host: &str, port: u16) -> Option<String> {
    let _g = LOCK.lock().ok()?;
    load().remove(&key(host, port))
}

/// Remember `fingerprint` as trusted for `host:port` (replacing any old one).
pub fn trust(host: &str, port: u16, fingerprint: &str) -> Result<()> {
    let _g = LOCK.lock().map_err(|_| anyhow::anyhow!("trust store lock poisoned"))?;
    let path = path().context("resolving the data dir")?;
    let mut all = load();
    all.insert(key(host, port), fingerprint.to_string());
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    std::fs::write(&path, serde_json::to_vec_pretty(&all)?)
        .with_context(|| format!("writing {}", path.display()))
}

/// SHA-256 of a DER certificate as colon-separated hex — the form browsers
/// and FileZilla show, so users can compare it against what the host lists.
pub fn fingerprint(der: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let hex: Vec<String> = Sha256::digest(der).iter().map(|b| format!("{b:02X}")).collect();
    format!("SHA256:{}", hex.join(":"))
}
