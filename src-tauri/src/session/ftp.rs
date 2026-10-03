use crate::profiles::{AuthMethod, ConnectionProfile};
use anyhow::{anyhow, Context, Result};
use encoding_rs::{Encoding, UTF_8, WINDOWS_1252};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use suppaftp::native_tls::TlsConnector;
use suppaftp::{FtpStream, NativeTlsConnector, NativeTlsFtpStream, TextCodec};

/// One FTP control connection. suppaftp is synchronous; we wrap it in a
/// `std::sync::Mutex` and route every operation through `spawn_blocking` so
/// it cannot block tokio's runtime threads. Data transfers go through the
/// same stream (FTP has no native multiplexing — operations serialise on the
/// control connection by design).
pub struct FtpSession {
    pub id: String,
    pub profile: ConnectionProfile,
    inner: Arc<StdMutex<FtpStreamKind>>,
    /// The server advertised `MLST` in FEAT, so `MLSD` listings are available.
    mlsd: bool,
}

/// suppaftp ships two separate stream types depending on whether TLS is
/// involved. We keep them in an enum so all callsites can speak to either.
pub enum FtpStreamKind {
    Plain(FtpStream),
    Tls(NativeTlsFtpStream),
}

impl FtpStreamKind {
    pub fn mlsd(&mut self, path: &str) -> Result<Vec<String>> {
        match self {
            Self::Plain(s) => s.mlsd(Some(path)).map_err(into_anyhow),
            Self::Tls(s) => s.mlsd(Some(path)).map_err(into_anyhow),
        }
    }
    pub fn list(&mut self, path: Option<&str>) -> Result<Vec<String>> {
        match self {
            Self::Plain(s) => s.list(path).map_err(into_anyhow),
            Self::Tls(s) => s.list(path).map_err(into_anyhow),
        }
    }
    pub fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        match self {
            Self::Plain(s) => s.rename(from, to).map_err(into_anyhow),
            Self::Tls(s) => s.rename(from, to).map_err(into_anyhow),
        }
    }
    pub fn rm(&mut self, path: &str) -> Result<()> {
        match self {
            Self::Plain(s) => s.rm(path).map_err(into_anyhow),
            Self::Tls(s) => s.rm(path).map_err(into_anyhow),
        }
    }
    pub fn rmdir(&mut self, path: &str) -> Result<()> {
        match self {
            Self::Plain(s) => s.rmdir(path).map_err(into_anyhow),
            Self::Tls(s) => s.rmdir(path).map_err(into_anyhow),
        }
    }
    pub fn mkdir(&mut self, path: &str) -> Result<()> {
        match self {
            Self::Plain(s) => s.mkdir(path).map_err(into_anyhow),
            Self::Tls(s) => s.mkdir(path).map_err(into_anyhow),
        }
    }
    pub fn site(&mut self, cmd: &str) -> Result<()> {
        match self {
            Self::Plain(s) => s.site(cmd).map(|_| ()).map_err(into_anyhow),
            Self::Tls(s) => s.site(cmd).map(|_| ()).map_err(into_anyhow),
        }
    }
    pub fn size(&mut self, path: &str) -> Result<usize> {
        match self {
            Self::Plain(s) => s.size(path).map_err(into_anyhow),
            Self::Tls(s) => s.size(path).map_err(into_anyhow),
        }
    }
    pub fn retr_to_writer<W: std::io::Write>(
        &mut self,
        path: &str,
        mut sink: W,
    ) -> Result<u64> {
        match self {
            Self::Plain(s) => s
                .retr(path, |r| std::io::copy(r, &mut sink).map_err(suppaftp::FtpError::ConnectionError))
                .map_err(into_anyhow),
            Self::Tls(s) => s
                .retr(path, |r| std::io::copy(r, &mut sink).map_err(suppaftp::FtpError::ConnectionError))
                .map_err(into_anyhow),
        }
    }
    pub fn put_from_reader<R: std::io::Read>(
        &mut self,
        path: &str,
        reader: &mut R,
    ) -> Result<u64> {
        match self {
            Self::Plain(s) => s.put_file(path, reader).map_err(into_anyhow),
            Self::Tls(s) => s.put_file(path, reader).map_err(into_anyhow),
        }
    }
    pub fn set_text_codec(&mut self, codec: Option<TextCodec>) {
        match self {
            Self::Plain(s) => s.set_text_codec(codec),
            Self::Tls(s) => s.set_text_codec(codec),
        }
    }
    /// The FEAT keywords the server advertises, upper-cased. Empty when the
    /// server doesn't implement FEAT.
    fn features(&mut self) -> Vec<String> {
        let feat = match self {
            Self::Plain(s) => s.feat(),
            Self::Tls(s) => s.feat(),
        };
        feat.map(|f| f.keys().map(|k| k.trim().to_ascii_uppercase()).collect())
            .unwrap_or_default()
    }
    /// `OPTS UTF8 ON`. Servers like IIS and FileZilla Server only switch path
    /// names to UTF-8 once the client asks; errors mean "not supported" and
    /// are ignored.
    fn opts_utf8_on(&mut self) {
        let _ = match self {
            Self::Plain(s) => s.opts("UTF8", Some("ON")),
            Self::Tls(s) => s.opts("UTF8", Some("ON")),
        };
    }
    pub fn quit(&mut self) {
        match self {
            Self::Plain(s) => {
                let _ = s.quit();
            }
            Self::Tls(s) => {
                let _ = s.quit();
            }
        }
    }
}

fn into_anyhow(e: suppaftp::FtpError) -> anyhow::Error {
    anyhow!(e.to_string())
}

/// The character set a profile asked for (`ftpEncoding`).
enum Charset {
    /// Unset / "auto": negotiate UTF-8, but fall back to Windows-1252 the
    /// first time the server sends bytes that aren't valid UTF-8.
    Auto,
    /// "utf-8": always UTF-8 (suppaftp's native behaviour).
    Utf8,
    /// Any other WHATWG label ("windows-1252", "shift_jis", "gbk", ...).
    Fixed(&'static Encoding),
}

fn resolve_charset(setting: Option<&str>) -> Result<Charset> {
    let label = setting.map(str::trim).unwrap_or("");
    if label.is_empty() || label.eq_ignore_ascii_case("auto") {
        return Ok(Charset::Auto);
    }
    let enc = Encoding::for_label(label.as_bytes())
        .ok_or_else(|| anyhow!("Unknown FTP character set \"{label}\""))?;
    if enc == UTF_8 {
        Ok(Charset::Utf8)
    } else if enc.output_encoding() != enc {
        // UTF-16 and "replacement" can't be encoded to; neither is used on an
        // FTP control channel anyway.
        Err(anyhow!("FTP character set \"{label}\" is not supported"))
    } else {
        Ok(Charset::Fixed(enc))
    }
}

fn fixed_codec(enc: &'static Encoding) -> TextCodec {
    TextCodec::new(
        move |text| enc.encode(text).0.into_owned(),
        move |bytes| enc.decode_without_bom_handling(bytes).0.into_owned(),
    )
}

/// UTF-8 until the server proves otherwise. Once a reply or listing line
/// isn't valid UTF-8 the server is using a legacy codepage, so latch to
/// Windows-1252 for the rest of the session — in both directions, so a name
/// read from a listing is sent back as the exact bytes the server gave us.
/// (Without this, the name round-trips as U+FFFD, the server can't find the
/// path, and many servers answer `LIST` with an empty listing.) Windows-1252
/// maps every byte, so even a Shift_JIS or GBK server stays navigable; the
/// names just look wrong until the user picks the right charset.
fn auto_codec(host: String) -> TextCodec {
    let legacy = Arc::new(AtomicBool::new(false));
    let legacy_enc = legacy.clone();
    TextCodec::new(
        move |text| {
            if legacy_enc.load(Ordering::Relaxed) {
                WINDOWS_1252.encode(text).0.into_owned()
            } else {
                text.as_bytes().to_vec()
            }
        },
        move |bytes| {
            if !legacy.load(Ordering::Relaxed) {
                match std::str::from_utf8(bytes) {
                    Ok(s) => return s.to_string(),
                    Err(_) => {
                        legacy.store(true, Ordering::Relaxed);
                        tracing::warn!(
                            "FTP {host}: server sent non-UTF-8 text; falling back to                              Windows-1252 (set a character set on the connection to override)"
                        );
                    }
                }
            }
            WINDOWS_1252.decode_without_bom_handling(bytes).0.into_owned()
        },
    )
}

impl FtpSession {
    pub fn supports_mlsd(&self) -> bool {
        self.mlsd
    }

    /// Run a closure with mutable access to the underlying FTP stream on a
    /// blocking thread. Use this for any FTP operation — it ensures the
    /// blocking syscalls don't pin a tokio worker.
    pub async fn with_stream<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut FtpStreamKind) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut g = inner.lock().map_err(|_| anyhow!("FTP stream lock poisoned"))?;
            f(&mut g)
        })
        .await
        .map_err(|e| anyhow!("FTP task join failed: {e}"))?
    }
}

pub async fn ftp_connect(profile: &ConnectionProfile) -> Result<FtpSession> {
    let host = profile.host.clone();
    let port = profile.port;
    let username = profile.username.clone();
    let password = match &profile.auth {
        AuthMethod::Password { password } => password.clone(),
        AuthMethod::Key { .. } => {
            return Err(anyhow!(
                "FTP does not support private-key auth; switch to password"
            ))
        }
        AuthMethod::Agent => {
            return Err(anyhow!(
                "FTP does not support ssh-agent auth; switch to password"
            ))
        }
        AuthMethod::KeyRef { .. } => {
            return Err(anyhow!(
                "FTP does not support keychain key auth; switch to password"
            ))
        }
    };
    let want_tls = profile.protocol.eq_ignore_ascii_case("ftps");
    let charset = resolve_charset(profile.ftp_encoding.as_deref())?;

    let id = uuid::Uuid::new_v4().to_string();
    let host_for_blocking = host.clone();
    let (stream, mlsd) = tokio::task::spawn_blocking(move || -> Result<(FtpStreamKind, bool)> {
        let addr = format!("{host_for_blocking}:{port}");
        if want_tls {
            // Explicit FTPS: connect as a NativeTlsFtpStream-typed stream
            // (still plain TCP at this point), then issue AUTH TLS via
            // into_secure. NativeTlsFtpStream and FtpStream are different
            // generic instantiations, so the type has to be picked up front.
            let s = NativeTlsFtpStream::connect(&addr)
                .with_context(|| format!("FTP connect {addr}"))?;
            let tls_connector = TlsConnector::new()
                .map_err(|e| anyhow!("TLS init: {e}"))?;
            let secured = s
                .into_secure(
                    NativeTlsConnector::from(tls_connector),
                    &host_for_blocking,
                )
                .map_err(|e| anyhow!("FTPS AUTH TLS: {e}"))?;
            let mut tls = FtpStreamKind::Tls(secured);
            let mlsd = login(&mut tls, &username, &password, &charset, &host_for_blocking)?;
            Ok((tls, mlsd))
        } else {
            let s = FtpStream::connect(&addr)
                .with_context(|| format!("FTP connect {addr}"))?;
            let mut plain = FtpStreamKind::Plain(s);
            let mlsd = login(&mut plain, &username, &password, &charset, &host_for_blocking)?;
            Ok((plain, mlsd))
        }
    })
    .await
    .map_err(|e| anyhow!("FTP connect task: {e}"))??;

    Ok(FtpSession {
        id,
        profile: profile.clone(),
        inner: Arc::new(StdMutex::new(stream)),
        mlsd,
    })
}

/// Log in, then settle the connection's character set. Returns whether the
/// server supports MLSD.
fn login(
    stream: &mut FtpStreamKind,
    user: &str,
    password: &str,
    charset: &Charset,
    host: &str,
) -> Result<bool> {
    // A fixed codepage applies to USER/PASS too; auto and UTF-8 send them as
    // UTF-8, which is identical for the ASCII credentials nearly everyone uses.
    if let Charset::Fixed(enc) = charset {
        stream.set_text_codec(Some(fixed_codec(enc)));
    }
    match stream {
        FtpStreamKind::Plain(s) => s.login(user, password).map_err(into_anyhow)?,
        FtpStreamKind::Tls(s) => s.login(user, password).map_err(into_anyhow)?,
    }
    let features = stream.features();
    let has = |f: &str| features.iter().any(|k| k == f);
    match charset {
        Charset::Fixed(_) => {}
        Charset::Utf8 => stream.opts_utf8_on(),
        Charset::Auto => {
            if has("UTF8") {
                stream.opts_utf8_on();
            }
            stream.set_text_codec(Some(auto_codec(host.to_string())));
        }
    }
    Ok(has("MLST"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_charset_labels() {
        assert!(matches!(resolve_charset(None).unwrap(), Charset::Auto));
        assert!(matches!(resolve_charset(Some(" Auto ")).unwrap(), Charset::Auto));
        assert!(matches!(resolve_charset(Some("UTF-8")).unwrap(), Charset::Utf8));
        assert!(matches!(
            resolve_charset(Some("ISO-8859-1")).unwrap(),
            Charset::Fixed(e) if e == WINDOWS_1252
        ));
        assert!(resolve_charset(Some("utf-16le")).is_err());
        assert!(resolve_charset(Some("klingon")).is_err());
    }

    #[test]
    fn fixed_codec_round_trips_bytes() {
        let sjis = Encoding::for_label(b"shift_jis").unwrap();
        let codec = fixed_codec(sjis);
        let wire = sjis.encode("CWD /資料\r\n").0.into_owned();
        assert_eq!(codec_decode(&codec, &wire), "CWD /資料\r\n");
    }

    #[test]
    fn auto_codec_latches_to_windows_1252() {
        let codec = auto_codec("test".into());
        // Valid UTF-8 stays UTF-8, and commands go out as UTF-8.
        assert_eq!(codec_decode(&codec, "café".as_bytes()), "café");
        assert_eq!(codec_encode(&codec, "é"), "é".as_bytes());
        // A Latin-1 listing line flips the session to Windows-1252...
        let line = b"-rw-r--r-- 1 ftp ftp 3 Jan 1 2024 caf\xe9.txt";
        assert!(codec_decode(&codec, line).ends_with("café.txt"));
        // ...so the same name is sent back as the server's original bytes.
        assert_eq!(codec_encode(&codec, "café.txt"), b"caf\xe9.txt");
    }

    fn codec_decode(codec: &TextCodec, bytes: &[u8]) -> String {
        codec.decode_text(bytes)
    }
    fn codec_encode(codec: &TextCodec, text: &str) -> Vec<u8> {
        codec.encode_text(text)
    }
}
