# Vendored suppaftp 6.3.0 (patched)

Upstream: https://github.com/veeso/suppaftp — MIT OR Apache-2.0, by
Christian Visintin. Wired in through `[patch.crates-io]` in
`src-tauri/Cargo.toml`.

## Why

suppaftp writes every command as UTF-8 and decodes listings with
`from_utf8_lossy`. Against a server that uses another charset (Latin-1,
Windows-1252, Shift_JIS, GBK, ...) any non-ASCII name comes back mangled,
and the mangled name gets sent back on the next command. The server then
can't find the path, and many servers answer `LIST` on a missing path
with an empty listing (Faro issue #24). No upstream release, up to 12.x,
has a hook for this.

## The patch (sync client only)

- `types::TextCodec`: an encoder/decoder pair (`encode_text`,
  `decode_text`), exported from the crate root.
- `ImplFtpStream::set_text_codec(Option<TextCodec>)`: when set, `perform`
  encodes each command with it, and `LIST`/`NLST`/`MLSD` lines, `PWD` and
  `MLST` replies are decoded with it. The codec carries over through
  `into_secure`.

Also fixed: in active mode the accepted data socket is switched back to
blocking. On Windows it inherits the listener's non-blocking mode, so every
active-mode transfer failed with WouldBlock (surfacing as `BadResponse`).

With no codec set, behaviour matches upstream byte for byte. To move to a
newer suppaftp, re-apply these changes or drop the patch once upstream
grows an equivalent.
