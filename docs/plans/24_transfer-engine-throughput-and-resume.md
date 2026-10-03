# Plan 24 — Transfer engine: throughput, safe writes, resume, integrity

## Context

Plan 17 gave the `TransferManager` (`src-tauri/src/transfer.rs`) a real
queue, pause, retry and a throttle. The byte paths underneath are still the
first-version ones, and on large files or high-latency links they lose
throughput and data:

- **SFTP downloads use one read at a time.** `run_ssh_download` reads 64 KiB at
  a time through russh-sftp's `AsyncRead`. Its `poll_read`
  (`russh-sftp-2.3.0/src/client/fs/file.rs:146`) sends one READ request and
  waits for the reply before sending the next. That caps throughput at
  `64 KiB / RTT`, about 640 KB/s on a 100 ms link however fast the line is.
  Uploads already pipeline: up to 8 writes are in flight (`max_concurrent_writes`).
- **Object stores (S3, R2, B2, GCS, Azure) run one stream at a time.**
  `run_object_download` streams a single `get()`. `run_object_upload` awaits
  each 8 MiB `put_part` one after another; the comment there about
  "parallelism" is wrong. A failed multipart upload is never `abort()`ed, so
  orphaned parts keep costing storage.
- **The object_store timeout covers the whole response body.** No
  `ClientOptions` is set (`session/object.rs`), so the crate's default
  **30 s request timeout** applies. Per `object_store-0.11.2/src/client/mod.rs:115`
  it runs "until the response body has finished". So any single GET or part
  that takes more than 30 s fails, and auto-retry then starts it again from
  byte 0.
- **No temp file.** Downloads call `File::create` on the final path. With the
  Overwrite option the user's existing file is truncated before the first byte
  arrives, and a failed or canceled transfer leaves a truncated file behind.
  Nothing is preallocated, and nothing calls `sync_all`.
- **Resume works only for FTP, isn't saved, and isn't validated.**
  `Transfer.resumable` is an in-memory FTP-only flag. On every other backend,
  pause, auto-retry and manual retry all restart from 0
  (`RestartFromPause`). Nothing survives an app restart.
- **A short transfer is reported as complete.** `finalize` sets
  `transferred = size` on success, and no backend checks the bytes it wrote
  against the expected size.
- **A paused transfer keeps its concurrency slot.** `checkpoint` parks the task
  while it still holds the semaphore permit, so pausing three transfers stalls
  the whole queue (default concurrency 3).
- **No speed or ETA anywhere.** Every transfer emits its own progress event
  every 100 ms, with no batching across transfers.
- **FTP runs one operation at a time per session.** One `StdMutex` per
  session covers every operation, including the whole data copy, so a
  download blocks directory browsing on that connection.

### Techniques this plan adopts

| Technique | Phase |
|---|---|
| Write every range into **one preallocated file at its offset**, no segment files and no merge step | 2 |
| **Halve the largest remaining range** when a worker goes idle; the original owner sees a shared `end` offset shrink and stops there | 3 |
| **Minimum split ≈ 6 s of per-worker throughput** (`max(floor, speed × 6)`) | 3 |
| **Ramp-up:** start with 1 connection, add one every 0.5 s, and on an error drop one and wait longer before adding again | 3 |
| **Auto-tune the connection count:** add a stream; keep it only if throughput rises > 10 % over an 8 s window | 3 |
| **Resume state written after the data:** write data → fsync → then record progress | 4 |
| **Resume validated against the remote file** (size + ETag/mtime) so two versions are never stitched together | 4 |
| **A retry budget that resets whenever bytes move**, exponential backoff with jitter, respect `Retry-After` | 5 |
| **Stall watchdog:** flag "not responding" after a few seconds, abort and re-run the range after ~20 s; drop slow connections near the end of a file | 5 |
| **Checksum verification** where the backend can provide one | 6 |
| **Atomic byte counters and a moving-average speed** | 3, 7 |

## Scope

**In:** SFTP read pipelining; parallel ranged downloads and parallel
multipart uploads on object stores; a shared ranged-download driver with
work stealing; `.faro-part` files with atomic finalize; resume on every
backend that supports byte ranges, saved in `faro.db` and validated against
the remote file; better retry and stall handling; completion and size checks
plus an optional hash check; speed and ETA; pause releasing its slot; a
separate FTP connection per transfer.

**Out (follow-ups):** chunked or session uploads for Dropbox, Google Drive and
Box (these load the whole file into memory; Dropbox refuses files over
150 MB); parallel uploads for OneDrive upload sessions; a parallel version of
Faro Agent `ReadChunk` (delta sync already covers its biggest case); limits per
host or per connection beyond FTP; scheduled transfers.

## Approach

### Phase 1 — Quick fixes (small, ship first)

- **object_store timeouts.** Build every object store (`s3_connect`,
  `azure_connect`, GCS) with `ClientOptions`:
  - keep a connect timeout of about 10 s;
  - replace the whole-request timeout with no timeout (`with_timeout_disabled()`);
  - rely on the stall watchdog (Phase 5) to catch connections that hang.
  - Leave object_store's own `RetryConfig` at its defaults for request
    failures. It does not retry a body that breaks mid-stream; our range
    retry handles that.
- **Abort orphaned multipart uploads.** On any error or cancel, call
  `upload.abort()` (best effort, logged).
- **Check the size before reporting Done.** Every backend arm returns the
  bytes it actually wrote. `finalize` marks Done only when that equals the
  expected size; it stops forcing `transferred = size`. The exception is a
  size unknown at enqueue (`remote_size` failed, so size is 0): then the
  written count becomes the size.
- **Pause releases the concurrency slot.** `checkpoint` returns a `Paused`
  error instead of parking. The runner drops the permit, waits on the pause
  gate, then calls `admit()` again to rejoin the queue. Until Phase 4 the
  transfer still restarts from 0 on resume, except FTP, which keeps resuming as
  it does today.

### Phase 2 — Safe local writes (`PartFile`)

A shared helper in `src-tauri/src/transfer/partfile.rs`, the first step in
moving `transfer.rs` into a module directory:

- **Write to a temp file next to the target:** `<name>.faro-part`.
  - Preallocate with `set_len(size)` when the size is known. This fails fast
    on a full disk; also check free space with `fs2`/`sysinfo` and raise a
    clear `ENOSPC`-style error.
  - Positioned writes through one **writer task** that owns the `std::fs::File`
    and receives `(offset, Bytes)` over a bounded mpsc channel. The bound gives
    backpressure.
  - The writer **groups adjacent writes**, flushing at about 4 MiB or 1 s.
  - Positioned writes are `FileExt::seek_write` on Windows and
    `FileExt::write_at` on Unix, run in `spawn_blocking`.
- **Finalize:** `sync_all` the temp file. The Overwrite/Skip/Rename choice is
  made **at rename time, not at enqueue**, so an existing file is replaced only
  once the new one is complete. Then rename atomically (`MoveFileEx` with
  replace on Windows, through `std::fs::rename`), and copy the remote mtime
  when known.
- **Cancel** deletes the `.faro-part`. **Pause and error keep it**, because
  that is the resume basis for Phase 4.
- **Every local download** goes through `PartFile`, including the
  single-stream backends (WebDAV, HTTP, OAuth clouds, FTP). Agent delta keeps
  its own temp file plus rename.
- The folder-sync and directory-diff scanners must ignore `*.faro-part`.
  Check `scan.rs` and the sync exclude defaults; on startup, delete
  `.faro-part` files left behind that have no resume row in the database.

### Phase 3 — Ranged engine (the throughput core)

**Trait** (`src-tauri/src/transfer/ranged.rs`):

```rust
#[async_trait]
trait RangeSource: Send + Sync {
    /// Stream bytes [offset, offset+len). Must return exactly `len`
    /// bytes or an error; a short read is an error, never success.
    async fn read_range(&self, offset: u64, len: u64, sink: RangeSink) -> Result<()>;
    fn max_parallel(&self) -> usize;          // backend ceiling
    fn identity(&self) -> RemoteIdentity;     // size + etag/mtime (Phase 4)
}
```

`RangeSink` pushes chunks to the `PartFile` writer, applies `checkpoint`
(pause and throttle), adds to the transfer's `AtomicU64` byte counter, and
reads the range's shared `AtomicU64 end` before each chunk. When `end`
shrinks below the current position, it stops cleanly, which is how a range
gets stolen. No locks are needed.

**Driver** (`segmented_download`):

- **Files under 16 MiB,** or a source whose `max_parallel() == 1`: one range,
  same path as before, no extra connections.
- **Otherwise:**
  - Start with 1 worker covering `[0, size)`.
  - **Ramp-up:** add one worker every 0.5 s up to
    `min(transferSegments setting, max_parallel())`.
  - **Error backoff:** on a range error, lower the cap by 1 and double the
    ramp-up interval. This copes with servers that limit connections per user.
  - **Work stealing:** a worker that becomes idle takes the **largest
    remaining** range (not the lowest offset) and
    splits off its back half. It only splits when
    `remaining > 2 × min_split`, where
    `min_split = max(4 MiB, per_worker_speed × 6 s)`.
  - **Optional auto-tune:** once the ramp-up reaches the cap, try
    `cap + 1` (up to the backend ceiling) and keep it only if throughput rises
    by more than 10 % over 8 s; otherwise stop probing. Off unless the
    `transferSegments` setting is `auto`.
- **Completed ranges** are tracked as a sorted, merged interval list. That is
  the resume state for Phase 4.

**Backends:**

- **SFTP (the biggest win).** Open a **dedicated SFTP channel per download**
  (a `RawSftpSession` on a fresh `channel_open_session`, separate from the
  shared browsing session in `ensure_sftp`).
  - Within one range, keep **N reads in flight**: a `FuturesOrdered` of
    `raw.read(handle, off, chunk)` with N = 32 and a chunk of
    `min(read_len limit, 256 KiB)`, falling back to 32 KiB if the server
    caps it. This is what OpenSSH `sftp -R` does.
  - Multiple ranges are optional on top (`max_parallel` = 4). Pipelining alone
    should fill most links.
  - **Uploads:** raise `Config.max_concurrent_writes` to 32 and the write
    buffer to 256 KiB (within `write_len`) on the upload channel.
- **Object stores** use `get_opts` with `GetOptions { range, if_match: etag }`.
  The `if_match` makes the server refuse a range if the object changed partway
  through. `max_parallel` = 8.
  - **Uploads:** keep up to `transferSegments` parts in flight, using
    `WriteMultipart::new_with_chunk_size` with `wait_for_capacity`, or a
    `FuturesUnordered` over `put_part`. Part size is
    `max(8 MiB, ceil(size / 9_000))` so very large files stay under S3's
    10,000-part limit.
  - **Small uploads:** the single-PUT cutoff (16 MiB) stays, but the PUT
    streams from the file instead of `read_to_end` where object_store allows.
- **HTTP and WebDAV** use `Range: bytes=a-b`.
  - Accept a range only when the server returns `206` with a matching
    `Content-Range`. A `200` means no range support; fall back to a single
    stream.
  - If `Accept-Ranges` is missing on a file over 1 MiB, send a small test
    range request before giving up on ranges.
  - Send `If-Range` with the ETag.
- **FTP:** extra ranges need extra logins (`REST` + `RETR` on a new
  connection, then stop once enough bytes are read, using `ABOR`).
  `max_parallel` = 1 by default, with a connection setting to allow 2–4, since
  many servers cap logins per user. Phase 8 provides the extra connections.

### Phase 4 — Resume that survives restarts

- **New `faro.db` migration:** a `transfer_resume` table with
  - `id`, `connection_id`, `kind`, `source`, `destination`, `part_path`,
    `size`;
  - `remote_etag`, `remote_mtime`, `local_mtime` (for uploads);
  - `ranges_done` (JSON interval list);
  - for uploads, `multipart_upload_id` and `parts_done` (part number, ETag) on
    object stores;
  - `updated_at`.
- **Write order:** flush the `PartFile` writer, `sync_data`, then upsert the
  row. Do this every 8 MiB or 2 s, on pause, and on error. The database must
  never claim bytes that aren't on disk.
- **Validate before resuming:** the remote file's size plus ETag (object stores,
  HTTP, WebDAV) or mtime (SFTP, FTP) must match the saved identity, and for
  uploads the local file's size and mtime must too. On a mismatch, restart from
  0 and show "remote changed, restarted" in the row. Never stitch two versions
  together.
- **On startup:** reload unfinished rows as `Paused` with a Resume action.
  They need their connection, so they wait until that connection is open.
  Clear the row and the `.faro-part` on Done or Cancel.
- **One shared resume mechanism.** `RestartFromPause` becomes
  `Resume { ranges_done }`. Pause, auto-retry, manual retry and an app restart
  all go through it.
  - FTP's in-memory `resumable` flag folds into the saved row.
  - **SFTP upload resume:** `stat` the remote, then `open_with_flags(WRITE)` at
    an offset. This is valid only when the saved identity says we wrote that
    file.
  - **S3 multipart resume:** call `ListParts` for the saved upload id and send
    only the parts that are missing. If the upload id is gone, start over.
    object_store 0.11 doesn't expose `ListParts`, so either keep the in-flight
    multipart writer alive across pause within a session (cheap) or call the S3
    API directly for resume across restarts. Decide when building; see Risks.

### Phase 5 — Retry and stall handling

- **Retry budget that resets on progress.** Each range (or the whole transfer
  in single-stream mode) gets about 8 attempts with backoff of 1 s → 30 s and
  full jitter. **The budget resets whenever bytes have moved since the last
  failure**, so a 50 GB transfer over flaky Wi-Fi never runs out of retries.
  This replaces `MAX_AUTO_RETRIES = 2` with fixed 5 s/20 s delays.
  - In segmented mode, a failing range is retried on its own; the transfer
    fails only when a range runs out of retries.
  - Respect `Retry-After` and S3 `SlowDown`/`503` (these waits don't use up
    attempts).
- **Stall watchdog per range:** no bytes for 5 s marks the row
  "not responding" (UI only). No bytes for 20 s aborts the range and re-runs it
  from its current offset, without throwing away progress.
- **Drop slow connections near the end:** once the remaining work is no more
  than the active worker count, any range running below 10 % of the
  transfer's median per-worker speed for 10 s is split or re-run.
- **Classify errors with types where the backend gives them** (object_store
  `Error` variants, SFTP `StatusCode`, FTP reply codes) before falling back to
  `classify_message`. Auth, NotFound, Permission and NoSpace are never
  retried.
- **Folder transfers:** `start_directory_*` collects per-file enqueue errors
  and keeps going instead of stopping at the first error with `?`. Failed files
  show up as Error rows, so the rest of the batch still runs.

### Phase 6 — Integrity

- **Always:** compare bytes written to the expected size (Phase 1), and in
  segmented mode check that the range list covers `[0, size)` exactly.
- **Optional `transferVerify` setting**, off by default:
  - **Object store uploads:** for a single-part PUT, compare the returned ETag
    with the local MD5 (S3, R2, B2; for Azure, `Content-MD5` when present).
    Multipart ETags aren't MD5s; skip those, or compute the S3
    `md5-of-md5s-N` form.
  - **SFTP:** when the session supports exec (Plan 10), compare
    `sha256sum`/`shasum -a 256` on the remote with a local hash computed while
    writing (the hash is computed in the writer task, so no second read). Fall
    back to a size check only.
  - **On a mismatch,** mark the row Error with "verification failed" and keep
    the `.faro-part` for inspection. Never rename a file that failed
    verification into place.
- **Faro Agent** already verifies with BLAKE3 for delta; whole-file Agent
  transfers can reuse `signature_of_file`'s whole-file hash.

### Phase 7 — Speed, ETA, event batching

- **Backend:**
  - Each transfer keeps a 1 s-bucket ring of 10 samples, fed by its atomic
    counter.
  - `Transfer` gains `bytesPerSec` (average over the buckets that have data,
    not always ÷ 10, so it isn't under-reported while warming up) and `etaSecs`
    (`(size − transferred) / speed`, omitted when speed is 0 or the size is
    unknown).
  - For segmented transfers, `segments` (the number of active workers) is
    exposed for the row tooltip.
- **One manager tick every 250 ms** emits a single
  `transfer://progress-batch` with every changed transfer, instead of each
  transfer emitting every 100 ms. `added`, `done`, `error` and `updated` stay
  immediate. `transfersStore` handles the batch with one `set()`.
- **Frontend (`TransferQueue.tsx`):** speed and ETA per row, the queue's total
  speed in the header, "not responding" and "remote changed, restarted"
  states, and a Resume action on restored rows.

### Phase 8 — A separate FTP connection per transfer

- `FtpSession` keeps its control connection for browsing. Each FTP transfer
  checks out its **own logged-in connection** from a small per-session pool
  (cap from the connection's "max transfer connections", default 2), so
  browsing no longer waits behind a copy.
  - The pool reuses the existing hardened connect path (TLS session cache,
    passive NAT fix, charset).
  - **FTP connection errors:** on `421` (too many connections) or `530`, back
    off the pool cap the same way Phase 3 lowers its cap on errors.
- Phase 3's FTP `max_parallel > 1` builds on this pool.

## Settings (`settings` table, Settings → Transfers)

- `transferSegments`: max parallel ranges or parts per file. `1` turns
  segmentation off; `auto` (the default) ramps up within the backend's ceiling.
- `transferVerify`: off / on (Phase 6).
- Existing: `transferConcurrency`, throttle, `deltaSync`. The throttle's token
  bucket is shared by all ranges unchanged, since every range calls
  `checkpoint`.

## Integration points

- `src-tauri/src/transfer.rs` becomes `transfer/` with `mod.rs` (manager, queue,
  runners), `partfile.rs`, `ranged.rs`, `resume.rs` and `speed.rs`. Backend
  arms stay in `mod.rs` or move to `transfer/backends/*.rs` as they're
  rewritten.
- `src-tauri/src/session/object.rs`: `ClientOptions`; expose the store plus
  identity (`head` → ETag/size).
- `src-tauri/src/session/mod.rs`: `open_raw_sftp_channel()` for dedicated
  transfer channels.
- `src-tauri/src/session/ftp.rs`: the transfer connection pool (Phase 8).
- `src-tauri/src/db.rs`: the `transfer_resume` migration.
- `src-tauri/src/scan.rs` / `foldersync.rs`: ignore `*.faro-part`.
- `src/lib/ipc.ts`, `src/lib/types.ts`, `src/stores/transfersStore.ts`,
  `src/components/TransferQueue.tsx`, `src/components/Settings.tsx`.

## Risks

- **SFTP server limits.** Some servers (old ProFTPD mod_sftp, embedded
  devices) cap the number of in-flight requests or channels. Read the
  `limits@openssh.com` extension when it's offered, start N at 16, and halve
  it on a failure or unexpectedly small reads. Dedicated channels count
  against `MaxSessions` (OpenSSH default 10): if opening one fails, fall back
  to the shared channel.
- **Range ownership races.** The stealer and the owner both touch `end`.
  `end` only ever shrinks, by compare-and-swap; the owner reads it before
  each write and cuts the chunk to fit. Property-test that no byte is written
  twice or skipped.
- **object_store multipart resume across restarts** isn't in 0.11's public API.
  Ship resume within a session first. Resume across restarts may need a direct
  `ListParts` call, or an object_store upgrade (check 0.12 when building).
- **No whole-request timeout** could hide a stuck connection if the Phase 5
  watchdog regresses. Phases 1 and 5 must land together, or Phase 1 sets a
  large finite timeout (for example 1 h) until the watchdog exists.
- **Windows file locking.** Antivirus can briefly lock a newly written file
  just before the rename. Retry the rename a few times with short backoff
  (and drop the `File` before renaming).
- **Preallocated sparse files** look full size in Explorer while they
  download. That's fine for a `.faro-part`, and it's the reason for the
  distinct extension.

## Verification

- **Unit tests:**
  - a fake `RangeSource` with injected latency, failures and stalls: the
    downloaded bytes are identical, ranges cover the file exactly,
    work stealing triggers, the ramp-up backs off on errors, the retry budget
    resets on progress;
  - `PartFile` handles out-of-order offset writes and finalize, and keeps the
    original file on a failed transfer under Overwrite;
  - the resume row round-trips; an identity mismatch restarts from 0;
  - the speed ring and ETA;
  - pause releases the permit (with 3 slots, pausing 3 transfers lets the 4th
    start).
- **Throughput, before and after, on real servers:**
  - **SFTP:** a 1 GiB file from a remote host with ≥ 50 ms RTT (or local
    OpenSSH with `clumsy`/netem latency). Expect several times the throughput.
    Compare with `sftp -R 64`.
  - **S3:** MinIO locally, and a real bucket (R2 or AWS) over the internet,
    for a 2 GiB download and upload. Confirm parts go in parallel, nothing is
    left in `ListMultipartUploads` after a cancel, and a 5-minute single GET no
    longer dies at 30 s.
  - **Azure:** Azurite, same matrix.
  - **FTP:** browse during an active download (no blocking); a server capped at
    one login per user drops back to a single stream cleanly.
- **Resume, end to end:** start a 2 GiB download → kill the app at about
  40 % → relaunch → the row shows Paused → Resume continues from about 40 %
  (check the bytes on the wire) → the final hash matches the source. Repeat
  after changing the remote file between runs: it restarts from 0 with the
  message.
- **Headless UI harness** (`scripts/verify-terminal.mjs` pattern): speed and
  ETA render, restored Paused rows, "not responding" state, batched progress
  events update rows.

## Build order

Phase 1 (one small slice, immediately useful) → 2 → 3 (SFTP first, then
object stores) → 4 → 5 → 6 → 7 → 8. Phases 7 and 8 don't depend on 3–6 and
can be built in between if a UI or FTP slice is wanted earlier.
