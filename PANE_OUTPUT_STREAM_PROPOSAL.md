# ✅ RESOLVED — no new API needed; herdr already streams a single pane

> **Update (2026-09-05).** The proposal below is **superseded.** While scoping the
> PR I discovered herdr already ships exactly this capability in stable releases,
> via the CLI subcommands `herdr terminal session observe` / `control`. The
> [herdr-mirror](https://github.com/nikok6/herdr-mirror) plugin uses the same
> primitive. herdr HQ now mirrors and drives panes through it, with **zero
> changes to herdr**. Keep the design notes below only as background.

## How to stream (and drive) one pane

Two CLI subcommands, one frame protocol. Both talk to the local herdr socket and
print **one JSON object per line** on stdout — raw ANSI frames, full repaints +
incremental diffs (default size 120×40 if `--cols/--rows` are omitted):

```
herdr terminal session observe <pane>  [--cols N] [--rows N]              # read-only
herdr terminal session control <pane>  [--cols N] [--rows N] [--takeover] # writable
```

```json
{"type":"terminal.frame","seq":42,"encoding":"ansi","width":120,"height":40,"full":false,"bytes":"<base64 ANSI>"}
{"type":"terminal.closed","reason":"pane closed"}
```

Write those `bytes` straight into an emulator (xterm.js, etc.): correct colours,
cursor, synchronized output — no polling, no scrape-and-repaint.

`control` additionally reads **JSON commands on its stdin**:

```json
{"type":"terminal.input","text":"ls\n"}          // or {"bytes":"<base64>"}
{"type":"terminal.resize","cols":160,"rows":48}
{"type":"terminal.scroll","direction":"up","lines":10,"source":"wheel"}
{"type":"terminal.release"}
```

## The one gotcha (observe vs control)

`observe` is **passive**: `--cols/--rows` only size the frame *you* receive; the
pane's real PTY keeps whatever size its controlling client set, so you get a
**clipped** view of a larger grid. That's fine for a top-anchored app but guts a
full-screen TUI (Claude Code's input box / permission line sit at the
bottom-right and fall outside the slice). Use **`control` (with `--takeover`)** to
become the controlling client: herdr then resizes the PTY to *your* size and the
process redraws to fit exactly. When your stream ends, herdr hands the size back
to whatever clients remain — the resize is **not permanent**.

## Over SSH

Run the `control`/`observe` CLI **on the remote host** and pipe its stdio through
ssh: frames come back over stdout, commands go in over stdin.

## What herdr HQ actually does

- spawns `herdr terminal session control <pane> --cols C --rows R --takeover`
  (sized by the browser terminal's FitAddon) and relays each `terminal.frame`
  into xterm.js;
- keystrokes go through the socket API `pane.send_input` (no key-name→byte
  translation needed);
- mouse-wheel history uses `terminal.scroll` on the control stdin;
- leaving a pane closes the stream so herdr resizes it back promptly.

See herdr HQ commit `bc24737` ("Terminal: control the pane (resize-to-fit)
instead of passive observe") and `src/herdrhq/remote/attach.py`.

---

<sub>The original proposal follows, preserved for context. It argued for building a
`pane.stream_output` socket method — unnecessary, since the above already exists.</sub>

---

# Proposal: `pane.stream_output` — a per-pane raw output stream for herdr's socket API

**Status:** ~~design sketch for a PR against herdr~~ **superseded — see the Resolved section above.**
**Author context:** written from a read of herdr v0.8.2 source. Line numbers are
from that checkout; verify against the branch you build on.
**Audience:** an engineer/agent implementing this as a herdr PR.

---

## 1. Summary

Add a socket-API method, `pane.stream_output`, that lets an external client
subscribe to a **single pane** and receive its terminal output **as it is
produced**, over a held connection — the equivalent of tmux control-mode
`%output` / `tmux pipe-pane`. Today the only way to observe a pane's output
through the API is `pane.read`, which returns a **snapshot** of the visible
screen; consumers must poll it. This forces every external UI (e.g. a browser
terminal) into scrape-and-repaint with the latency and fidelity limits that
implies.

The good news: **herdr already contains every hard piece.** It already streams
per-pane output to its own attached clients (incremental ANSI diffs), it
already has a per-pane *streaming* method on the socket API (`pane.graphics.stream`),
it already has an outbound push-stream handler (`events.subscribe`), and every
pane's raw PTY bytes already pass through a single closure. This proposal wires
those together and exposes them.

## 2. Motivation / use case

External clients (dashboards, web terminals, editor integrations) want to
render a live herdr pane with real-terminal fidelity and latency:

- **Latency.** `pane.read` snapshots + `pane.updated` events cap practical
  refresh at herdr's ~10 Hz change-event cadence, even though the pane buffer
  updates within a few ms. A raw stream removes the poll entirely.
- **Fidelity & features.** A raw byte stream feeds straight into a client-side
  terminal emulator (xterm.js, etc.), giving correct cursor position, wide
  chars, scroll regions, and enabling client-side *predictive local echo*
  (mosh-style) — none of which a screen snapshot can support.
- **Parity with tmux.** tmux exposes exactly this via `pipe-pane` and control
  mode (`-CC`); it is why iTerm2 can render tmux windows as native tabs. herdr
  is otherwise a superset API-wise but lacks this one primitive.

## 3. Goals / non-goals

**Goals**
- One method to subscribe to one pane's live output over a held API connection.
- An initial "catch-up" so a fresh subscriber starts from a correct screen.
- Robust lifecycle: multiple concurrent subscribers, clean unsubscribe on
  disconnect, and pane-exit termination.
- **Never block or slow the pane's PTY read loop**, regardless of a slow client.

**Non-goals**
- Changing the existing TUI client-attach transport.
- Replacing `pane.read` (it stays; snapshots are still useful).
- Cross-pane / whole-session streaming (that is what client attach already is).

## 4. How herdr is built today (the pieces we reuse)

Paths are relative to the repo root; line numbers from v0.8.2. Two facts that
shape the whole design:

- **Concurrency is mixed.** The app/headless loop is **tokio**
  (`src/server/headless.rs` `tokio::select!` over `api_rx` / events / render
  notify), but the **socket-API server is blocking — one OS thread per
  connection** (`src/api/server.rs:91-118`), and **each pane's PTY reader is
  its own plain OS thread** (`herdr-pty-{pane_id}`). So a streaming handler is a
  blocking thread that owns its `LocalStream` and drains a channel; it does not
  live in the tokio runtime. Plan the byte-delivery channel accordingly (a
  `std`/`crossbeam` mpsc or a `tokio::broadcast` read with `blocking_recv`).
- **The terminal emulator is libghostty via FFI** (`src/ghostty/`); `portable-pty`
  (vendored `=0.9.0`) is used only to open the PTY, not to parse. Raw child
  bytes are handed to ghostty's VT parser and **discarded** — only the parsed
  grid + scrollback is retained (see §4.2). This is why a raw stream must tap the
  live read and cannot be reconstructed from stored state.

### 4.1 The socket-API server and method dispatch
- Wire schema: **`src/api/schema.rs`** — `Request` (line ~36) and the
  `Method` enum (line ~47). Each method is a variant with a
  `#[serde(rename = "group.name")]`. `pane.read` is `PaneRead(PaneReadParams)`
  (~195); `pane.send_input` is `PaneSendInput(...)` (~193);
  `events.subscribe` is `EventsSubscribe(EventsSubscribeParams)` (~232).
- **Streaming methods already exist here.** `pane.graphics.stream` is
  `PaneGraphicsStream(PaneGraphicsStreamParams)` (~204), marked
  `#[schemars(skip)]` (streaming methods are excluded from the generated JSON
  schema). It has companion internal variants (`PaneGraphicsStreamOpen/Close/…`)
  that are `#[serde(skip)]`.
- Request dispatch for streaming vs unary lives in **`src/api/server.rs`**
  (the API socket server). The top-level `match request.method` (~197) routes:
  - `Method::PaneGraphicsStream(params)` → `pane_graphics_stream::serve(stream, …)`
    — hands the raw connection (`LocalStream`) to a per-pane stream handler.
  - `Method::EventsSubscribe(params)` → `stream_subscriptions(stream, …)`
    — holds the connection and **pushes many messages** to the client.
  - `Method::EventsWait / AgentWait / PaneWaitForOutput` → block-until handlers.
  - default arm → ordinary one-shot `handle_request(...)` + `write_text_line`.
  So there is a first-class notion of "this method takes over the connection
  and streams", and adding another is a new `match` arm + handler module.
- The per-pane stream template: **`src/api/server/pane_graphics_stream.rs`**,
  `serve(stream, request_id, params, api_tx, running)`. It shows the connection
  lifecycle, framing helpers (`dispatch_stream_open`, `write_json_line`,
  `CONNECTION_POLL_INTERVAL`, `is_connection_closed_error`), and timeouts.
  (Note: graphics streaming is *inbound* — client uploads image frames — but
  the connection-takeover plumbing is identical to what an outbound output
  stream needs.)
- The outbound push template: `stream_subscriptions(...)` (the
  `events.subscribe` handler in `src/api/server.rs`) shows the server→client
  push loop and how it consults the event source until the client disconnects.
- Cross-thread plumbing: the API server thread reaches the app via
  `ApiRequestSender` (`api_tx`) and helpers like `dispatch_to_app_with_timeout`.
  The shared event source is **`src/api/event_hub.rs`** — a small
  `Arc<Mutex<…>>` ring buffer (`push` / `events_after(seq)`, cap 512). This is
  fine for low-rate control events but **must not** be the transport for
  high-rate raw output (see §6.3).

### 4.2 The pane / PTY / terminal runtime (the byte source)
- **PTY spawn:** `src/pty/backend/unix.rs:12` `spawn_with_portable_pty` →
  `openpty`, returning `SpawnedPty { master_fd }`; called from `src/pane.rs:2271`.
- **The read loop is a dedicated OS thread per pane.** The master fd is owned by
  a PTY I/O actor (`src/pty/actor/unix.rs:376` `PtyIoActor::spawn`, thread at
  `:426`). Its blocking loop `PtyIoActorRunner::run()` (`:505`) reads into an
  8 KiB buffer in `read_once()` (`:819-852`) and invokes the callback:
  ```rust
  // src/pty/actor/unix.rs:834
  let result = (self.on_read)(&buf[..n]);
  ```
  `on_read: ReadCallback = Box<dyn FnMut(&[u8]) -> PtyReadResult + Send>`
  (`:42`). **Every raw byte of every pane passes through `&buf[..n]` here.**
- **The callback that owns the pane context** is built in `src/pane.rs`, in two
  near-identical copies: normal spawn **`src/pane.rs:2320-2367`** (registered
  `:2375`) and handoff spawn **`:2139-2184`** (registered `:2194`). Inside it
  (`:2328` / `:2147`):
  ```rust
  let result = terminal.process_pty_bytes(pane_id, shell_pid, bytes, &response_writer);
  ```
  → `src/pane/terminal.rs:1307` → ghostty FFI `Terminal::write()`
  (`src/ghostty/mod.rs:874`), which **consumes and discards** the raw bytes into
  the parser. This same closure already bumps a per-pane change counter
  (`content_seq.fetch_add`, `:2325`), publishes bell/cwd/clipboard `AppEvent`s,
  and calls `render_notify.notify_one()` — so it already holds the pane id and
  tokio handles. **This `src/pane.rs:2320`/`2139` site is the right place to
  tee** (clone `bytes` to a per-pane channel), preferable to the lower
  `actor/unix.rs:834` site.
- **No raw byte history is kept.** `pane.read` *reconstructs* ANSI from the
  parsed grid: `read_terminal_snapshot` (`src/app/api_helpers.rs:109`) → ghostty
  `read_ansi_screen` (`src/ghostty/mod.rs:1250`). So the "initial screen" replay
  (§5.2) reuses that same snapshot path; the *live* stream must come from the
  tee above.
- Pane runtime struct: `PaneRuntime` (`src/pane.rs:1236`) with
  `io: PaneRuntimeIo::Actor(PtyIoActorHandle)` (`:1257`) — the write side; there
  is **no read-side hook today**, so one must be added here. App-facing newtype:
  `TerminalRuntime` (`src/terminal/runtime.rs:17`), stored in a registry keyed by
  terminal id.
- **No existing raw fan-out to piggyback on.** Attached TUI clients receive
  *rendered grid frames*, not raw bytes: `src/server/render_stream.rs`
  (`ClientRenderState::{Semantic|TerminalAnsi}`), diffed by
  `src/protocol/render_ansi.rs` `BlitEncoder`, sent as
  `ServerMessage::Terminal(TerminalFrame)` (`src/protocol/wire.rs:1329`). That
  is a re-render of the grid, downstream of parsing — useful as a *fallback*
  encoding (§6, option B) but it is not a raw byte broadcast. The only raw bytes
  are at the tee point above.

## 5. Proposed API

### 5.1 Method
`pane.stream_output` — subscribe to one pane's live output. The connection is
held open; the server writes NDJSON frames until the client disconnects or the
pane exits.

**Params** (`PaneStreamOutputParams`):
```jsonc
{
  "pane_id": "wG:p1",         // required; the pane to follow
  "replay": "screen"          // optional: "screen" (default) | "none"
                              //   "screen": send the current visible screen first
                              //   "none":   only bytes produced after subscribe
}
```

### 5.2 Wire protocol (server → client, one JSON object per line)
```jsonc
// 1. ack
{"type":"stream_started","pane_id":"wG:p1","cols":211,"rows":66}

// 2. optional initial repaint (when replay="screen"): the current screen as
//    a single base64 ANSI blob the client can write verbatim to its emulator
{"type":"repaint","data":"<base64 ANSI of the visible screen>","cols":211,"rows":66}

// 3. live output — raw PTY bytes as produced, base64-encoded, in order
{"type":"output","seq":42,"data":"<base64 raw bytes>"}

// 4. pane resized (herdr controls pane size; client should resize its emulator)
{"type":"resized","cols":180,"rows":50}

// 5. the server dropped bytes to a slow client; a repaint follows to resync
{"type":"lag","dropped":true}
{"type":"repaint","data":"…","cols":211,"rows":66}

// 6. terminal states
{"type":"closed","reason":"pane_exited"}   // pane ended
{"type":"error","message":"unknown pane"}  // bad request
```
Base64 keeps it inside the existing NDJSON framing (no binary framing needed)
and is the simplest v1. If throughput matters, herdr already has a
**length-prefixed binary framing** precedent: `pane.graphics.stream` writes a
JSON header line followed by a binary body
(`src/api/server/pane_graphics_stream.rs`) — reuse that convention for a
`%output`-style binary frame in a later revision. Raw PTY bytes (not
re-rendered ANSI) are preferred: the client's own emulator is the source of
truth, cursor and all.

## 6. Implementation plan

### 6.1 Schema (`src/api/schema.rs`)
- Add near the other pane variants (~line 204, by `PaneGraphicsStream`):
  ```rust
  #[serde(rename = "pane.stream_output")]
  #[schemars(skip)]                 // streaming methods are excluded from the JSON schema
  PaneStreamOutput(PaneStreamOutputParams),
  ```
- Add `PaneStreamOutputParams { pane_id: String, #[serde(default)] replay: ReplayMode }`
  and a `ReplayMode` enum (`Screen` default, `None`). Frame types can be plain
  `serde_json::json!` in the handler (as the graphics stream does) rather than
  first-class schema types, to keep the schema surface small.

### 6.2 Dispatch + handler (`src/api/server.rs` + new `src/api/server/pane_output_stream.rs`)
- Two match sites in `src/api/server.rs` (both closed/exhaustive — the compiler
  will flag either if missed):
  - `api_method_name` (~`:375`) — add the name mapping for logging.
  - `handle_connection_with_stop`'s `match request.method` (~`:197`) — add a
    **streaming** arm (do NOT fall through to the one-shot `handle_request` /
    `method_body =>` arm at `:274`):
    ```rust
    Method::PaneStreamOutput(params) => {
        let result = pane_output_stream::serve(stream, request_id.clone(), params, api_tx, running);
        // …log completed/failed like the PaneGraphicsStream / EventsSubscribe arms…
        result
    }
    ```
- New module `pane_output_stream::serve(...)` — a **blocking** handler owning the
  `LocalStream`, modeled on `stream_subscriptions` (`src/api/server.rs:689`, the
  outbound push loop) plus the connection helpers from `pane_graphics_stream.rs`
  (`dispatch_stream_open`, `write_json_line`, `CONNECTION_POLL_INTERVAL`,
  `is_connection_closed_error`, `should_stop_connection`). Shape:
  1. Register the subscriber with the app by dispatching a new
     `pane.stream_output.open` (mirror `PaneGraphicsStreamOpen`, see §6.3) over
     `api_tx`; the reply carries a byte-channel receiver, current `(cols, rows)`,
     and (if `replay == "screen"`) the current screen snapshot. Missing pane →
     write `{"type":"error"}` and return.
  2. Write `stream_started`; if replaying, write `repaint`.
  3. Loop with a blocking `recv` (short timeout) on the byte channel: base64 each
     chunk, write `output` frames with an incrementing `seq`; between reads check
     `should_stop_connection`/`running` and the `stream_active` flag so a client
     disconnect breaks the loop. On channel-lag (slow client), write `lag` then a
     fresh `repaint`. On pane-exit sentinel, write `closed`.
  4. On exit, dispatch `pane.stream_output.close` to unregister (mirror
     `PaneGraphicsStreamClose`).

### 6.3 Byte source & fan-out (the one genuinely new mechanism)
No raw tap or raw-byte buffer exists today, so this is the substantive new code.
Three joined changes:

- **Tap the pane's `on_read` closure** at `src/pane.rs:2320-2367` (and the
  handoff copy `:2139-2184`), right where it already clones the pane context.
  After `process_pty_bytes`, forward the raw `bytes` to a per-pane broadcaster
  **iff it has subscribers** — near-zero cost when nobody is listening (the hot
  path must stay cheap):
  ```rust
  if let Some(tx) = &output_tx { if tx.receiver_count() > 0 { let _ = tx.send(bytes.clone()); } }
  ```
  Use a **bounded** `tokio::sync::broadcast` (e.g. capacity ~1024 chunks). A slow
  receiver gets `RecvError::Lagged`, which the handler turns into `lag` +
  `repaint` — **the PTY thread never blocks.** This is the critical safety
  property. (You can also gate the tee on the existing `content_seq` if useful.)
- **Expose subscribe/current-screen on the runtime.** Add to `PaneRuntime`
  (`src/pane.rs:1236`, next to `content_seq`) and surface via `TerminalRuntime`
  (`src/terminal/runtime.rs:17`):
  - `subscribe_output(&self) -> (broadcast::Receiver<Bytes>, u16, u16)` — receiver
    + current cols/rows;
  - reuse `read_terminal_snapshot` (`src/app/api_helpers.rs:109`) for the initial
    screen so replay matches exactly what `pane.read` would return.
- **Register/tear down across the blocking-API ↔ tokio-app boundary** using the
  existing stream mechanism: `ApiRequestMessage` (`src/api/mod.rs:89`) already
  carries `respond_to` (one-shot reply) and `stream_active: Option<Arc<AtomicBool>>`
  (`:93`); graphics streaming registers via `dispatch_stream_open`/`_frame`
  (`src/api/server.rs:804-825`) and is specially routed in the app pump at
  `src/server/headless.rs:2915-2921`. Add a `pane.stream_output.open` handler in
  `src/app/api/panes.rs` that looks up the `TerminalRuntime`
  (`lookup_runtime`, `src/app/creation.rs:358`), calls `subscribe_output`, and
  returns the receiver + dims + snapshot to the blocking `serve()` via the reply
  channel; a `.close` handler drops the registration. Honor `stream_active` for
  teardown on client disconnect, exactly as the graphics stream does.

Do **not** route raw output through `EventHub` (`src/api/event_hub.rs`): it is a
mutexed 512-entry ring polled every `CONNECTION_POLL_INTERVAL` (100 ms) — fine
for control events, but it would drop/coalesce a byte firehose. Use the
dedicated per-pane broadcast above. (Note the existing `PaneOutputMatched`
subscription and `pane.wait_for_output` are also just 100 ms `pane.read`
snapshot-polling — not a precedent to copy for raw bytes.)

### 6.4 Lifecycle & edges
- **Multiple subscribers:** `broadcast` fans out natively.
- **Unsubscribe:** dropping the receiver (on disconnect) drops the count to 0;
  the `on_read` tap then does nothing. No explicit teardown race.
- **Pane exit:** the runtime already emits pane-exit to the app; surface it to
  the handler (close the broadcast or send a sentinel) → `closed` frame.
- **Resize:** when herdr resizes the pane, emit a `resized` frame (hook the
  existing resize path in the runtime) so the client resizes its emulator.
- **Backpressure:** covered by the bounded broadcast + `lag`/`repaint` resync.

## 7. Tests
- Unit/integration mirror existing API tests: `tests/api_ping.rs`,
  `tests/cli/panes.rs`, and especially `tests/multi_client.rs`
  (multi-subscriber fan-out).
- New integration test: open a pane running a shell, `pane.stream_output`
  subscribe, `pane.send_input` a command, assert the echoed/produced bytes
  arrive on the stream in order and that a second concurrent subscriber sees the
  same. Add a slow-consumer test asserting `lag`+`repaint` rather than a stalled
  PTY.

## 8. Docs & schema
- Streaming methods are `#[schemars(skip)]`, so `herdr api schema --json` won't
  list it (consistent with `pane.graphics.stream`). Document it in the socket-API
  docs (`docs/…/socket-api.mdx`) with the wire protocol from §5.
- Note it in `CHANGELOG.md`.

## 9. Effort & risk

**Effort:** moderate — days, not weeks, for someone comfortable in the codebase;
more for a first-time contributor ramping on the API/app/pty split. Most of the
surface is additive and patterned on `pane_graphics_stream` + `stream_subscriptions`.

**Main risks / fiddly bits**
1. The API-thread ↔ app-thread subscription handshake (§6.3) — getting a
   `broadcast::Receiver` back to the handler cleanly, respecting herdr's
   ownership model. This is the real design work.
2. Backpressure correctness — the bounded broadcast + lag/repaint must be
   airtight so no client can slow a pane. Test it explicitly.
3. Initial-screen replay coherence — reuse the exact serialization `pane.read`
   uses so the client's emulator starts in a valid state before live bytes.
4. Raw vs rendered choice — raw PTY bytes are simplest and highest fidelity;
   only fall back to the `BlitEncoder` diff path if raw framing proves too heavy
   over the socket.

## 10. What this unlocks downstream
A consumer (e.g. herdr HQ) pipes `output` frames straight into a client-side
xterm.js: true cursor, correct wide-char/scroll handling, and the option of
predictive local echo — i.e. iTerm2-over-tmux-class responsiveness — with no
screen scraping. `pane.read` remains for one-shot snapshots.

---

### Quick file index for the implementer
| Concern | File(s) |
| --- | --- |
| Method enum / params | `src/api/schema.rs` (Method ~47; PaneGraphicsStream ~204) + `src/api/schema/panes.rs` |
| Stream dispatch (2 sites, exhaustive) | `src/api/server.rs` (`match request.method` ~197; `api_method_name` ~375) |
| Per-pane stream template (binary framing) | `src/api/server/pane_graphics_stream.rs` (`serve`) |
| Outbound push template | `src/api/server.rs` `stream_subscriptions` ~689 (events.subscribe) |
| Stream register/teardown mechanism | `ApiRequestMessage.stream_active` `src/api/mod.rs:89-93`; `dispatch_stream_open/_frame` `src/api/server.rs:804-825`; app routing `src/server/headless.rs:2915-2921` |
| Event hub (control events; NOT for output) | `src/api/event_hub.rs` |
| **Raw byte tee point** | `src/pane.rs:2320-2367` (normal), `:2139-2184` (handoff); lower alt `src/pty/actor/unix.rs:834` |
| PTY spawn / read thread | `src/pty/backend/unix.rs:12`, `src/pty/actor/unix.rs` (`spawn` :376, `read_once` :819) |
| Emulator (bytes discarded after parse) | `src/ghostty/mod.rs` (`write` :874, `read_ansi_screen` :1250) via FFI |
| Pane/terminal runtime (add subscribe hook) | `src/pane.rs:1236` (`PaneRuntime`), `src/terminal/runtime.rs:17` (`TerminalRuntime`) |
| `pane.read` snapshot (reuse for replay) | `src/app/api_helpers.rs:109`; handler `src/app/api/panes.rs:1489` |
| Runtime lookup | `src/app/creation.rs:358` (`lookup_runtime`), `:369` (`lookup_runtime_sender`) |
| Existing render fan-out (fallback encoding only) | `src/protocol/render_ansi.rs` (`BlitEncoder`), `src/server/render_stream.rs`, `src/protocol/wire.rs:1329` |
| Tests to mirror | `tests/multi_client.rs`, `tests/api_ping.rs`, `tests/cli/panes.rs` |
