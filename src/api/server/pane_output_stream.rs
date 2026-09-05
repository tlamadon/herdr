//! `pane.stream_output` — stream one pane's raw PTY output over a held API
//! connection as NDJSON frames.
//!
//! The handler owns the blocking connection thread. It registers a
//! subscription through the app (which stashes the live broadcast receiver in
//! `crate::api::output_stream`), replays the current screen when asked, then
//! forwards raw output chunks as they are produced. A slow client observes a
//! `lag` frame followed by a fresh `repaint` instead of ever slowing the
//! pane's PTY reader.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use tokio::sync::broadcast;

use crate::api::schema::{Method, PaneStreamOutputOpenParams, PaneStreamOutputParams, Request};
use crate::api::{output_stream, ApiRequestSender};
use crate::ipc::{is_connection_closed_error, LocalStream};
use crate::pane::PaneOutputFrame;

use super::{
    api_response_outcome, dispatch_to_app_with_timeout, should_stop_connection, write_json_line,
    write_text_line_allow_disconnect, APP_RESPONSE_TIMEOUT, CONNECTION_POLL_INTERVAL,
};

/// Cap on how many raw bytes are coalesced into a single `output` frame.
const MAX_OUTPUT_FRAME_BYTES: usize = 256 * 1024;

pub(super) fn serve(
    mut stream: LocalStream,
    request_id: String,
    params: PaneStreamOutputParams,
    api_tx: &ApiRequestSender,
    running: &Arc<AtomicBool>,
) -> std::io::Result<()> {
    let Some(opened) = open_subscription(
        &mut stream,
        &request_id,
        &params.pane_id,
        params.replay,
        api_tx,
    )?
    else {
        return Ok(());
    };
    let mut receiver = opened.subscription.receiver;
    let runtime = opened.runtime;

    if !write_frame(
        &mut stream,
        &serde_json::json!({
            "type": "stream_started",
            "pane_id": params.pane_id,
            "cols": opened.subscription.cols,
            "rows": opened.subscription.rows,
        }),
    )? {
        return Ok(());
    }
    if let Some(repaint) = opened.subscription.repaint_ansi.as_deref() {
        if !write_repaint(
            &mut stream,
            repaint,
            opened.subscription.cols,
            opened.subscription.rows,
        )? {
            return Ok(());
        }
    }

    let mut seq = 0_u64;
    loop {
        if should_stop_connection(&mut stream, running)? {
            return Ok(());
        }
        let received = recv_with_timeout(&runtime, &mut receiver, CONNECTION_POLL_INTERVAL);
        let action = match received {
            RecvOutcome::Timeout => continue,
            RecvOutcome::RuntimeGone => return Ok(()),
            RecvOutcome::Frame(PaneOutputFrame::Output(bytes)) => {
                let mut buffer = bytes.to_vec();
                let follow_up = drain_ready_output(&mut receiver, &mut buffer);
                seq = seq.saturating_add(1);
                if !write_frame(
                    &mut stream,
                    &serde_json::json!({
                        "type": "output",
                        "seq": seq,
                        "data": base64::engine::general_purpose::STANDARD.encode(&buffer),
                    }),
                )? {
                    return Ok(());
                }
                follow_up
            }
            RecvOutcome::Frame(PaneOutputFrame::Resized { cols, rows }) => {
                StreamAction::Resized { cols, rows }
            }
            RecvOutcome::Lagged => StreamAction::Lagged,
            RecvOutcome::Closed => StreamAction::Closed,
        };

        match action {
            StreamAction::Continue => {}
            StreamAction::Resized { cols, rows } => {
                if !write_frame(
                    &mut stream,
                    &serde_json::json!({"type": "resized", "cols": cols, "rows": rows}),
                )? {
                    return Ok(());
                }
            }
            StreamAction::Closed => {
                let _ = write_frame(
                    &mut stream,
                    &serde_json::json!({"type": "closed", "reason": "pane_exited"}),
                );
                return Ok(());
            }
            StreamAction::Lagged => {
                if !write_frame(
                    &mut stream,
                    &serde_json::json!({"type": "lag", "dropped": true}),
                )? {
                    return Ok(());
                }
                // Resync with a fresh subscription: the coherent repaint it
                // carries replaces everything the lagged receiver dropped.
                let Some(reopened) = open_subscription(
                    &mut stream,
                    &request_id,
                    &params.pane_id,
                    crate::api::schema::PaneStreamOutputReplay::Screen,
                    api_tx,
                )?
                else {
                    return Ok(());
                };
                receiver = reopened.subscription.receiver;
                let Some(repaint) = reopened.subscription.repaint_ansi.as_deref() else {
                    return Ok(());
                };
                if !write_repaint(
                    &mut stream,
                    repaint,
                    reopened.subscription.cols,
                    reopened.subscription.rows,
                )? {
                    return Ok(());
                }
            }
        }
    }
}

/// Register a subscription through the app and claim it from the stash.
/// Returns `None` after writing the failure to the client (or when the client
/// is already gone); the caller should end the stream.
fn open_subscription(
    stream: &mut LocalStream,
    request_id: &str,
    pane_id: &str,
    replay: crate::api::schema::PaneStreamOutputReplay,
    api_tx: &ApiRequestSender,
) -> std::io::Result<Option<output_stream::PendingOutputStream>> {
    let token = output_stream::next_token();
    let response = dispatch_to_app_with_timeout(
        Request {
            id: request_id.to_owned(),
            method: Method::PaneStreamOutputOpen(PaneStreamOutputOpenParams {
                pane_id: pane_id.to_owned(),
                replay,
                token,
            }),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    if api_response_outcome(&response) != "ok" {
        // Drop a stash entry the app may still create after a timeout.
        drop(output_stream::take(token));
        write_text_line_allow_disconnect(stream, &response)?;
        return Ok(None);
    }
    let Some(entry) = output_stream::take(token) else {
        write_frame(
            stream,
            &serde_json::json!({
                "type": "error",
                "message": "output subscription was not registered",
            }),
        )?;
        return Ok(None);
    };
    Ok(Some(entry))
}

enum StreamAction {
    Continue,
    Resized { cols: u16, rows: u16 },
    Lagged,
    Closed,
}

enum RecvOutcome {
    Frame(PaneOutputFrame),
    Timeout,
    Lagged,
    Closed,
    RuntimeGone,
}

/// Await the next frame on the app runtime with a poll timeout so the loop
/// can keep checking for client disconnect and server shutdown.
fn recv_with_timeout(
    runtime: &tokio::runtime::Handle,
    receiver: &mut broadcast::Receiver<PaneOutputFrame>,
    timeout: Duration,
) -> RecvOutcome {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(async { tokio::time::timeout(timeout, receiver.recv()).await })
    }));
    match outcome {
        // block_on panics when the app runtime is already shutting down.
        Err(_) => RecvOutcome::RuntimeGone,
        Ok(Err(_elapsed)) => RecvOutcome::Timeout,
        Ok(Ok(Ok(frame))) => RecvOutcome::Frame(frame),
        Ok(Ok(Err(broadcast::error::RecvError::Lagged(_)))) => RecvOutcome::Lagged,
        Ok(Ok(Err(broadcast::error::RecvError::Closed))) => RecvOutcome::Closed,
    }
}

/// Coalesce immediately-available output into `buffer` (bounded), stopping at
/// the first non-output event so frame ordering is preserved.
fn drain_ready_output(
    receiver: &mut broadcast::Receiver<PaneOutputFrame>,
    buffer: &mut Vec<u8>,
) -> StreamAction {
    while buffer.len() < MAX_OUTPUT_FRAME_BYTES {
        match receiver.try_recv() {
            Ok(PaneOutputFrame::Output(bytes)) => buffer.extend_from_slice(&bytes),
            Ok(PaneOutputFrame::Resized { cols, rows }) => {
                return StreamAction::Resized { cols, rows };
            }
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Lagged(_)) => return StreamAction::Lagged,
            Err(broadcast::error::TryRecvError::Closed) => return StreamAction::Closed,
        }
    }
    StreamAction::Continue
}

fn write_repaint(
    stream: &mut LocalStream,
    repaint_ansi: &str,
    cols: u16,
    rows: u16,
) -> std::io::Result<bool> {
    write_frame(
        stream,
        &serde_json::json!({
            "type": "repaint",
            "data": base64::engine::general_purpose::STANDARD.encode(repaint_ansi.as_bytes()),
            "cols": cols,
            "rows": rows,
        }),
    )
}

/// Write one NDJSON frame. Returns `Ok(false)` when the client disconnected.
fn write_frame<T: serde::Serialize>(stream: &mut LocalStream, value: &T) -> std::io::Result<bool> {
    match write_json_line(stream, value) {
        Ok(()) => Ok(true),
        Err(err) if is_connection_closed_error(&err) => Ok(false),
        Err(err) => Err(err),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::api::schema::{ErrorBody, ErrorResponse, ResponseResult, SuccessResponse};
    use crate::api::{ApiRequestMessage, EventHub};
    use crate::pane::PaneRuntime;
    use std::io::{BufRead, BufReader, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::mpsc;

    static NEXT_LOCAL_STREAM_ID: AtomicU64 = AtomicU64::new(1);

    fn local_stream_pair() -> (LocalStream, LocalStream, PathBuf) {
        use interprocess::local_socket::traits::Listener as _;
        let unique = format!(
            "hpo-{}-{}.sock",
            std::process::id(),
            NEXT_LOCAL_STREAM_ID.fetch_add(1, Ordering::Relaxed)
        );
        let path = std::env::temp_dir().join(unique);
        let listener = crate::ipc::bind_local_listener(&path).unwrap();
        let client = crate::ipc::connect_local_stream(&path).unwrap();
        let server = listener.accept().unwrap();
        (client, server, path)
    }

    fn decode_data(frame: &serde_json::Value) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(frame["data"].as_str().expect("data field"))
            .expect("valid base64")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_output_replays_screen_then_streams_until_pane_exit() {
        let (api_tx, mut api_rx) = mpsc::unbounded_channel::<ApiRequestMessage>();
        let (mut client, server, _path) = local_stream_pair();
        client
            .write_all(
                br#"{"id":"out_1","method":"pane.stream_output","params":{"pane_id":"pane_1"}}"#,
            )
            .unwrap();
        client.write_all(b"\n").unwrap();
        client.flush().unwrap();

        let running = Arc::new(AtomicBool::new(true));
        let server_running = Arc::clone(&running);
        let event_hub = EventHub::default();
        let server_thread = std::thread::spawn(move || {
            super::super::handle_connection(server, &api_tx, &event_hub, &server_running, None)
        });

        let open = api_rx.recv().await.expect("open request");
        let (token, replay) = match &open.request.method {
            Method::PaneStreamOutputOpen(params) => {
                assert_eq!(params.pane_id, "pane_1");
                (params.token, params.replay)
            }
            other => panic!("unexpected request: {other:?}"),
        };
        assert_eq!(replay, crate::api::schema::PaneStreamOutputReplay::Screen);

        let runtime = PaneRuntime::test_with_screen_bytes(80, 24, b"replayed");
        crate::api::output_stream::stash(token, runtime.subscribe_output(true));
        open.respond_to
            .send(
                serde_json::to_string(&SuccessResponse {
                    id: open.request.id,
                    result: ResponseResult::Ok {},
                })
                .unwrap(),
            )
            .unwrap();

        runtime.test_process_pty_bytes(b"live-bytes");
        drop(runtime);

        let mut reader = BufReader::new(&mut client);
        let mut next_frame = || {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            serde_json::from_str::<serde_json::Value>(&line).unwrap()
        };

        let started = next_frame();
        assert_eq!(started["type"], "stream_started");
        assert_eq!(started["pane_id"], "pane_1");
        assert_eq!(started["cols"], 80);
        assert_eq!(started["rows"], 24);

        let repaint = next_frame();
        assert_eq!(repaint["type"], "repaint");
        assert_eq!(repaint["cols"], 80);
        let repaint_ansi = String::from_utf8(decode_data(&repaint)).unwrap();
        assert!(repaint_ansi.contains("replayed"));

        let output = next_frame();
        assert_eq!(output["type"], "output");
        assert_eq!(output["seq"], 1);
        assert_eq!(decode_data(&output), b"live-bytes");

        let closed = next_frame();
        assert_eq!(closed["type"], "closed");
        assert_eq!(closed["reason"], "pane_exited");

        drop(reader);
        drop(client);
        server_thread.join().unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_output_forwards_open_errors_and_cleans_up() {
        let (api_tx, mut api_rx) = mpsc::unbounded_channel::<ApiRequestMessage>();
        let (mut client, server, _path) = local_stream_pair();
        client
            .write_all(
                br#"{"id":"out_2","method":"pane.stream_output","params":{"pane_id":"gone","replay":"none"}}"#,
            )
            .unwrap();
        client.write_all(b"\n").unwrap();
        client.flush().unwrap();

        let running = Arc::new(AtomicBool::new(true));
        let server_running = Arc::clone(&running);
        let event_hub = EventHub::default();
        let server_thread = std::thread::spawn(move || {
            super::super::handle_connection(server, &api_tx, &event_hub, &server_running, None)
        });

        let open = api_rx.recv().await.expect("open request");
        let token = match &open.request.method {
            Method::PaneStreamOutputOpen(params) => {
                assert_eq!(params.pane_id, "gone");
                assert_eq!(
                    params.replay,
                    crate::api::schema::PaneStreamOutputReplay::None
                );
                params.token
            }
            other => panic!("unexpected request: {other:?}"),
        };
        open.respond_to
            .send(
                serde_json::to_string(&ErrorResponse {
                    id: open.request.id,
                    error: ErrorBody {
                        code: "pane_not_found".into(),
                        message: "pane gone not found".into(),
                    },
                })
                .unwrap(),
            )
            .unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let response: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(response["error"]["code"], "pane_not_found");

        drop(reader);
        drop(client);
        server_thread.join().unwrap().unwrap();
        assert!(crate::api::output_stream::take(token).is_none());
    }
}
