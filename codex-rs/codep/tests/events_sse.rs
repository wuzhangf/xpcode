//! worker-events/1: the OUTPUT_DELTA spine write and the SSE read surface on
//! `GET /local/v1/tasks/{id}/events`. Tests run against the real dispatch —
//! guards, query parsing and route match included — with a Store on a tempdir
//! and no live engine (`Host::driver: None`), so no model is involved.

#![allow(clippy::unwrap_used)]

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use codep::driver::SharedStore;
use codep::driver::output_delta;
use codep::host::Host;
use codep::host::router;
use codep::store::Store;
use codex_protocol::protocol::AgentMessageContentDeltaEvent;
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

fn open_store() -> (SharedStore, PathBuf) {
    let dir = std::env::temp_dir().join(format!("codep-events-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(
        &dir.join("store.db"),
        &json!({
            "authorityId": "authority-test",
            "storeId": "store-test",
            "workerId": "worker-test",
        }),
    )
    .unwrap();
    (Arc::new(Mutex::new(store)), dir)
}

fn seed_task(store: &SharedStore, task: &str) {
    store
        .lock()
        .unwrap()
        .save(&json!({
            "taskId": task,
            "engineId": "codep",
            "state": "RUNNING",
            "stateVersion": "1",
            "quiescent": false,
        }))
        .unwrap();
}

fn delta_event(delta: &str) -> AgentMessageContentDeltaEvent {
    AgentMessageContentDeltaEvent {
        thread_id: "thread-1".to_string(),
        turn_id: "turn-1".to_string(),
        item_id: "item-1".to_string(),
        delta: delta.to_string(),
    }
}

async fn serve(store: SharedStore, state_root: PathBuf) -> (String, JoinHandle<()>) {
    let host = Arc::new(Host {
        driver: None,
        store,
        token: "tok-test".to_string(),
        state_root,
        boot_id: "boot-test".to_string(),
        model: "model-test".to_string(),
        projects: vec![],
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = router(host);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("127.0.0.1:{port}"), server)
}

// ---------- minimal HTTP/1.1 client (SSE needs incremental body reads) ----------

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

async fn http_get(
    addr: &str,
    path: &str,
    extra: &[(&str, &str)],
) -> (u16, Vec<(String, String)>, BufReader<TcpStream>) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer tok-test\r\nConnection: close\r\n"
    );
    for (k, v) in extra {
        request.push_str(&format!("{k}: {v}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut r = BufReader::new(stream);
    let mut status_line = String::new();
    r.read_line(&mut status_line).await.unwrap();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        r.read_line(&mut line).await.unwrap();
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        let (k, v) = line.split_once(':').unwrap();
        headers.push((k.trim().to_string(), v.trim().to_string()));
    }
    (status, headers, r)
}

async fn read_body(r: &mut BufReader<TcpStream>, headers: &[(String, String)]) -> String {
    if let Some(len) = header(headers, "content-length") {
        let len: usize = len.parse().unwrap();
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf).await.unwrap();
        return String::from_utf8_lossy(&buf).into_owned();
    }
    let mut buf = Vec::new();
    r.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}

/// One chunk of an HTTP chunked body; empty Vec = terminating 0-chunk.
async fn read_chunk(r: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    let mut size_line = String::new();
    if r.read_line(&mut size_line).await? == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let size = usize::from_str_radix(size_line.trim().split(';').next().unwrap_or_default(), 16)
        .map_err(io::Error::other)?;
    if size == 0 {
        // Trailer section ends with a blank line.
        loop {
            let mut line = String::new();
            r.read_line(&mut line).await?;
            if line.trim().is_empty() {
                return Ok(Vec::new());
            }
        }
    }
    let mut buf = vec![0u8; size];
    r.read_exact(&mut buf).await?;
    let mut crlf = [0u8; 2];
    r.read_exact(&mut crlf).await?;
    Ok(buf)
}

/// Next SSE frame (`...\n\n`) from a chunked body, accumulated across chunks.
/// Comment frames (`:`-prefixed, e.g. heartbeats) are skipped so event-frame
/// assertions stay deterministic.
async fn read_sse_frame(r: &mut BufReader<TcpStream>, acc: &mut String) -> Option<String> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            while let Some(pos) = acc.find("\n\n") {
                let frame: String = acc.drain(..pos + 2).collect();
                if frame.starts_with(':') {
                    continue;
                }
                return Some(frame);
            }
            match read_chunk(r).await {
                Ok(c) if c.is_empty() => return None,
                Ok(c) => acc.push_str(&String::from_utf8_lossy(&c)),
                Err(_) => return None,
            }
        }
    })
    .await
    .ok()?
}

fn assert_event_frame(frame: &str, id: &str, kind: &str) {
    // The frame carries a trailing blank line; drop it before splitting.
    let lines: Vec<&str> = frame.trim_end_matches('\n').lines().collect();
    assert_eq!(
        lines.len(),
        3,
        "frame must be id/event/data lines: {frame:?}"
    );
    assert_eq!(lines[0], format!("id: {id}"));
    assert_eq!(lines[1], format!("event: {kind}"));
    let data = lines[2].strip_prefix("data: ").unwrap();
    let event: Value = serde_json::from_str(data).unwrap();
    assert_eq!(event["kind"], kind);
    assert_eq!(event["seq"], id);
}

// ---------- tests ----------

#[tokio::test]
async fn output_delta_lands_in_events_with_monotonic_seq() {
    let (store, _dir) = open_store();
    seed_task(&store, "t1");
    {
        let s = store.lock().unwrap();
        s.event("t1", "turn/started", json!({"nativeTurnId":"turn-1"}))
            .unwrap();
    }
    output_delta(&store, "t1", &delta_event("Hel"));
    output_delta(&store, "t1", &delta_event("lo"));

    let page = store.lock().unwrap().events("t1", 0, 50).unwrap();
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    let seqs: Vec<i64> = items
        .iter()
        .map(|e| e["seq"].as_str().unwrap().parse().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3], "event seq must stay monotonic");
    assert_eq!(items[1]["kind"], "OUTPUT_DELTA");
    assert_eq!(items[2]["kind"], "OUTPUT_DELTA");
    assert_eq!(items[1]["payload"]["nativeTurnId"], "turn-1");
    assert_eq!(items[1]["payload"]["nativeItemId"], "item-1");
    assert_eq!(items[1]["payload"]["delta"], "Hel");
    assert_eq!(items[2]["payload"]["delta"], "lo");
}

#[tokio::test]
async fn json_polling_returns_output_delta_rows_byte_shape() {
    let (store, dir) = open_store();
    seed_task(&store, "t1");
    output_delta(&store, "t1", &delta_event("chunk-1"));
    let (addr, server) = serve(store, dir).await;

    let (status, headers, mut r) = http_get(&addr, "/local/v1/tasks/t1/events?cursor=0", &[]).await;
    assert_eq!(status, 200);
    assert!(
        header(&headers, "content-type")
            .unwrap()
            .starts_with("application/json")
    );
    let body = read_body(&mut r, &headers).await;
    let page: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(page["cursorExpired"], false);
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["kind"], "OUTPUT_DELTA");
    assert_eq!(items[0]["payload"]["delta"], "chunk-1");
    server.abort();
}

#[tokio::test]
async fn sse_accept_negotiates_event_stream_and_frames() {
    let (store, dir) = open_store();
    seed_task(&store, "t1");
    {
        let s = store.lock().unwrap();
        s.event("t1", "turn/started", json!({"nativeTurnId":"turn-1"}))
            .unwrap();
    }
    output_delta(&store, "t1", &delta_event("chunk-1"));
    let (addr, server) = serve(store, dir).await;

    let (status, headers, mut r) = http_get(
        &addr,
        "/local/v1/tasks/t1/events",
        &[("Accept", "text/event-stream")],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(header(&headers, "content-type"), Some("text/event-stream"));
    assert_eq!(header(&headers, "cache-control"), Some("no-cache"));

    let mut acc = String::new();
    let f1 = read_sse_frame(&mut r, &mut acc).await.unwrap();
    let f2 = read_sse_frame(&mut r, &mut acc).await.unwrap();
    assert_event_frame(&f1, "1", "turn/started");
    assert_event_frame(&f2, "2", "OUTPUT_DELTA");
    server.abort();
}

#[tokio::test]
async fn sse_last_event_id_resumes_after_cursor() {
    let (store, dir) = open_store();
    seed_task(&store, "t1");
    {
        let s = store.lock().unwrap();
        s.event("t1", "turn/started", json!({"nativeTurnId":"turn-1"}))
            .unwrap();
    }
    output_delta(&store, "t1", &delta_event("a"));
    output_delta(&store, "t1", &delta_event("b"));
    let (addr, server) = serve(store, dir).await;

    let (status, _headers, mut r) = http_get(
        &addr,
        "/local/v1/tasks/t1/events",
        &[("Accept", "text/event-stream"), ("Last-Event-ID", "1")],
    )
    .await;
    assert_eq!(status, 200);
    let mut acc = String::new();
    let f1 = read_sse_frame(&mut r, &mut acc).await.unwrap();
    let f2 = read_sse_frame(&mut r, &mut acc).await.unwrap();
    // Resume emits only seq > Last-Event-ID.
    assert_event_frame(&f1, "2", "OUTPUT_DELTA");
    assert_event_frame(&f2, "3", "OUTPUT_DELTA");
    server.abort();
}

#[tokio::test]
async fn sse_cursor_and_last_event_id_conflict_is_400() {
    let (store, dir) = open_store();
    seed_task(&store, "t1");
    let (addr, server) = serve(Arc::clone(&store), dir).await;

    let (status, headers, mut r) = http_get(
        &addr,
        "/local/v1/tasks/t1/events?cursor=1",
        &[("Accept", "text/event-stream"), ("Last-Event-ID", "2")],
    )
    .await;
    assert_eq!(status, 400);
    let body = read_body(&mut r, &headers).await;
    assert!(body.contains("EVENT_CURSOR_CONFLICT"), "body: {body}");

    // Agreeing values are not a conflict: resume at seq > 2 and only the
    // delta appended afterwards (seq 3) arrives.
    {
        let s = store.lock().unwrap();
        s.event("t1", "turn/started", json!({"nativeTurnId":"turn-1"}))
            .unwrap();
        s.event("t1", "OUTPUT_DELTA", json!({"delta":"old"}))
            .unwrap();
    }
    let (status, _headers, mut r) = http_get(
        &addr,
        "/local/v1/tasks/t1/events?cursor=2",
        &[("Accept", "text/event-stream"), ("Last-Event-ID", "2")],
    )
    .await;
    assert_eq!(status, 200);
    let mut acc = String::new();
    output_delta(&store, "t1", &delta_event("tail"));
    let frame = read_sse_frame(&mut r, &mut acc).await.unwrap();
    assert_event_frame(&frame, "3", "OUTPUT_DELTA");
    server.abort();
}

#[tokio::test]
async fn sse_bad_cursor_rejected_like_json_face() {
    let (store, dir) = open_store();
    seed_task(&store, "t1");
    let (addr, server) = serve(store, dir).await;

    for (path, extra) in [
        (
            "/local/v1/tasks/t1/events?cursor=abc",
            vec![("Accept", "text/event-stream")],
        ),
        (
            "/local/v1/tasks/t1/events",
            vec![("Accept", "text/event-stream"), ("Last-Event-ID", "abc")],
        ),
        ("/local/v1/tasks/t1/events?cursor=abc", vec![]),
    ] {
        let (status, headers, mut r) = http_get(&addr, path, &extra).await;
        assert_eq!(status, 409, "{path}");
        let body = read_body(&mut r, &headers).await;
        assert!(body.contains("INVALID_ARGUMENT"), "{path}: {body}");
    }
    server.abort();
}

#[tokio::test]
async fn json_face_ignores_last_event_id_header() {
    let (store, dir) = open_store();
    seed_task(&store, "t1");
    output_delta(&store, "t1", &delta_event("chunk-1"));
    let (addr, server) = serve(store, dir).await;

    // A poller that happens to send Last-Event-ID still gets the JSON page.
    let (status, headers, mut r) = http_get(
        &addr,
        "/local/v1/tasks/t1/events?cursor=1",
        &[("Last-Event-ID", "99")],
    )
    .await;
    assert_eq!(status, 200);
    let body = read_body(&mut r, &headers).await;
    let page: Value = serde_json::from_str(&body).unwrap();
    assert!(page["items"].is_array());
    server.abort();
}
