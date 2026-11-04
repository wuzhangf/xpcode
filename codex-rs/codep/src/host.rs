//! Axum worker-host HTTP surface (worker-host-contract v1).
//!
//! Guard order, applied to every request before route dispatch:
//! loopback Host -> Bearer auth -> X-Worker-Local-Store -> DUPLICATE_QUERY ->
//! (POST: JSON object body, INTEGER_REQUIRED, requestKey==Idempotency-Key).
//! Anything under /local/v1 or /worker-host/v1 that is not routed returns
//! 404 FIXED_LOCAL_ROUTE_REQUIRED.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::body::to_bytes;
use axum::extract::Request;
use axum::extract::State;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::sse::Event as SseEvent;
use axum::response::sse::KeepAlive;
use axum::response::sse::Sse;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::time::Duration;
use tokio::time::sleep;

use crate::canon;
use crate::driver::Driver;
use crate::driver::SharedStore;

pub const ENGINE_ID: &str = "codep";
pub const ENGINE_VERSION: &str = "codep 0.1.0 codex-core-0.163.0a11.2";
const BODY_MAX: usize = 1024 * 1024;

pub struct Host {
    pub driver: Arc<Driver>,
    pub store: SharedStore,
    pub token: String,
    pub state_root: PathBuf,
    pub boot_id: String,
    pub model: String,
    pub projects: Vec<Value>,
}

impl Host {
    fn store_id(&self) -> String {
        self.store
            .lock()
            .map(|s| s.identity["storeId"].as_str().unwrap_or("").to_owned())
            .unwrap_or_default()
    }

    fn identity(&self) -> Value {
        self.store
            .lock()
            .map(|s| s.identity.clone())
            .unwrap_or_else(|_| json!({}))
    }

    fn task(&self, id: &str) -> Result<Value, Response> {
        if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Err(error("TASK_NOT_FOUND"));
        }
        let s = self.store.lock().map_err(|_| error("STORE_POISONED"))?;
        s.task(id).map_err(|_| error("TASK_NOT_FOUND"))
    }

    fn project(&self, id: &str) -> Result<Value, Response> {
        self.projects
            .iter()
            .find(|p| p["projectId"].as_str() == Some(id))
            .cloned()
            .ok_or_else(|| error("PROJECT_NOT_REGISTERED"))
    }
}

pub async fn run(host: Arc<Host>, port: u16) -> Result<()> {
    let app = Router::new().fallback(dispatch).with_state(host);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    tracing::info!(port, "codep worker-host listening");
    axum::serve(listener, app).await?;
    Ok(())
}

// ---------- response helpers ----------

fn reply(status: u16, v: Value) -> Response {
    (StatusCode::from_u16(status).unwrap_or(StatusCode::OK), axum::Json(v)).into_response()
}

fn status_for(code: &str) -> u16 {
    if code == "LOCAL_AUTH_REQUIRED" {
        401
    } else if code.ends_with("_NOT_FOUND") || code == "FIXED_LOCAL_ROUTE_REQUIRED" {
        404
    } else {
        409
    }
}

fn error(code: &str) -> Response {
    reply(
        status_for(code),
        json!({"code":code,"message":code,"sourceCode":null}),
    )
}

fn ct_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

// ---------- request plumbing ----------

fn query_pairs(raw: &str) -> Result<Vec<(String, String)>, Response> {
    let mut seen = HashSet::new();
    let mut out = vec![];
    if raw.is_empty() {
        return Ok(out);
    }
    for part in raw.split('&') {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        let k = url_decode(k);
        if !seen.insert(k.clone()) {
            return Err(error("DUPLICATE_QUERY"));
        }
        out.push((k, url_decode(v)));
    }
    Ok(out)
}

fn url_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        if c == '%' {
            let h = it.next().and_then(|c| c.to_digit(16));
            let l = it.next().and_then(|c| c.to_digit(16));
            if let (Some(h), Some(l)) = (h, l) {
                out.push(char::from_u32(h * 16 + l).unwrap_or('?'));
            }
        } else if c == '+' {
            out.push(' ');
        } else {
            out.push(c);
        }
    }
    out
}

async fn dispatch(State(h): State<Arc<Host>>, req: Request) -> Response {
    let host_hdr = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !(host_hdr.starts_with("127.0.0.1:") || host_hdr.starts_with("localhost:")) {
        return error("LOOPBACK_REQUIRED");
    }
    let auth = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !ct_eq(auth, &format!("Bearer {}", h.token)) {
        return error("LOCAL_AUTH_REQUIRED");
    }
    if let Some(v) = req
        .headers()
        .get("x-worker-local-store")
        .and_then(|v| v.to_str().ok())
    {
        if v != h.store_id() {
            return error("STORE_IDENTITY_CHANGED");
        }
    }
    let query = match query_pairs(req.uri().query().unwrap_or("")) {
        Ok(q) => q,
        Err(r) => return r,
    };
    let wants_sse = req
        .headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("text/event-stream"))
        .unwrap_or(false);
    let last_event_id = req
        .headers()
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let method = req.method().clone();
    let idem = req
        .headers()
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let path = req.uri().path().to_string();

    let mut body = Value::Null;
    if method == Method::POST {
        let bytes = match to_bytes(req.into_body(), BODY_MAX).await {
            Ok(b) => b,
            Err(_) => return error("REQUEST_TOO_LARGE"),
        };
        if bytes.is_empty() {
            return error("REQUEST_BODY_REQUIRED");
        }
        body = match serde_json::from_slice::<Value>(&bytes) {
            Ok(v) => v,
            Err(_) => return error("INVALID_ARGUMENT"),
        };
        if !body.is_object() {
            return error("INVALID_ARGUMENT");
        }
        if canon::canonical(&body).is_err() {
            return error("INTEGER_REQUIRED");
        }
        if let Some(rk) = body.get("requestKey").and_then(Value::as_str) {
            if idem.as_deref() != Some(rk) {
                return error("KEY_MISMATCH");
            }
        }
    }

    let seg: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let get = |k: &str| {
        query
            .iter()
            .find(|(q, _)| q == k)
            .map(|(_, v)| v.clone())
    };
    match (method.as_str(), seg.as_slice()) {
        ("GET", ["worker-host", "v1", "manifest"]) => manifest(&h),
        ("GET", ["worker-host", "v1", "capabilities"]) => capabilities(),
        ("GET", ["local", "v1", "identity"]) => {
            let id = h.identity();
            reply(
                200,
                json!({
                    "schemaVersion":"worker-local/1",
                    "storeId":id["storeId"],
                    "bootId":h.boot_id,
                    "authorityId":id["authorityId"],
                    "workerId":id["workerId"],
                }),
            )
        }
        ("GET", ["local", "v1", "engines"]) => engines(&h),
        ("GET", ["local", "v1", "projects"]) => {
            let items: Vec<Value> = h
                .projects
                .iter()
                .map(|p| {
                    json!({
                        "projectId":p["projectId"],
                        "displayName":p["displayName"],
                        "path":p["path"],
                        "headRevision":p["headRevision"],
                        "hasUncommittedChanges":false,
                    })
                })
                .collect();
            reply(200, json!({"items":items}))
        }
        ("POST", ["local", "v1", "task-preflights"]) => match preflight(&h, &body) {
            Ok(v) => reply(200, v),
            Err(r) => r,
        },
        ("POST", ["local", "v1", "tasks"]) => create_task(&h, &body).await,
        ("GET", ["local", "v1", "tasks"]) => list_tasks(&h, get("limit")),
        ("GET", ["local", "v1", "task-requests", key]) => create_receipt(&h, key),
        ("GET", ["local", "v1", "tasks", id]) => match h.task(id) {
            Ok(t) => reply(200, public_task(&t)),
            Err(r) => r,
        },
        ("POST", ["local", "v1", "tasks", id, "commands"]) => command(&h, id, &body).await,
        ("GET", ["local", "v1", "tasks", id, "commands", key]) => op_receipt(&h, id, key),
        ("POST", ["local", "v1", "tasks", id, "messages"]) => task_message(&h, id, &body).await,
        ("GET", ["local", "v1", "tasks", id, "messages"]) => task_messages(&h, id, &get),
        ("GET", ["local", "v1", "tasks", id, "interaction-requests", key]) => {
            op_receipt(&h, id, key)
        }
        ("GET", ["local", "v1", "tasks", id, "input-requests"]) => match h.task(id) {
            Ok(_) => reply(200, json!({"items":[]})),
            Err(r) => r,
        },
        ("POST", ["local", "v1", "tasks", id, "input-requests", _irid, "responses"]) => {
            match h.task(id) {
                Ok(_) => error("NATIVE_INPUT_EXPIRED"),
                Err(r) => r,
            }
        }
        ("GET", ["local", "v1", "tasks", id, "events"]) => {
            if wants_sse {
                task_events_sse(&h, id, get("cursor").or(last_event_id))
            } else {
                task_events(&h, id, &get)
            }
        }
        ("GET", ["local", "v1", "tasks", id, "output"]) => match h.task(id) {
            Ok(t) => reply(
                200,
                json!({"text":t["output"],"coverage":"BOUNDED_PUBLIC_MESSAGES"}),
            ),
            Err(r) => r,
        },
        ("GET", ["local", "v1", "tasks", id, "metrics"]) => match h.task(id) {
            Ok(t) => {
                let usage = t["usage"].clone();
                reply(
                    200,
                    json!({
                        "cost":null,
                        "costCoverage":"UNKNOWN",
                        "usageCoverage": if usage.is_null() {"NO_MODEL"} else {"NATIVE_TASK_TOTAL"},
                        "configuredModel":h.model,
                        "usage": if usage.is_null() { json!({"totalTokens":"0"}) } else { usage },
                    }),
                )
            }
            Err(r) => r,
        },
        ("GET", ["local", "v1", "tasks", id, "artifacts"]) => match h.task(id) {
            Ok(_) => reply(200, json!({"items":[]})),
            Err(r) => r,
        },
        ("GET", ["local", "v1", "tasks", id, "artifacts", _aid]) => match h.task(id) {
            Ok(_) => error("ARTIFACT_NOT_FOUND"),
            Err(r) => r,
        },
        _ => error("FIXED_LOCAL_ROUTE_REQUIRED"),
    }
}

// ---------- discovery ----------

fn capability_items() -> Value {
    json!([
        {"id":"execution.messages","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"PUBLIC_FINAL_MESSAGES","evidenceRef":null,"reasonCode":null,"limits":{}},
        {"id":"execution.stop","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"CANCEL_TURN","evidenceRef":null,"reasonCode":null,"limits":{}},
        {"id":"execution.steer","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"IN_TURN_STEER","evidenceRef":null,"reasonCode":null,"limits":{}},
        {"id":"execution.follow_up","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"POST_TURN_FOLLOW_UP","evidenceRef":null,"reasonCode":null,"limits":{}},
        {"id":"execution.pause","implementation":"NOT_IMPLEMENTED","qualification":"GAP","availability":"UNAVAILABLE","semantics":"PAUSE_TURN","evidenceRef":null,"reasonCode":"UNSUPPORTED","limits":{}},
        {"id":"execution.resume","implementation":"NOT_IMPLEMENTED","qualification":"GAP","availability":"UNAVAILABLE","semantics":"RESUME_TURN","evidenceRef":null,"reasonCode":"UNSUPPORTED","limits":{}},
        {"id":"task.artifacts","implementation":"NOT_IMPLEMENTED","qualification":"GAP","availability":"UNAVAILABLE","semantics":"TASK_ARTIFACTS","evidenceRef":null,"reasonCode":"UNSUPPORTED","limits":{}},
        {"id":"task.input_requests","implementation":"NOT_IMPLEMENTED","qualification":"GAP","availability":"UNAVAILABLE","semantics":"PENDING_INPUT_REQUESTS","evidenceRef":null,"reasonCode":"UNSUPPORTED","limits":{}}
    ])
}

fn manifest(h: &Host) -> Response {
    let id = h.identity();
    reply(
        200,
        json!({
            "authorityId":id["authorityId"],
            "storeId":id["storeId"],
            "workerId":id["workerId"],
            "bootId":h.boot_id,
            "schemaVersion":"worker-host/1",
            "storeGuard":"HEADER_V1",
            "engine":{"id":ENGINE_ID,"version":ENGINE_VERSION},
            "contracts":["worker-host/1","worker-local/1"],
            "capabilities":capability_items(),
            "supportedPaths":["GET /manifest","GET /capabilities"],
        }),
    )
}

fn capabilities() -> Response {
    reply(
        200,
        json!({"schemaVersion":"worker-capabilities/1","items":capability_items()}),
    )
}

fn engines(h: &Host) -> Response {
    reply(
        200,
        json!({"items":[{
            "engineId":ENGINE_ID,
            "engineVersion":ENGINE_VERSION,
            "version":ENGINE_VERSION,
            "profileId":"MANAGED",
            "pause":false,"resume":false,"stop":true,"steer":true,"followUp":true,
            "capabilities":{},
            "executionPolicy":{},
            "configuredModel":h.model,
        }]}),
    )
}

// ---------- preflight ----------

fn preflight_response(h: &Host, project_id: &str, engine: &str) -> Result<Value, Response> {
    if engine != ENGINE_ID {
        return Err(error("ENGINE_NOT_REGISTERED"));
    }
    let p = h.project(project_id)?;
    let base = json!({
        "projectId":p["projectId"],
        "engineId":ENGINE_ID,
        "engineVersion":ENGINE_VERSION,
        "profileId":"MANAGED",
        "profileHome":null,
        "loadsUserExtensions":false,
        "headRevision":p["headRevision"],
        "hasUncommittedChanges":false,
        "workingCopyPolicy":"COMMITTED_BASE_ONLY",
        "configuredModel":h.model,
    });
    let rev = canon::canonical_sha256(&base).map_err(|_| error("INVALID_ARGUMENT"))?;
    let mut out = base;
    out["optionsRevision"] = json!(rev);
    Ok(out)
}

fn preflight(h: &Host, body: &Value) -> Result<Value, Response> {
    let o = fields(body, &["projectId", "engineId", "policyPresetId"])?;
    let project_id = req_str(o, "projectId")?;
    let engine = req_str(o, "engineId")?;
    preflight_response(h, project_id, engine)
}

// ---------- field helpers ----------

fn fields<'a>(body: &'a Value, allowed: &[&str]) -> Result<&'a Map<String, Value>, Response> {
    let o = body.as_object().ok_or_else(|| error("INVALID_ARGUMENT"))?;
    for k in o.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(error("UNEXPECTED_FIELD"));
        }
    }
    Ok(o)
}

fn req_str<'a>(o: &'a Map<String, Value>, k: &str) -> Result<&'a str, Response> {
    o.get(k)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| error("INVALID_ARGUMENT"))
}

fn store_of(h: &Host) -> Result<std::sync::MutexGuard<'_, crate::store::Store>, Response> {
    h.store.lock().map_err(|_| error("STORE_POISONED"))
}

fn receipt_json(key: &str, state: &str, detail: Value) -> Value {
    json!({"kind":"WORKER_LOCAL","requestKey":key,"state":state,"detail":detail})
}

/// Persist a REJECTED receipt for a request that failed validation/guards, so
/// a replay of the same request key sees the same settled outcome.
fn rejected(h: &Host, scope: &str, key: &str, body: &Value, task: &str, detail: &str) {
    let receipt = receipt_json(key, "REJECTED", json!(detail));
    if let Ok(mut s) = h.store.lock() {
        let _ = s.accept(scope, key, body, &receipt, task);
        let _ = s.receipt(scope, key, "REJECTED", &receipt);
    }
}

// ---------- task lifecycle ----------

fn public_task(t: &Value) -> Value {
    let mut v = t.clone();
    let state = t["state"].as_str().unwrap_or("");
    let quiescent = t["quiescent"].as_bool().unwrap_or(true);
    let live = matches!(
        state,
        "PREPARING" | "QUEUED" | "STARTING" | "RUNNING" | "STOP_REQUESTED"
    ) && !quiescent;
    v["capabilities"] = json!({
        "stop": live,
        "steer": state == "RUNNING" && !quiescent,
        "followUp": state == "SUCCEEDED" && quiescent,
        "pause": false,
        "resume": false,
    });
    v
}

async fn create_task(h: &Arc<Host>, body: &Value) -> Response {
    let o = match fields(
        body,
        &[
            "requestKey",
            "projectId",
            "engineId",
            "optionsRevision",
            "instruction",
            "budget",
            "writeScope",
            "environment",
            "mcpServers",
            "skills",
            "hooks",
            "policyPresetId",
            "materials",
        ],
    ) {
        Ok(o) => o,
        Err(r) => return r,
    };
    let (key, project_id, engine, options_revision, instruction) = match (|| {
        Ok::<_, Response>((
            req_str(o, "requestKey")?,
            req_str(o, "projectId")?,
            req_str(o, "engineId")?,
            req_str(o, "optionsRevision")?,
            req_str(o, "instruction")?,
        ))
    })() {
        Ok(v) => v,
        Err(r) => return r,
    };
    if instruction.len() > 16384 {
        return error("INVALID_ARGUMENT");
    }
    {
        let s = match store_of(h) {
            Ok(s) => s,
            Err(r) => return r,
        };
        match s.existing("create", key, body) {
            Ok(Some(_)) => {
                // Replay returns the same task record; the stored result may
                // have been refreshed by receipt phases, so resolve by key.
                let t = s
                    .tasks()
                    .unwrap_or_default()
                    .into_iter()
                    .find(|t| t["requestKey"].as_str() == Some(key));
                return match t {
                    Some(t) => reply(202, public_task(&t)),
                    None => reply(202, s.lookup("create", key).unwrap_or(json!({}))),
                };
            }
            Ok(None) => {}
            Err(_) => return error("IDEMPOTENCY_CONFLICT"),
        }
    }
    let preflight = match preflight_response(h, project_id, engine) {
        Ok(p) => p,
        Err(r) => return r,
    };
    if preflight["optionsRevision"].as_str() != Some(options_revision) {
        return error("PREFLIGHT_CHANGED");
    }
    let task_id = format!("native-{}", &canon::new_id()[..24]);
    let workspace = h
        .state_root
        .join("workspaces")
        .join(&task_id)
        .to_string_lossy()
        .to_string();
    let task = json!({
        "taskId":task_id,
        "engineId":ENGINE_ID,
        "engineVersion":ENGINE_VERSION,
        "projectId":project_id,
        "requestKey":key,
        "state":"PREPARING",
        "stateVersion":"1",
        "generation":"1",
        "nativeSessionId":null,
        "nativeTurnId":null,
        "currentCommandId":key,
        "quiescent":false,
        "configuredModel":h.model,
        "workspace":workspace,
        "segmentId":format!("seg-{}",canon::new_id()),
        "ownerEpoch":h.boot_id,
        "output":"",
        "publicItemIds":[],
        "turns":{},
        "usage":null,
        "createdAt":canon::now(),
    });
    {
        let mut s = match store_of(h) {
            Ok(s) => s,
            Err(r) => return r,
        };
        if let Err(e) = s.accept_task(&task, key, body) {
            tracing::warn!("accept_task failed: {e}");
            return error("INTERNAL");
        }
    }
    let ws_path = h.state_root.join("workspaces").join(&task_id);
    let final_task = match h
        .driver
        .start_task(&task_id, &ws_path, instruction, key)
        .await
    {
        Ok(()) => match store_of(h).and_then(|s| s.task(&task_id).map_err(|_| error("INTERNAL"))) {
            Ok(t) => t,
            Err(r) => return r,
        },
        Err(e) => {
            tracing::warn!(task_id, "task start failed: {e}");
            let t = {
                let mut s = match store_of(h) {
                    Ok(s) => s,
                    Err(r) => return r,
                };
                let t = s
                    .update(&task_id, |t| {
                        t["state"] = json!("FAILED");
                        t["quiescent"] = json!(true);
                        t["reasonCode"] = json!("NATIVE_START_FAILED");
                    })
                    .unwrap_or(task.clone());
                let _ = s.event(&task_id, "task.failed", json!({"detail":format!("{e}")}));
                let _ = s.receipt(
                    "create",
                    key,
                    "FAILED",
                    &receipt_json(key, "FAILED", json!("native thread start failed")),
                );
                t
            };
            h.driver.retire(&task_id).await;
            t
        }
    };
    // Refresh the idempotent-replay result so it carries nativeSessionId.
    let projected = public_task(&final_task);
    {
        let mut s = match store_of(h) {
            Ok(s) => s,
            Err(r) => return r,
        };
        let _ = s.receipt("create", key, "ACCEPTED", &projected);
    }
    reply(202, projected)
}

fn create_receipt(h: &Host, key: &str) -> Response {
    let s = match store_of(h) {
        Ok(s) => s,
        Err(r) => return r,
    };
    // The create-scope ledger row proves the request existed; the task record
    // (looked up by requestKey) is the receipt body the client wants.
    let found = s
        .tasks()
        .unwrap_or_default()
        .into_iter()
        .find(|t| t["requestKey"].as_str() == Some(key));
    match found {
        Some(t) => reply(200, public_task(&t)),
        None => match s.lookup("create", key) {
            Ok(v) => reply(200, v),
            Err(_) => error("REQUEST_NOT_FOUND"),
        },
    }
}

fn list_tasks(h: &Host, limit: Option<String>) -> Response {
    let limit = match limit {
        Some(s) => match s.parse::<usize>() {
            Ok(n) if n >= 1 => n.min(50),
            _ => return error("INVALID_ARGUMENT"),
        },
        None => 50,
    };
    let s = match store_of(h) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let tasks = s.tasks().unwrap_or_default();
    let items: Vec<Value> = tasks
        .iter()
        .rev()
        .take(limit)
        .map(|t| {
            json!({
                "taskId":t["taskId"],
                "projectId":t["projectId"],
                "engineId":t["engineId"],
                "engineVersion":t["engineVersion"],
                "state":t["state"],
            })
        })
        .collect();
    reply(200, json!({"items":items}))
}

fn op_receipt(h: &Host, task: &str, key: &str) -> Response {
    if let Err(r) = h.task(task) {
        return r;
    }
    let s = match store_of(h) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match s.lookup_auto(task, key) {
        Ok(v) => reply(200, v),
        Err(_) => error("REQUEST_NOT_FOUND"),
    }
}

// ---------- commands (STOP / PAUSE / RESUME) ----------

fn guard_fields(t: &Value, o: &Map<String, Value>) -> Result<(), Response> {
    for (field, current) in [
        ("generation", &t["generation"]),
        ("nativeSessionId", &t["nativeSessionId"]),
        ("nativeTurnId", &t["nativeTurnId"]),
        ("targetCommandId", &t["currentCommandId"]),
    ] {
        let sent = o.get(field).cloned().unwrap_or(Value::Null);
        if sent != *current {
            return Err(error("NATIVE_TARGET_CHANGED"));
        }
    }
    Ok(())
}

async fn command(h: &Arc<Host>, id: &str, body: &Value) -> Response {
    let o = match fields(
        body,
        &[
            "requestKey",
            "intent",
            "generation",
            "nativeSessionId",
            "nativeTurnId",
            "targetCommandId",
        ],
    ) {
        Ok(o) => o.clone(),
        Err(r) => return r,
    };
    let key = match req_str(&o, "requestKey") {
        Ok(k) => k.to_owned(),
        Err(r) => return r,
    };
    let intent = match req_str(&o, "intent") {
        Ok(k) => k.to_owned(),
        Err(r) => return r,
    };
    let t = match h.task(id) {
        Ok(t) => t,
        Err(r) => return r,
    };
    {
        let s = match store_of(h) {
            Ok(s) => s,
            Err(r) => return r,
        };
        match s.existing(id, &key, body) {
            Ok(Some(v)) => return reply(202, v),
            Ok(None) => {}
            Err(_) => return error("IDEMPOTENCY_CONFLICT"),
        }
    }
    if let Err(r) = guard_fields(&t, &o) {
        rejected(h, id, &key, body, id, "NATIVE_TARGET_CHANGED");
        return r;
    }
    match intent.as_str() {
        "PAUSE" | "RESUME" => {
            rejected(h, id, &key, body, id, "NATIVE_ACTION_UNSUPPORTED");
            error("NATIVE_ACTION_UNSUPPORTED")
        }
        "STOP" => {
            let state = t["state"].as_str().unwrap_or("");
            let quiescent = t["quiescent"].as_bool().unwrap_or(true);
            let live = matches!(
                state,
                "PREPARING" | "QUEUED" | "STARTING" | "RUNNING" | "STOP_REQUESTED"
            ) && !quiescent;
            if !live {
                rejected(h, id, &key, body, id, "NATIVE_NOT_RUNNING");
                return error("NATIVE_NOT_RUNNING");
            }
            let receipt = receipt_json(&key, "ACCEPTED", json!({"intent":"STOP"}));
            {
                let mut s = match store_of(h) {
                    Ok(s) => s,
                    Err(r) => return r,
                };
                if s.accept(id, &key, body, &receipt, id).is_err() {
                    return error("INTERNAL");
                }
                if s
                    .update(id, |t| {
                        t["state"] = json!("STOP_REQUESTED");
                        t["pendingStopKey"] = json!(key);
                    })
                    .is_err()
                {
                    return error("INTERNAL");
                }
            }
            let driver = Arc::clone(&h.driver);
            let store = Arc::clone(&h.store);
            let task = id.to_owned();
            let stop_key = key.clone();
            tokio::spawn(async move {
                let _ = driver.interrupt(&task).await;
                // TurnAborted settles CANCELLED when a turn was active; if it
                // does not arrive within the grace window, settle here.
                for _ in 0..200 {
                    sleep(Duration::from_millis(50)).await;
                    let done = store
                        .lock()
                        .ok()
                        .and_then(|s| s.task(&task).ok())
                        .map(|t| {
                            t["state"].as_str() == Some("CANCELLED")
                                && t["quiescent"].as_bool() == Some(true)
                        })
                        .unwrap_or(true);
                    if done {
                        return;
                    }
                }
                if let Ok(mut s) = store.lock() {
                    let cmd = s
                        .task(&task)
                        .ok()
                        .and_then(|t| t["currentCommandId"].as_str().map(str::to_owned))
                        .unwrap_or_default();
                    let _ = s.update(&task, |t| {
                        if t["state"].as_str() != Some("CANCELLED") {
                            t["state"] = json!("CANCELLED");
                            t["quiescent"] = json!(true);
                            t["reasonCode"] = json!("TASK_STOPPED");
                        }
                        t.as_object_mut().map(|m| m.remove("pendingStopKey"));
                    });
                    let _ = s.event(&task, "task.cancelled", json!({"reasonCode":"TASK_STOPPED"}));
                    let _ = s.receipt_auto(
                        &task,
                        &stop_key,
                        "APPLIED",
                        &receipt_json(&stop_key, "APPLIED", json!("stop applied")),
                    );
                    if !cmd.is_empty() && cmd != stop_key {
                        let _ = s.receipt_auto(
                            &task,
                            &cmd,
                            "CANCELLED",
                            &receipt_json(&cmd, "CANCELLED", json!("turn interrupted")),
                        );
                    }
                }
                driver.retire(&task).await;
            });
            reply(202, receipt)
        }
        _ => {
            rejected(h, id, &key, body, id, "INVALID_ARGUMENT");
            error("INVALID_ARGUMENT")
        }
    }
}

// ---------- task messages (steer / follow-up) ----------

async fn task_message(h: &Arc<Host>, id: &str, body: &Value) -> Response {
    let o = match fields(
        body,
        &[
            "requestKey",
            "kind",
            "content",
            "generation",
            "nativeSessionId",
            "nativeTurnId",
            "targetCommandId",
        ],
    ) {
        Ok(o) => o.clone(),
        Err(r) => return r,
    };
    let (key, kind, content) = match (|| {
        Ok::<_, Response>((
            req_str(&o, "requestKey")?,
            req_str(&o, "kind")?,
            req_str(&o, "content")?,
        ))
    })() {
        Ok((a, b, c)) => (a.to_owned(), b.to_owned(), c.to_owned()),
        Err(r) => return r,
    };
    if content.len() > 16384 {
        return error("INVALID_ARGUMENT");
    }
    let t = match h.task(id) {
        Ok(t) => t,
        Err(r) => return r,
    };
    {
        let s = match store_of(h) {
            Ok(s) => s,
            Err(r) => return r,
        };
        match s.existing(id, &key, body) {
            Ok(Some(v)) => return reply(202, v),
            Ok(None) => {}
            Err(_) => return error("IDEMPOTENCY_CONFLICT"),
        }
    }
    if let Err(r) = guard_fields(&t, &o) {
        rejected(h, id, &key, body, id, "NATIVE_TARGET_CHANGED");
        return r;
    }
    let receipt = receipt_json(&key, "ACCEPTED", json!({"currentCommandId":key}));
    let state = t["state"].as_str().unwrap_or("");
    let quiescent = t["quiescent"].as_bool().unwrap_or(true);
    match kind.as_str() {
        "NATIVE_STEER" => {
            let turn = t["nativeTurnId"].as_str().unwrap_or_default().to_owned();
            if state != "RUNNING" || quiescent || turn.is_empty() {
                rejected(h, id, &key, body, id, "NATIVE_NOT_RUNNING");
                return error("NATIVE_NOT_RUNNING");
            }
            if h.driver.live_thread(id).await.is_none() {
                rejected(h, id, &key, body, id, "NATIVE_WRITER_UNPROVEN");
                return error("NATIVE_WRITER_UNPROVEN");
            }
            {
                let mut s = match store_of(h) {
                    Ok(s) => s,
                    Err(r) => return r,
                };
                if s
                    .accept_user(id, &key, body, &receipt, &content, false)
                    .is_err()
                {
                    return error("INTERNAL");
                }
            }
            let d = Arc::clone(&h.driver);
            let (task, k, c) = (id.to_owned(), key.clone(), content.clone());
            tokio::spawn(async move {
                let _ = d.steer(&task, &k, &c, &turn).await;
            });
            reply(202, receipt)
        }
        "NATIVE_FOLLOW_UP" => {
            if state != "SUCCEEDED" || !quiescent {
                rejected(h, id, &key, body, id, "NATIVE_NOT_IDLE");
                return error("NATIVE_NOT_IDLE");
            }
            if h.driver.live_thread(id).await.is_none() {
                rejected(h, id, &key, body, id, "NATIVE_WRITER_UNPROVEN");
                return error("NATIVE_WRITER_UNPROVEN");
            }
            {
                let mut s = match store_of(h) {
                    Ok(s) => s,
                    Err(r) => return r,
                };
                if s
                    .accept_user(id, &key, body, &receipt, &content, true)
                    .is_err()
                {
                    return error("INTERNAL");
                }
            }
            let d = Arc::clone(&h.driver);
            let (task, k, c) = (id.to_owned(), key.clone(), content.clone());
            tokio::spawn(async move {
                let _ = d.follow_up(&task, &k, &c).await;
            });
            reply(202, receipt)
        }
        _ => {
            rejected(h, id, &key, body, id, "INVALID_ARGUMENT");
            error("INVALID_ARGUMENT")
        }
    }
}

// ---------- read surfaces ----------

fn parse_i64(v: Option<String>) -> Result<Option<i64>, Response> {
    match v {
        None => Ok(None),
        Some(s) => s
            .parse::<i64>()
            .map(Some)
            .map_err(|_| error("INVALID_ARGUMENT")),
    }
}

fn task_messages(h: &Host, id: &str, get: &dyn Fn(&str) -> Option<String>) -> Response {
    if let Err(r) = h.task(id) {
        return r;
    }
    let cursor = match parse_i64(get("cursor")) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let before = match parse_i64(get("before")) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if cursor.is_some() && before.is_some() {
        return error("INVALID_ARGUMENT");
    }
    let limit = match get("limit") {
        Some(s) => match s.parse::<i64>() {
            Ok(n) if (1..=100).contains(&n) => n,
            _ => return error("INVALID_ARGUMENT"),
        },
        None => 100,
    };
    let mut s = match store_of(h) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match s.messages(id, cursor, before, limit) {
        Ok(v) => reply(200, v),
        Err(e) => {
            let code = format!("{e}");
            error(match code.as_str() {
                "MESSAGE_PAGE_TOO_LARGE" => "MESSAGE_PAGE_TOO_LARGE",
                _ => "INVALID_ARGUMENT",
            })
        }
    }
}

fn task_events(h: &Host, id: &str, get: &dyn Fn(&str) -> Option<String>) -> Response {
    if let Err(r) = h.task(id) {
        return r;
    }
    let cursor = match parse_i64(get("cursor")) {
        Ok(v) => v.unwrap_or(0),
        Err(r) => return r,
    };
    let limit = match get("limit") {
        Some(s) => match s.parse::<i64>() {
            Ok(n) if (1..=200).contains(&n) => n,
            _ => return error("INVALID_ARGUMENT"),
        },
        None => 50,
    };
    let s = match store_of(h) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match s.events(id, cursor, limit) {
        Ok(v) => reply(200, v),
        Err(_) => error("INTERNAL"),
    }
}

/// SSE branch of the events leaf: same durable event objects as the polling
/// leaf, framed as `id: <seq>` / `event: <kind>` / `data: <event json>`; `Last-Event-ID` or the
/// `cursor` query param resumes mid-sequence. The stream stays open until the
/// client disconnects; axum drops the stream on disconnect.
fn task_events_sse(h: &Arc<Host>, id: &str, cursor: Option<String>) -> Response {
    if let Err(r) = h.task(id) {
        return r;
    }
    let start = cursor
        .as_deref()
        .and_then(|c| c.parse::<i64>().ok())
        .unwrap_or(0);
    let host = Arc::clone(h);
    let task = id.to_owned();
    let stream = futures::stream::unfold((VecDeque::<Value>::new(), start), move |(mut pending, mut seq)| {
        let host = Arc::clone(&host);
        let task = task.clone();
        async move {
            loop {
                if let Some(ev) = pending.pop_front() {
                    let out = SseEvent::default()
                        .id(ev["seq"].as_str().unwrap_or_default())
                        .event(ev["kind"].as_str().unwrap_or("message"))
                        .data(ev.to_string());
                    return Some((Ok::<SseEvent, Infallible>(out), (pending, seq)));
                }
                let page = match host.store.lock() {
                    Ok(s) => s
                        .events(&task, seq, 200)
                        .ok()
                        .and_then(|v| v["items"].as_array().cloned()),
                    Err(_) => None,
                };
                match page {
                    Some(items) if !items.is_empty() => {
                        for ev in &items {
                            if let Some(n) = ev["seq"].as_str().and_then(|s| s.parse::<i64>().ok()) {
                                seq = n;
                            }
                        }
                        pending.extend(items);
                    }
                    _ => sleep(Duration::from_millis(150)).await,
                }
            }
        }
    });
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}
