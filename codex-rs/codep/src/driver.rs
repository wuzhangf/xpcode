//! In-process codex-core driver: one `ThreadManager`, one codex thread per
//! task, per-thread event loops projecting `EventMsg` into the durable store.
//!
//! Approval posture: `AskForApproval::Never` + `SandboxMode::DangerFullAccess`
//! means core should not surface approvals; the event loop still auto-answers
//! any surviving approval/elicitation (Approved for exec/patch/permissions,
//! Decline for elicitations, empty answers for user-input, failure for
//! dynamic tool calls) and records an `approval.auto_answered` audit event.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_config::LoaderOverrides;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use codex_core::config::ConfigOverrides;
use codex_core_api::AbsolutePathBuf;
use codex_core_api::Arg0DispatchPaths;
use codex_core_api::AskForApproval;
use codex_core_api::AuthManager;
use codex_core_api::CodexAppsToolsCache;
use codex_core_api::CodexHomeUserInstructionsProvider;
use codex_core_api::CodexThread;
use codex_core_api::EnvironmentManager;
use codex_core_api::EventMsg;
use codex_core_api::ExecServerRuntimeOptions;
use codex_core_api::ExtensionRegistryBuilder;
use codex_core_api::NewThread;
use codex_core_api::SessionSource;
use codex_core_api::StartIfIdleSubmission;
use codex_core_api::StartThreadOptions;
use codex_core_api::SteerSubmission;
use codex_core_api::ThreadManager;
use codex_core_api::TurnInput;
use codex_core_api::TurnInputRequest;
use codex_core_api::UserInput;
use codex_core_api::build_models_manager;
use codex_core_api::init_state_db;
use codex_core_api::install_image_generation_extension;
use codex_core_api::local_agent_graph_store_from_state_db;
use codex_core_api::passthrough_image_store;
use codex_core_api::resolve_installation_id;
use codex_core_api::thread_store_from_config;
use codex_protocol::ThreadId;
use codex_protocol::approvals::ElicitationAction;
use codex_protocol::config_types::SandboxMode;
use codex_protocol::dynamic_tools::DynamicToolCallOutputContentItem;
use codex_protocol::dynamic_tools::DynamicToolResponse;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::AgentMessageContentDeltaEvent;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ReviewDecision;
use codex_protocol::request_permissions::PermissionGrantScope;
use codex_protocol::request_permissions::RequestPermissionsResponse;
use codex_protocol::request_user_input::RequestUserInputAnswer;
use codex_protocol::request_user_input::RequestUserInputResponse;
use serde_json::Value;
use serde_json::json;
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;

use crate::store::Store;

pub type SharedStore = Arc<Mutex<Store>>;

pub struct TaskLive {
    pub thread: Arc<CodexThread>,
    pub thread_id: ThreadId,
}

pub struct Driver {
    pub tm: ThreadManager,
    pub base_config: Config,
    pub store: SharedStore,
    /// task_id -> live root thread
    tasks: AsyncMutex<HashMap<String, TaskLive>>,
    /// codex thread_id -> owning task (roots + discovered children)
    thread_task: AsyncMutex<HashMap<ThreadId, String>>,
}

/// Boot sequence copied from codex-rs/thread-manager-sample/src/main.rs.
pub async fn boot(arg0: &Arg0DispatchPaths, state_root: &Path, store: SharedStore) -> Result<Arc<Driver>> {
    let codex_home = state_root.join("codex-home");
    if !codex_home.join("config.toml").is_file() {
        bail!("LOCAL_STATE_NOT_INITIALIZED")
    }
    let config = ConfigBuilder::default()
        .codex_home(codex_home.clone())
        .harness_overrides(ConfigOverrides {
            approval_policy: Some(AskForApproval::Never),
            sandbox_mode: Some(SandboxMode::DangerFullAccess),
            model_provider: Some("codep".to_string()),
            codex_self_exe: arg0.codex_self_exe.clone(),
            codex_linux_sandbox_exe: arg0.codex_linux_sandbox_exe.clone(),
            ephemeral: Some(true),
            ..Default::default()
        })
        .loader_overrides(LoaderOverrides {
            ignore_login_requirements: true,
            ..Default::default()
        })
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let state_db = init_state_db(&config).await;
    let auth_manager =
        AuthManager::shared_from_config(&config, /*enable_codex_api_key_env*/ false).await?;
    let local_runtime_paths = ExecServerRuntimeOptions::from_optional_paths(
        config.codex_self_exe.clone(),
        config.codex_linux_sandbox_exe.clone(),
    )?;
    let thread_store = thread_store_from_config(&config, state_db.clone());
    let environment_manager = Arc::new(
        EnvironmentManager::from_codex_home(
            config.codex_home.clone(),
            Some(local_runtime_paths),
            config.http_client_factory(),
        )
        .await?,
    );
    let installation_id = resolve_installation_id(&config.codex_home).await?;
    let user_instructions_provider = Arc::new(CodexHomeUserInstructionsProvider::new(
        config.codex_home.clone(),
    ));
    let mut extensions = ExtensionRegistryBuilder::<Config>::new();
    install_image_generation_extension(&mut extensions, auth_manager.clone(), |config: &Config| {
        Some(config.codex_home.clone())
    });
    let tm = ThreadManager::new(
        &config,
        Arc::clone(&auth_manager),
        build_models_manager(&config, auth_manager),
        CodexAppsToolsCache::default(),
        SessionSource::Custom("codep".to_string()),
        environment_manager,
        Arc::new(extensions.build()),
        user_instructions_provider,
        /*analytics_events_client*/ None,
        passthrough_image_store(),
        Arc::clone(&thread_store),
        local_agent_graph_store_from_state_db(state_db.as_ref()),
        installation_id,
        /*attestation_provider*/ None,
        /*external_time_provider*/ None,
    );

    let driver = Arc::new(Driver {
        tm,
        base_config: config,
        store,
        tasks: AsyncMutex::new(HashMap::new()),
        thread_task: AsyncMutex::new(HashMap::new()),
    });

    // Subagent/child threads: attach a consumer each so their approvals never
    // hang unanswered (single-consumer event stream per thread).
    let mut created = driver.tm.subscribe_thread_created();
    let watcher = Arc::clone(&driver);
    tokio::spawn(async move {
        loop {
            match created.recv().await {
                Ok(thread_id) => {
                    if watcher.thread_task.lock().await.contains_key(&thread_id) {
                        continue; // root thread already has a loop
                    }
                    if let Ok(thread) = watcher.tm.get_thread(thread_id).await {
                        // Attribute the child to its parent's task for audit.
                        let parent_task = match thread.config_snapshot().await.parent_thread_id {
                            Some(parent) => watcher
                                .thread_task
                                .lock()
                                .await
                                .get(&parent)
                                .cloned(),
                            None => None,
                        };
                        if let Some(task) = parent_task {
                            watcher
                                .thread_task
                                .lock()
                                .await
                                .insert(thread_id, task.clone());
                            let d = Arc::clone(&watcher);
                            tokio::spawn(event_loop(d, task, thread, thread_id, true));
                        } else {
                            let d = Arc::clone(&watcher);
                            tokio::spawn(event_loop(d, String::new(), thread, thread_id, true));
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    Ok(driver)
}

fn user_input_request(text: &str, client_key: &str) -> TurnInputRequest {
    TurnInputRequest::new(TurnInput::UserInput {
        content: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
        client_id: Some(client_key.to_string()),
    })
}

impl Driver {
    /// Look up the live root thread for a task.
    pub async fn live_thread(&self, task: &str) -> Option<(ThreadId, Arc<CodexThread>)> {
        self.tasks
            .lock()
            .await
            .get(task)
            .map(|l| (l.thread_id, Arc::clone(&l.thread)))
    }

    /// Create the codex thread for a task and record nativeSessionId, then
    /// submit the first turn. Any failure leaves the task marked FAILED by
    /// the caller (host launch wrapper).
    pub async fn start_task(self: &Arc<Self>, task: &str, workspace: &Path, text: &str, key: &str) -> Result<()> {
        std::fs::create_dir_all(workspace)?;
        let mut cfg = self.base_config.clone();
        let cwd = AbsolutePathBuf::from_absolute_path(workspace)?;
        cfg.cwd = cwd.clone();
        cfg.workspace_roots = vec![cwd];
        let NewThread {
            thread_id, thread, ..
        } = self
            .tm
            .start_thread(StartThreadOptions::new(cfg))
            .await
            .context("start Codex thread")?;
        let thread_id_string = thread_id.to_string();
        let _ = thread
            .set_app_server_client_info(
                Some("codep".to_string()),
                Some(env!("CARGO_PKG_VERSION").to_string()),
                /*mcp_elicitations_auto_deny*/ true,
            )
            .await;
        self.tasks.lock().await.insert(
            task.to_string(),
            TaskLive {
                thread: Arc::clone(&thread),
                thread_id,
            },
        );
        self.thread_task
            .lock()
            .await
            .insert(thread_id, task.to_string());
        {
            let mut s = self.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
            s.update(task, |t| {
                t["nativeSessionId"] = json!(thread_id_string);
            })?;
        }
        tokio::spawn(event_loop(
            Arc::clone(self),
            task.to_string(),
            Arc::clone(&thread),
            thread_id,
            false,
        ));

        match thread.start_turn_if_idle(user_input_request(text, key)).await {
            Ok(StartIfIdleSubmission::Started { .. }) => {
                let mut s = self.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
                s.update(task, |t| {
                    if matches!(t["state"].as_str(), Some("PREPARING" | "QUEUED" | "STARTING")) {
                        t["state"] = json!("RUNNING");
                    }
                })?;
                Ok(())
            }
            Ok(StartIfIdleSubmission::NotSubmitted { reason }) => {
                bail!("LOCAL_TURN_NOT_SUBMITTED: {reason:?}")
            }
            Err(e) => Err(anyhow::anyhow!("LOCAL_TURN_SUBMIT_FAILED: {e}")),
        }
    }

    /// Follow-up on an idle thread: start a new turn on the same codex thread.
    pub async fn follow_up(&self, task: &str, key: &str, content: &str) -> Result<()> {
        let Some((_, thread)) = self.live_thread(task).await else {
            bail!("NATIVE_WRITER_UNPROVEN")
        };
        match thread
            .start_turn_if_idle(user_input_request(content, key))
            .await
        {
            Ok(StartIfIdleSubmission::Started { turn_id, .. }) => {
                self.after_submit_applied(task, key, &turn_id)
            }
            Ok(StartIfIdleSubmission::NotSubmitted { reason }) => {
                self.submit_not_applied(task, key, &format!("{reason:?}"))
            }
            Err(e) => self.submit_unknown(task, key, &format!("{e}")),
        }
    }

    /// Steer the running turn.
    pub async fn steer(&self, task: &str, key: &str, content: &str, expected_turn: &str) -> Result<()> {
        let Some((_, thread)) = self.live_thread(task).await else {
            bail!("NATIVE_WRITER_UNPROVEN")
        };
        match thread
            .steer_turn(user_input_request(content, key), expected_turn.to_string())
            .await
        {
            Ok(SteerSubmission::Steered { turn_id }) => {
                self.after_submit_applied(task, key, &turn_id)
            }
            Ok(SteerSubmission::NotSubmitted { reason }) => {
                self.submit_not_applied(task, key, &format!("{reason:?}"))
            }
            Err(e) => self.submit_unknown(task, key, &format!("{e}")),
        }
    }

    /// STOP: interrupt the active turn. The TurnAborted event (or the poll
    /// fallback in the host-side stop task) settles the task CANCELLED.
    pub async fn interrupt(&self, task: &str) -> Result<()> {
        if let Some((_, thread)) = self.live_thread(task).await {
            thread
                .submit(Op::Interrupt)
                .await
                .context("submit Op::Interrupt")?;
        }
        Ok(())
    }

    fn after_submit_applied(&self, task: &str, key: &str, turn_id: &str) -> Result<()> {
        let mut s = self.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
        s.update(task, |t| {
            if matches!(t["state"].as_str(), Some("PREPARING" | "QUEUED" | "STARTING" | "SUCCEEDED")) {
                t["state"] = json!("RUNNING");
                t["quiescent"] = json!(false);
            }
        })?;
        let thread = s
            .task(task)?
            .pointer("/nativeSessionId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let _ = s.mark_user_consumed(task, key, &thread, turn_id);
        s.receipt_auto(
            task,
            key,
            "APPLIED",
            &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","detail":"Native turn accepted the input"}),
        )?;
        Ok(())
    }

    fn submit_not_applied(&self, task: &str, key: &str, reason: &str) -> Result<()> {
        let mut s = self.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
        // A follow-up that could not start leaves the task where it was.
        s.update(task, |t| {
            if t["currentCommandId"].as_str() == Some(key)
                && !matches!(t["state"].as_str(), Some("CANCELLED" | "FAILED" | "UNKNOWN"))
            {
                // Revert the engaged mark: the turn never started.
                if t["nativeTurnId"].is_null() {
                    t["state"] = json!("PREPARING");
                } else {
                    t["state"] = json!("SUCCEEDED");
                    t["quiescent"] = json!(true);
                }
            }
        })?;
        s.receipt_auto(
            task,
            key,
            "NOT_APPLIED",
            &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"NOT_APPLIED","detail":format!("native submission declined: {reason}")}),
        )?;
        Ok(())
    }

    fn submit_unknown(&self, task: &str, key: &str, detail: &str) -> Result<()> {
        let mut s = self.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
        s.receipt_auto(
            task,
            key,
            "UNKNOWN",
            &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"UNKNOWN","detail":format!("native submission error: {detail}")}),
        )?;
        Ok(())
    }

    /// Drop the live handle for a terminal task, shutting the thread down.
    pub async fn retire(&self, task: &str) {
        let live = self.tasks.lock().await.remove(task);
        if let Some(live) = live {
            self.thread_task.lock().await.remove(&live.thread_id);
            let _ = live.thread.shutdown_and_wait().await;
        }
    }
}

/// Per-thread event consumer. For root threads (`child == false`) it projects
/// public messages, turn lifecycle and usage into the store. For child
/// threads it only keeps the approval safety net (plus an audit line on the
/// owning task when known).
async fn event_loop(
    d: Arc<Driver>,
    task: String,
    thread: Arc<CodexThread>,
    thread_id: ThreadId,
    child: bool,
) {
    let thread_id_string = thread_id.to_string();
    loop {
        let event = match thread.next_event().await {
            Ok(event) => event,
            Err(e) => {
                if !child {
                    warn!(task, "thread event stream ended: {e}");
                    settle_dead_writer(&d, &task, "NATIVE_EVENT_STREAM_CLOSED");
                }
                return;
            }
        };
        let msg = event.msg;
        if child {
            auto_answer(&d, &task, &thread, &msg, true).await;
            continue;
        }
        if let Err(e) = project(&d, &task, &thread, &thread_id_string, msg).await {
            warn!(task, "event projection failed: {e}");
        }
    }
}

/// The event stream is gone: if the task is still live its writer is
/// unproven — settle UNKNOWN honestly rather than pretending it finished.
fn settle_dead_writer(d: &Driver, task: &str, code: &str) {
    if task.is_empty() {
        return;
    }
    if let Ok(mut s) = d.store.lock() {
        if let Ok(t) = s.task(task) {
            let live = t["quiescent"] != true
                && matches!(
                    t["state"].as_str(),
                    Some("PREPARING" | "QUEUED" | "RUNNING" | "STARTING" | "STOP_REQUESTED")
                );
            if live {
                let cmd = t["currentCommandId"].as_str().unwrap_or_default().to_owned();
                let _ = s.update(task, |t| {
                    t["state"] = json!("UNKNOWN");
                    t["quiescent"] = json!(true);
                    t["reasonCode"] = json!(code);
                });
                let _ = s.receipt_auto(
                    task,
                    &cmd,
                    "UNKNOWN",
                    &json!({"kind":"WORKER_LOCAL","requestKey":cmd,"state":"UNKNOWN","detail":code}),
                );
                let _ = s.event(task, "task.unproven", json!({"reasonCode":code}));
            }
        }
    }
}

fn agent_message_text(item: &codex_protocol::items::AgentMessageItem) -> String {
    item.content
        .iter()
        .map(|c| match c {
            AgentMessageContent::Text { text } => text.clone(),
        })
        .collect::<Vec<_>>()
        .join("")
}

fn turn_item_tool_kind(item: &TurnItem) -> Option<&'static str> {
    Some(match item {
        TurnItem::CommandExecution(_) => "command_execution",
        TurnItem::McpToolCall(_) => "mcp_tool_call",
        TurnItem::FileChange(_) => "file_change",
        TurnItem::DynamicToolCall(_) => "dynamic_tool_call",
        TurnItem::WebSearch(_) => "web_search",
        TurnItem::ImageGeneration(_) => "image_generation",
        TurnItem::CollabAgentToolCall(_) => "collab_agent_tool_call",
        TurnItem::SubAgentActivity(_) => "sub_agent_activity",
        _ => return None,
    })
}

async fn project(
    d: &Arc<Driver>,
    task: &str,
    thread: &Arc<CodexThread>,
    thread_id_string: &str,
    msg: EventMsg,
) -> Result<()> {
    match msg {
        EventMsg::TurnStarted(e) => {
            let mut s = d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
            s.update(task, |t| {
                t["nativeTurnId"] = json!(e.turn_id);
                if t["nativeSessionId"].is_null() {
                    t["nativeSessionId"] = json!(thread_id_string);
                }
                if matches!(
                    t["state"].as_str(),
                    Some("PREPARING" | "QUEUED" | "STARTING" | "SUCCEEDED")
                ) {
                    t["state"] = json!("RUNNING");
                    t["quiescent"] = json!(false);
                }
            })?;
            s.event(task, "turn/started", json!({"nativeTurnId":e.turn_id}))?;
        }
        EventMsg::TurnComplete(e) => {
            let failed = e.error.is_some();
            {
                let mut s = d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
                if let Some(text) = e.last_agent_message.as_deref().filter(|t| !t.is_empty()) {
                    if s.assistant_count_for_turn(task, thread_id_string, &e.turn_id)? == 0 {
                        let _ = s.record_agent_message(
                            task,
                            thread_id_string,
                            &e.turn_id,
                            &format!("last-{turn_id}", turn_id = e.turn_id),
                            text,
                        );
                    }
                }
                let t = s.update(task, |t| {
                    t["state"] = json!(if failed { "FAILED" } else { "SUCCEEDED" });
                    t["quiescent"] = json!(true);
                    if failed {
                        t["reasonCode"] = json!("NATIVE_TURN_FAILED");
                    } else {
                        t.as_object_mut().map(|o| o.remove("reasonCode"));
                    }
                })?;
                s.event(
                    task,
                    "turn/completed",
                    json!({"nativeTurnId":e.turn_id,"state":if failed {"FAILED"} else {"SUCCEEDED"}}),
                )?;
                let cmd = t["currentCommandId"].as_str().unwrap_or_default().to_owned();
                if !cmd.is_empty() {
                    s.receipt_auto(
                        task,
                        &cmd,
                        if failed { "FAILED" } else { "APPLIED" },
                        &json!({"kind":"WORKER_LOCAL","requestKey":cmd,"state":if failed {"FAILED"} else {"APPLIED"},"detail":"turn completed"}),
                    )?;
                }
            }
            // Completed tasks stay live for follow-up; failed threads retire.
            if failed {
                d.retire(task).await;
            }
        }
        EventMsg::TurnAborted(e) => {
            {
                let mut s = d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
                let stop_key = s.task(task)?["pendingStopKey"].as_str().map(str::to_owned);
                let t = s.update(task, |t| {
                    t["state"] = json!("CANCELLED");
                    t["quiescent"] = json!(true);
                    t["reasonCode"] = json!("TASK_STOPPED");
                    t.as_object_mut().map(|o| o.remove("pendingStopKey"));
                })?;
                s.event(
                    task,
                    "turn/aborted",
                    json!({"nativeTurnId":e.turn_id,"state":"CANCELLED"}),
                )?;
                let cmd = t["currentCommandId"].as_str().map(str::to_owned);
                if let Some(key) = stop_key.clone() {
                    s.receipt_auto(
                        task,
                        &key,
                        "APPLIED",
                        &json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","detail":"turn interrupted; task cancelled"}),
                    )?;
                }
                if let Some(cmd) = cmd.filter(|c| Some(c) != stop_key.as_ref()) {
                    s.receipt_auto(
                        task,
                        &cmd,
                        "CANCELLED",
                        &json!({"kind":"WORKER_LOCAL","requestKey":cmd,"state":"CANCELLED","detail":"turn interrupted"}),
                    )?;
                }
            }
            d.retire(task).await;
        }
        EventMsg::ItemStarted(e) => {
            if let Some(kind) = turn_item_tool_kind(&e.item) {
                let s = d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
                s.event(
                    task,
                    "TOOL_START",
                    json!({"nativeTurnId":e.turn_id,"toolKind":kind}),
                )?;
            }
            if let TurnItem::UserMessage(item) = &e.item {
                if let Some(key) = item.client_id.as_deref() {
                    let mut s = d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
                    let _ = s.record_user_consumption(
                        task,
                        key,
                        thread_id_string,
                        &e.turn_id,
                        &item.id,
                    );
                }
            }
        }
        EventMsg::ItemCompleted(e) => {
            match &e.item {
                TurnItem::UserMessage(item) => {
                    if let Some(key) = item.client_id.as_deref() {
                        let mut s =
                            d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
                        let _ = s.record_user_consumption(
                            task,
                            key,
                            thread_id_string,
                            &e.turn_id,
                            &item.id,
                        );
                    }
                }
                TurnItem::AgentMessage(item) => {
                    let text = agent_message_text(item);
                    if !text.is_empty() {
                        let mut s =
                            d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
                        let _ = s.record_agent_message(
                            task,
                            thread_id_string,
                            &e.turn_id,
                            &item.id,
                            &text,
                        );
                    }
                }
                _ => {}
            }
            if let Some(kind) = turn_item_tool_kind(&e.item) {
                let s = d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
                s.event(
                    task,
                    "TOOL_END",
                    json!({"nativeTurnId":e.turn_id,"toolKind":kind}),
                )?;
            }
        }
        EventMsg::TokenCount(e) => {
            if let Some(info) = &e.info {
                let u = &info.total_token_usage;
                let mut s = d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
                s.update(task, |t| {
                    t["usage"] = json!({
                        "inputTokens": u.input_tokens.to_string(),
                        "cachedInputTokens": u.cached_input_tokens.to_string(),
                        "outputTokens": u.output_tokens.to_string(),
                        "reasoningOutputTokens": u.reasoning_output_tokens.to_string(),
                        "totalTokens": u.total_tokens.to_string(),
                    });
                })?;
            }
        }
        EventMsg::Error(e) => {
            let s = d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
            s.event(
                task,
                "task.error",
                json!({"message":e.message}),
            )?;
        }
        EventMsg::StreamError(e) => {
            let s = d.store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
            s.event(
                task,
                "stream.error",
                json!({"message":e.message}),
            )?;
        }
        EventMsg::ShutdownComplete => {}
        EventMsg::AgentMessageContentDelta(e) => {
            output_delta(&d.store, task, &e);
        }
        _ => {
            auto_answer(d, task, thread, &msg, false).await;
        }
    }
    Ok(())
}

/// Typewriter spine projection: `AgentMessageContentDelta` carries the public
/// assistant text increments core already streams through the event loop; pin
/// each to the durable events table as `OUTPUT_DELTA` so the events read
/// surface can replay or tail them (worker-events/1). Reasoning, plan and
/// exec-output deltas stay private and keep falling to the catch-all.
/// A persist failure must never kill the turn — warn and continue, same as
/// `record_agent_message`'s non-fatal handling above.
pub fn output_delta(store: &SharedStore, task: &str, e: &AgentMessageContentDeltaEvent) {
    let result = store
        .lock()
        .map_err(|_| anyhow::anyhow!("STORE_POISONED"))
        .and_then(|s| {
            s.event(
                task,
                "OUTPUT_DELTA",
                json!({
                    "nativeTurnId": e.turn_id,
                    "nativeItemId": e.item_id,
                    "delta": e.delta,
                }),
            )
        });
    if let Err(err) = result {
        warn!(task, "OUTPUT_DELTA event persist failed: {err}");
    }
}

/// Safety net for approval-shaped events that should never appear under
/// Never+DangerFullAccess: answer them anyway so no turn can wedge, and write
/// a durable audit line for every automatic answer.
async fn auto_answer(d: &Arc<Driver>, task: &str, thread: &Arc<CodexThread>, msg: &EventMsg, child: bool) {
    let audit = |kind: &str, payload: Value| {
        if task.is_empty() {
            return;
        }
        if let Ok(s) = d.store.lock() {
            let _ = s.event(task, "approval.auto_answered", {
                let mut p = payload;
                p["autoAnswer"] = json!(kind);
                p["childThread"] = json!(child);
                p
            });
        }
    };
    match msg {
        EventMsg::ExecApprovalRequest(e) => {
            audit(
                "exec_approval.approved",
                json!({"callId":e.call_id,"turnId":e.turn_id}),
            );
            let _ = thread
                .submit(Op::ExecApproval {
                    id: e.effective_approval_id(),
                    turn_id: Some(e.turn_id.clone()),
                    decision: ReviewDecision::Approved,
                })
                .await;
        }
        EventMsg::ApplyPatchApprovalRequest(e) => {
            audit(
                "patch_approval.approved",
                json!({"callId":e.call_id,"turnId":e.turn_id}),
            );
            let _ = thread
                .submit(Op::PatchApproval {
                    id: e.call_id.clone(),
                    decision: ReviewDecision::Approved,
                })
                .await;
        }
        EventMsg::ElicitationRequest(e) => {
            audit(
                "elicitation.declined",
                json!({"serverName":e.server_name,"requestId":serde_json::to_value(&e.id).unwrap_or(Value::Null)}),
            );
            let _ = thread
                .submit(Op::ResolveElicitation {
                    server_name: e.server_name.clone(),
                    request_id: e.id.clone(),
                    decision: ElicitationAction::Decline,
                    content: None,
                    meta: None,
                })
                .await;
        }
        EventMsg::RequestPermissions(e) => {
            audit(
                "permissions.granted",
                json!({"callId":e.call_id,"turnId":e.turn_id}),
            );
            let _ = thread
                .submit(Op::RequestPermissionsResponse {
                    id: e.call_id.clone(),
                    response: RequestPermissionsResponse {
                        permissions: e.permissions.clone(),
                        scope: PermissionGrantScope::Session,
                        strict_auto_review: false,
                    },
                })
                .await;
        }
        EventMsg::RequestUserInput(e) => {
            audit(
                "user_input.empty_answers",
                json!({"callId":e.call_id,"turnId":e.turn_id}),
            );
            let answers = e
                .questions
                .iter()
                .map(|q| {
                    (
                        q.id.clone(),
                        RequestUserInputAnswer {
                            answers: Vec::new(),
                        },
                    )
                })
                .collect();
            let _ = thread
                .submit(Op::UserInputAnswer {
                    id: e.call_id.clone(),
                    response: RequestUserInputResponse { answers },
                })
                .await;
        }
        EventMsg::DynamicToolCallRequest(e) => {
            audit(
                "dynamic_tool_call.failed",
                json!({"callId":e.call_id,"tool":e.tool}),
            );
            let _ = thread
                .submit(Op::DynamicToolResponse {
                    id: e.call_id.clone(),
                    response: DynamicToolResponse {
                        content_items: vec![DynamicToolCallOutputContentItem::InputText {
                            text: "dynamic tool calls are not supported by this host".to_string(),
                        }],
                        success: false,
                    },
                })
                .await;
        }
        _ => {}
    }
}
