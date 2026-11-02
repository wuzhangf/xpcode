# Embedding codex-core in-process (codep recipe)

Template: `codex-rs/thread-manager-sample/src/main.rs` — the ONLY sample that holds
`codex_core::ThreadManager` directly. Do NOT use InProcessAppServerClient (tui/exec route).

All types re-exported through `codex-core-api` (`core-api/src/lib.rs:5-150`) plus
`codex-core` (`ConfigBuilder`,`ConfigOverrides`,`LoaderOverrides`) and `codex-protocol`
(`SandboxMode`,`ReviewDecision`,`ElicitationAction`,`Event`,`TurnItem`,...).

## Boot sequence (copy thread-manager-sample/src/main.rs)

```
arg0_dispatch_or_else(run_main)                      // arg0/src/lib.rs:192 — REQUIRED
  set_default_originator("codep")
  codex_home = <codep state dir>/codex-home (must exist if CODEX_HOME set)
  load_config_toml_with_layer_stack(codex_home, cwd, cli_overrides, ConfigLoadOptions)
  bootstrap_auth_config / resolve_bootstrap_respect_system_proxy
  config = ConfigBuilder::default()
      .codex_home(..).harness_overrides(ConfigOverrides{
          approval_policy: Some(AskForApproval::Never),
          sandbox_mode:    Some(SandboxMode::DangerFullAccess),
          model_provider:  Some("<provider id>"),       // String!
          codex_self_exe / codex_linux_sandbox_exe: from Arg0DispatchPaths,
          ephemeral: true,
          .. })
      .loader_overrides(LoaderOverrides{ ignore_login_requirements: true, .. })
      .build().await
  state_db = init_state_db(&config).await
  auth = AuthManager::shared_from_config(&config, false)
  runtime = ExecServerRuntimeOptions::from_optional_paths(config.codex_self_exe, config.codex_linux_sandbox_exe)?
  thread_store = thread_store_from_config(&config, state_db)
  env_mgr = EnvironmentManager::from_codex_home(config.codex_home.clone(), Some(runtime.local_runtime_paths), config.http_client_factory())
  installation_id = resolve_installation_id(&config.codex_home)
  uip = CodexHomeUserInstructionsProvider::new(config.codex_home.clone())
  extensions = ExtensionRegistryBuilder::new() (+install_image_generation_extension) .build()
  tm = ThreadManager::new(&config, auth, build_models_manager(&config,auth), CodexAppsToolsCache::default(),
        SessionSource::Custom("codep"), env_mgr, extensions, uip, None, passthrough_image_store(),
        thread_store, local_agent_graph_store_from_state_db(state_db.as_ref()), installation_id, None, None)
  nt = tm.start_thread(StartThreadOptions::new(config))   // -> NewThread{thread_id, thread, session_configured}
```

Pitfalls (verified):
- `arg0_dispatch_or_else` MUST wrap main (thread stacks, tokio runtime, .env load, apply_patch/Windows sandbox re-exec PATH). Windows generates `apply_patch.bat` — do not skip PATH injection.
- `CODEX_HOME` if set must already exist.
- `workspace-write` silently degrades to read-only on Windows without sandbox backend — hence DangerFullAccess.
- `approval_policy=never` is rejected if managed requirements ban danger-full-access — fine for us.
- Config literal (400+ fields) is fragile — use ConfigBuilder+ConfigOverrides.

## Driving turns

- `thread.start_turn_if_idle(TurnInputRequest::user_input(vec![UserInput::Text{text, text_elements:vec![]}]))` — main submit path (codex_thread.rs:429-451).
- `thread.steer_turn(request, expected_turn_id)` — steer while running.
- `thread.submit(Op::Interrupt)` — stop current turn (TurnAborted event).
- `thread.next_event()` — single-consumer event stream `Event{id, msg: EventMsg}` per thread.
- `tm.subscribe_thread_created()` — broadcast of child/subagent ThreadIds; attach a `get_thread(id)` + event loop each (else their approvals hang).
- `tm.get_thread(id)`, `list_thread_ids()`, `thread.shutdown_and_wait()`, `tm.shutdown_all_threads_bounded(t)`.

## Events → projection (EventMsg variants, protocol/src/protocol.rs:1467-1687)

| codep use | variant | payload |
|---|---|---|
| assistant delta (PUBLIC — stream later) | `AgentMessageContentDelta` | `{thread_id,turn_id,item_id,delta}` |
| assistant final | `AgentMessage` | `{message}` |
| item lifecycle | `ItemStarted`/`ItemCompleted` | `TurnItem` enum (UserMessage/AgentMessage/Plan/Reasoning/CommandExecution/DynamicToolCall/FileChange/McpToolCall/...) |
| turn lifecycle | `TurnStarted`/`TurnComplete`/`TurnAborted` | TurnComplete has `last_agent_message: Option<String>` |
| exec approval | `ExecApprovalRequest` | answer `Op::ExecApproval{id: event.effective_approval_id(), turn_id:Some(t), decision: ReviewDecision}` |
| patch approval | `ApplyPatchApprovalRequest` | `Op::PatchApproval{id: call_id, decision}` |
| elicitation | `ElicitationRequest` | `Op::ResolveElicitation{server_name, request_id: event.id, decision: ElicitationAction::Decline, ..}` |
| permissions | `RequestPermissions` | `Op::RequestPermissionsResponse{id:call_id, response:{permissions, scope: PermissionGrantScope::Session,..}}` |
| user input | `RequestUserInput` | `Op::UserInputAnswer{id:call_id, response:{answers}}` |
| reasoning/tool deltas (PRIVATE — never expose) | `ReasoningContentDelta`,`ReasoningRawContentDelta`,`PlanDelta`,`ExecCommandOutputDelta` | filter out |

Also useful: `item_event_to_server_notification(msg, thread_id, turn_id)` (core-api:7) maps events to app-server v2 notification shapes if wanted.

## codep approval posture (v1)

Never+DangerFullAccess ⇒ core should not emit ExecApprovalRequest. Safety net anyway:
in the per-thread event loop, auto-answer any surviving approval/elicitation:
`ReviewDecision::Denied` → NO — codep is unattended: `Approved` for exec/patch within workspace scope? For v1: answer `ReviewDecision::Approved` (we run DangerFullAccess anyway) and `ElicitationAction::Decline` for elicitations; log each auto-answer into the task ledger/audit.
Also call `thread.set_app_server_client_info(None,None, mcp_elicitations_auto_deny:true)` so MCP elicitations never surface.

## Model provider wiring (env-driven)

Codex-core only speaks `Responses` wire API. Write `<codex_home>/config.toml` (or harness overrides) with a custom provider:
```toml
model = "${MODEL_NAME}"
model_provider = "codep"
[model_providers.codep]
name = "codep"
base_url = "${MODEL_BASE_URL}"
env_key = "MODEL_API_KEY"              # provider reads key from env var NAME
wire_api = "responses"                 # only valid value
env_http_headers = { "<header-name>" = "ENV_VAR_NAME" }   # e.g. x-opencode-session
requires_openai_auth = false
```
Values come from process env (`D:\project\env` loads MODEL_API_KEY/MODEL_BASE_URL/MODEL_NAME/LLM_HEADER_x_opencode_session). Never write the key into config.toml — env_key is a variable NAME, resolved at call time.

## Build environment (this machine)

```bash
PATH="$HOME/.cargo/bin:/d/project/tools/nwf-build/llvm-mingw-20260908-ucrt-x86_64/bin:$PATH" \
RUSTUP_TOOLCHAIN=1.99.1-x86_64-pc-windows-gnu \
cargo check -p codep
```
rust-toolchain.toml pins channel 1.99.1 → resolves to the installed gnu toolchain; llvm-mingw supplies cc for C deps (aws-lc-sys, ring, sqlite). If a C dependency cannot build under gnu, STOP and report — do not switch toolchains silently.

## Crate deps (minimal)

`codex-core-api`, `codex-core`, `codex-protocol`, `codex-config` (LoaderOverrides/ConfigToml),
`axum`, `tokio`(full), `serde`,`serde_json`, `anyhow`, `tracing`,`tracing-subscriber`,
`rusqlite`(bundled), `sha2`, `uuid`, `clap`(derive), `toml`/`toml_edit`, `http-body-util`, `chrono`.
Check workspace `[workspace.dependencies]` names in codex-rs/Cargo.toml first — prefer `x = { workspace = true }`.
