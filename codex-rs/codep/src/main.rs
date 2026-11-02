//! codep — worker-host service wrapping codex-core's ThreadManager in-process.
//!
//!   codep init  --state <dir> [--project <path>]
//!   codep serve --state <dir> --port <p>

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Result;
use anyhow::bail;
use clap::Parser;
use clap::Subcommand;
use codep::canon;
use codep::driver;
use codep::driver::SharedStore;
use codep::host;
use codep::host::Host;
use codep::store::Store;
use codex_core_api::Arg0DispatchPaths;
use codex_core_api::arg0_dispatch_or_else;
use codex_core_api::set_default_originator;
use serde_json::Value;
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "codep", about = "worker-host service backed by in-process codex-core")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Initialize a worker-host state directory.
    Init {
        /// State directory to create/fill.
        #[arg(long, value_name = "DIR")]
        state: PathBuf,
        /// Register an additional project path.
        #[arg(long, value_name = "PATH")]
        project: Option<PathBuf>,
    },
    /// Serve the worker-host HTTP contract on loopback.
    Serve {
        /// State directory created by `codep init`.
        #[arg(long, value_name = "DIR")]
        state: PathBuf,
        /// Loopback port.
        #[arg(long, value_name = "PORT")]
        port: u16,
    },
}

fn main() -> Result<()> {
    arg0_dispatch_or_else(run)
}

async fn run(arg0: Arg0DispatchPaths) -> Result<()> {
    if let Err(e) = set_default_originator("codep".to_string()) {
        tracing::warn!("failed to set originator: {e:?}");
    }
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .try_init()
        .ok();
    match Cli::parse().command {
        Cmd::Init { state, project } => init(&state, project.as_deref()),
        Cmd::Serve { state, port } => serve(&arg0, &state, port).await,
    }
}

// ---------- init ----------

fn read_json_file(path: &Path) -> Result<Option<Value>> {
    match std::fs::read_to_string(path) {
        Ok(s) => Ok(Some(serde_json::from_str(&s)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn write_json_file(path: &Path, v: &Value) -> Result<()> {
    std::fs::write(path, serde_json::to_string_pretty(v)?)?;
    Ok(())
}

fn init(state: &Path, project: Option<&Path>) -> Result<()> {
    std::fs::create_dir_all(state.join("codex-home"))?;
    std::fs::create_dir_all(state.join("workspaces"))?;

    // Stable persisted identity (preserved on re-init).
    let identity_path = state.join("identity.json");
    let identity = match read_json_file(&identity_path)? {
        Some(v) => v,
        None => {
            let v = json!({
                "authorityId": format!("authority-{}", canon::new_id()),
                "storeId": format!("store-{}", canon::new_id()),
                "workerId": format!("worker-{}", canon::new_id()),
            });
            write_json_file(&identity_path, &v)?;
            v
        }
    };

    // Bearer token: env wins on first init; the file wins thereafter so the
    // binding stays stable even when the env is not loaded.
    let token_path = state.join("token");
    if !token_path.is_file() {
        let token = std::env::var("WORKER_LOCAL_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| format!("wlt-{}{}", canon::new_id(), canon::new_id()));
        std::fs::write(&token_path, token)?;
    }

    // SQLite ledger.
    let _ = Store::open(&state.join("store.db"), &identity)?;

    // Project registry.
    let projects_path = state.join("projects.json");
    let mut registry = read_json_file(&projects_path)?.unwrap_or_else(|| json!({"items":[]}));
    let items = registry["items"]
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("projects.json items must be an array"))?;
    let mut register = |id: &str, path: &Path| -> Result<()> {
        let abs = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let abs_str = abs
            .to_string_lossy()
            .trim_start_matches(r"\\?\")
            .to_string();
        if items.iter().any(|p| {
            p["projectId"].as_str() == Some(id) || p["path"].as_str() == Some(abs_str.as_str())
        }) {
            return Ok(());
        }
        let display = abs
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| id.to_string());
        items.push(json!({
            "projectId": id,
            "displayName": display,
            "path": abs_str,
            "headRevision": format!("rev-{}", &canon::sha256_hex(abs_str.as_bytes())[..16]),
            "registeredAt": canon::now(),
        }));
        Ok(())
    };
    let e2e = Path::new(r"D:\project\worker-state\e2e-project");
    if e2e.is_dir() {
        register("e2e-scratch", e2e)?;
    }
    if let Some(p) = project {
        let id = format!("proj-{}", &canon::sha256_hex(p.to_string_lossy().as_bytes())[..16]);
        register(&id, p)?;
    }
    write_json_file(&projects_path, &registry)?;

    // Codex home: model provider "codep" driven by env. No secret values are
    // written — env_key / env_http_headers reference variable names only.
    let base_url = std::env::var("MODEL_BASE_URL").unwrap_or_default();
    let model = std::env::var("MODEL_NAME").unwrap_or_default();
    let mut toml = String::new();
    if !model.is_empty() {
        toml.push_str(&format!("model = \"{model}\"\n"));
    }
    toml.push_str("model_provider = \"codep\"\n\n[model_providers.codep]\nname = \"codep\"\n");
    if !base_url.is_empty() {
        toml.push_str(&format!("base_url = \"{base_url}\"\n"));
    }
    toml.push_str(
        "env_key = \"MODEL_API_KEY\"\nwire_api = \"responses\"\nenv_http_headers = { \"x-opencode-session\" = \"LLM_HEADER_x_opencode_session\" }\n",
    );
    std::fs::write(state.join("codex-home").join("config.toml"), toml)?;

    println!("codep init complete: {}", state.display());
    println!("identity: {identity}");
    println!("projects: {}", registry["items"]);
    Ok(())
}

// ---------- serve ----------

fn token(state: &Path) -> Result<String> {
    if let Ok(t) = std::fs::read_to_string(state.join("token")) {
        let t = t.trim().to_string();
        if !t.is_empty() {
            return Ok(t);
        }
    }
    if let Ok(t) = std::env::var("WORKER_LOCAL_TOKEN") {
        if !t.is_empty() {
            return Ok(t);
        }
    }
    bail!("LOCAL_STATE_NOT_INITIALIZED")
}

fn configured_model(state: &Path) -> String {
    if let Ok(m) = std::env::var("MODEL_NAME") {
        if !m.is_empty() {
            return m;
        }
    }
    if let Ok(toml) = std::fs::read_to_string(state.join("codex-home").join("config.toml")) {
        for line in toml.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("model") {
                if let Some(rest) = rest.trim_start().strip_prefix('=') {
                    return rest.trim().trim_matches('"').to_string();
                }
            }
        }
    }
    "unknown".to_string()
}

async fn serve(arg0: &Arg0DispatchPaths, state: &Path, port: u16) -> Result<()> {
    let identity: Value = read_json_file(&state.join("identity.json"))?
        .ok_or_else(|| anyhow::anyhow!("LOCAL_STATE_NOT_INITIALIZED"))?;
    let token = token(state)?;
    let projects: Vec<Value> = read_json_file(&state.join("projects.json"))?
        .and_then(|v| v["items"].as_array().cloned())
        .unwrap_or_default();
    let store: SharedStore = Arc::new(Mutex::new(Store::open(
        &state.join("store.db"),
        &identity,
    )?));
    {
        let mut s = store.lock().map_err(|_| anyhow::anyhow!("STORE_POISONED"))?;
        s.recover()?;
    }
    let driver = driver::boot(arg0, state, Arc::clone(&store)).await?;
    let host = Arc::new(Host {
        driver: Some(driver),
        store,
        token,
        state_root: state.to_path_buf(),
        boot_id: format!("boot-{}", canon::new_id()),
        model: configured_model(state),
        projects,
    });
    host::run(host, port).await
}
