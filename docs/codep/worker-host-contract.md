# Minimal worker-host contract for codep (v1)

Spec extracted from `EnterpriseDockerCode/workers/codex/src/host.rs` (HTTP surface),
`store.rs` (ledger), `workers/pi` (TS equivalent), and the gateway consumer
`BusinessAgentCode/apps/control-plane-web/server/management/native-tasks.ts` +
`packages/personal-gateway-core/src/upstream.ts` + Electron renderer contract
`apps/codeagent-desktop/packages/protocol/src/native-task.ts`.

Reference impl line numbers: `<upstream-workspace>/workers/codex/src/host.rs`.

## 0. Identity

- engineId: **`codep`** (new engine; gateway whitelist is patched separately on the client side).
- engineVersion: a fixed string, e.g. `codep 0.1.0 codex-core-0.163.0a11.2` — must be IDENTICAL in manifest, task-preflights response, task record/detail.
- Persisted store identity: `storeId`, `authorityId`, `workerId` stable across restarts (created at init); `bootId` new each boot.

## 1. Transport/auth (checked before any route)

Every request (gateway `ConnectionHub.rawLocal`):
- `Authorization: Bearer <token>` — constant-time compare, mismatch → 401 `{"code":"LOCAL_AUTH_REQUIRED"}`.
- `Host` must start `127.0.0.1:` or `localhost:` → else `LOOPBACK_REQUIRED`.
- `X-Worker-Local-Store: <storeId>` — if present must equal ours → `STORE_IDENTITY_CHANGED`.
- `Idempotency-Key` header: if body has `requestKey` it MUST equal header (except `/operations/lookup`) → `KEY_MISMATCH`.
- Bodies: JSON object only; `store::canonical` check = **no non-integer JSON numbers** (any float anywhere → `INTEGER_REQUIRED`). Unknown top-level fields on whitelisted routes → `UNEXPECTED_FIELD`.
- Query: duplicate keys → `DUPLICATE_QUERY`.
- Error body shape: `{"code":"<UPPER_SNAKE>","message":"<same>","sourceCode":null}`. Status map: `LOCAL_AUTH_REQUIRED`→401; `*_NOT_FOUND`/`FIXED_LOCAL_ROUTE_REQUIRED`→404; everything else→409. Codes must match `^[A-Z][A-Z0-9_]+$`.

## 2. Discovery routes

### GET /worker-host/v1/manifest → 200
```json
{
  "authorityId":"..","storeId":"..","workerId":"..","bootId":"..",
  "schemaVersion":"worker-host/1",
  "storeGuard":"HEADER_V1",
  "engine":{"id":"codep","version":"<engineVersion>"},
  "contracts":["worker-host/1","worker-local/1"],
  "capabilities":[ ...same items as /capabilities... ],
  "supportedPaths":["GET /manifest","GET /capabilities","POST /operations/lookup"]
}
```
Gateway checks: schemaVersion, non-empty ids ≤1024, engine.id in whitelist (patched to include `codep`), contracts contains `worker-local/1`, supportedPaths string[]. **If you implement manifest you MUST also implement /capabilities** — a non-2xx capabilities kills the whole binding.

### GET /worker-host/v1/capabilities → 200
`{"schemaVersion":"worker-capabilities/1","items":[{"id":"execution.messages","implementation":"IMPLEMENTED","qualification":"PARTIAL","availability":"AVAILABLE","semantics":"PUBLIC_FINAL_MESSAGES","evidenceRef":null,"reasonCode":null,"limits":{}} , ...]}`.
Rules: items ≤256; `qualification:'VERIFIED'` requires non-empty `evidenceRef` else 502. `items:[]` is legal. Declare honest GAP/UNAVAILABLE for pause/resume/materials/handoff/terminal/collab/etc.

### GET /local/v1/identity → `{schemaVersion:"worker-local/1", storeId, bootId, authorityId, workerId}` (storeId must equal manifest.storeId)
### GET /local/v1/engines → `{items:[{engineId:"codep", engineVersion, version:<same>, profileId:"MANAGED", pause:false, resume:false, stop:true, steer:true, followUp:true, capabilities:{}, executionPolicy:{}}]}` (gateway reads items[].engineId only)
### GET /local/v1/projects → `{items:[{projectId, displayName, path, headRevision, hasUncommittedChanges}]}` — projectId stable (gateway digests it into `local-<32hex>` external id; subsequent calls send back our raw projectId).

### POST /local/v1/task-preflights
Body: `{projectId, engineId, policyPresetId?}` (exact whitelist). Unknown project → `PROJECT_NOT_REGISTERED`; engine → `ENGINE_NOT_REGISTERED`.
Response (NativePreflight):
```json
{"projectId":"..","engineId":"codep","engineVersion":"..","optionsRevision":"<64-hex>",
 "profileId":"MANAGED","profileHome":null,"loadsUserExtensions":false,
 "headRevision":"<non-empty>","hasUncommittedChanges":false,
 "workingCopyPolicy":"COMMITTED_BASE_ONLY",
 "configuredModel":"<model>"}
```
`optionsRevision` = sha256hex of canonical JSON of the response minus optionsRevision — MUST be deterministic; worker recomputes at create time and compares (`PREFLIGHT_CHANGED`).

## 3. Task lifecycle

### POST /local/v1/tasks  (create)
Body whitelist: `requestKey,projectId,engineId,optionsRevision,instruction` (+ optionally accept `budget,writeScope,environment,mcpServers,skills,hooks,policyPresetId,materials` or reject with UNEXPECTED_FIELD — accepting-and-ignoring is fine for v1).
- `instruction` non-empty ≤16384.
- Recompute preflight → optionsRevision mismatch → `PREFLIGHT_CHANGED`.
- Idempotent: same requestKey + canonically-equal body → 202 same receipt/task; same key different body → `IDEMPOTENCY_CONFLICT`.
- Response **202** = full task record:
```json
{"taskId":"native-<id (letters/digits/hyphen only, native- prefix required)>",
 "engineId":"codep","engineVersion":"..","projectId":"..","state":"PREPARING",
 "generation":"..","nativeSessionId":"<thread id — TRUTHY or client can't send>",
 "nativeTurnId":null,"currentCommandId":"<requestKey>","quiescent":false,
 "capabilities":{"stop":true,"steer":true,"followUp":true,"pause":false,"resume":false},
 "configuredModel":"..","workspace":"<abs path>","segmentId":"..","ownerEpoch":".."}
```

### GET /local/v1/task-requests/{requestKey} → create receipt (same task record); 404 `REQUEST_NOT_FOUND`.
### GET /local/v1/tasks?limit≤50 → `{items:[{taskId,projectId,engineId,engineVersion,state}]}`.
### GET /local/v1/tasks/{id} → task detail (all fields above; keep `capabilities` truthful: steer only when RUNNING, followUp only when SUCCEEDED/idle, stop when live).
- state values Electron maps: `PREPARING|QUEUED|RUNNING|SUCCEEDED|FAILED|CANCELLED|UNKNOWN|TIMED_OUT|INTERRUPTED|STOPPED`. Minimum: PREPARING→RUNNING→SUCCEEDED|FAILED, CANCELLED on stop.

### POST /local/v1/tasks/{id}/commands
Body `{requestKey,intent:'STOP'|'PAUSE'|'RESUME',generation,nativeSessionId,nativeTurnId,targetCommandId}`.
Guard: every guard field must equal current task values (`targetCommandId===currentCommandId`) else `NATIVE_TARGET_CHANGED`. PAUSE/RESUME → 409 `NATIVE_ACTION_UNSUPPORTED`. STOP → interrupt thread (`Op::Interrupt`), task→CANCELLED, quiescent→true; receipt `{kind:'WORKER_LOCAL',requestKey,state:'ACCEPTED'}` → lookup should progress to `APPLIED`.

### POST /local/v1/tasks/{id}/messages
Body `{requestKey,kind:'NATIVE_STEER'|'NATIVE_FOLLOW_UP',content,generation,nativeSessionId,nativeTurnId,targetCommandId}`.
- STEER requires state==RUNNING else `NATIVE_NOT_RUNNING` → `thread.steer_turn`.
- FOLLOW_UP requires SUCCEEDED/quiescent else `NATIVE_NOT_IDLE` → `start_turn_if_idle`.
- content non-empty ≤16384. On accept: append user message row (sourceRequestKey=requestKey), set `currentCommandId=requestKey`, response **202** `{kind:'WORKER_LOCAL',requestKey,state:'ACCEPTED',detail:{...new currentCommandId...}}`.

### POST /local/v1/tasks/{id}/input-requests/{inputRequestId}/responses
Body `{requestKey,generation,nativeSessionId,nativeTurnId,targetCommandId,response}` — response is exactly one of `{confirmed:bool}|{value:string}|{decision:string}`.
v1 posture: we emit no PENDING input-requests → may just 409 `NATIVE_INPUT_EXPIRED`. (Later: map core ExecApprovalRequest events here if we re-enable approvals.)

### GET /local/v1/tasks/{id}/interaction-requests/{requestKey} AND /commands/{requestKey}
Original-key receipt lookup; 404 `REQUEST_NOT_FOUND`. Receipt `{kind:'WORKER_LOCAL',requestKey,state,detail}`; terminal states Electron waits for: `APPLIED|CONSUMED|REJECTED|FAILED|CANCELLED|NOT_APPLIED` — drive receipts to terminal fast (client unlocks its send box on settled).

## 4. Read surfaces (Electron polls every ~2.5s; missing routes show errors)

- `GET /local/v1/tasks/{id}/messages?cursor|before&limit(1-100)` — **use `native-messages/1` shape**:
```json
{"messageSchemaVersion":"native-messages/1","taskId":"..","storeId":"..",
 "transcriptRevision":"<decimal str>","items":[..],"firstSequence":"..","cursor":"..",
 "latestSequence":"..","hasMore":false,"cursorExpired":false,
 "historyMode":"STRUCTURED_FROM_START","legacy":null,
 "text":"<concatenated assistant text>","turns":{},"coverage":"BOUNDED_PUBLIC_MESSAGES"}
```
  items[] rows: `{messageId,sequence,revision,role:'user'|'assistant',text,textTruncated:bool,status:'ACCEPTED'|'CONSUMED'|'COMPLETED',occurredAt,updatedAt,sourceRequestKey?(user rows),nativeThreadId?,nativeTurnId?,commandId?}` — sequence/revision/transcriptRevision are **decimal strings**.
  Pagination: `cursor`→seq>cursor ASC; `before`→seq<before DESC(limit+1) flip; none→latest page. hasMore per direction.
- `GET /local/v1/tasks/{id}/events?cursor&limit` → `{items:[{cursor,seq,kind,occurredAt,payload}],cursor,cursorExpired:false}` — may return empty items (activity bar only reads TOOL_START/TOOL_END kinds; emit those if cheap).
- `GET /local/v1/tasks/{id}/output` → `{text,coverage:'BOUNDED_PUBLIC_MESSAGES'}`.
- `GET /local/v1/tasks/{id}/metrics` → `{cost:null,costCoverage:'UNKNOWN',usageCoverage:'NATIVE_TASK_TOTAL'|'NO_MODEL',configuredModel, "usage.totalTokens":"<digits>"…}`.
- `GET /local/v1/tasks/{id}/input-requests` → `{items:[]}` (v1).
- `GET /local/v1/tasks/{id}/artifacts` → `{items:[]}` or real artifacts `{artifactId,path,sha256}` + `GET .../artifacts/{id}` `{artifactId,path,sha256,encoding:'base64',bytes}`.

Anything else under /local/v1 or /worker-host/v1 that we don't implement → 404 `FIXED_LOCAL_ROUTE_REQUIRED` (never fall through silently).

## 5. v1 scope-down (explicitly out)

materials upload, work-handoffs, terminals, context-packages, auxiliary-sessions, transitions, collaborations, source-seals, audit, diagnostics, operations/lookup — routes simply absent (404). capabilities items declare the few implemented ones; rest GAP or omitted.
