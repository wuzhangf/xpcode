//! Durable worker-host ledger on SQLite (rusqlite, bundled).
//!
//! Tables:
//! - `source_identity` — one row: {authorityId, storeId, workerId}
//! - `tasks` — taskId -> full task record JSON
//! - `requests` — (scope,key) idempotency ledger: canonical body + phase + result
//! - `events` — per-task ordered event rows
//! - `public_messages` / `public_transcript_meta` — native-messages/1 index

use std::path::Path;

use anyhow::Result;
use anyhow::bail;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::params;
use serde_json::Value;
use serde_json::json;

use crate::canon;

const MESSAGE_TEXT_MAX: usize = 64 * 1024;
const MESSAGE_ESCAPED_MAX: usize = 128 * 1024;
const MESSAGE_PAGE_MAX: usize = 256 * 1024;
const MESSAGE_RESPONSE_MAX: usize = 1024 * 1024;
const HISTORY_STRUCTURED: &str = "STRUCTURED_FROM_START";

pub struct Store {
    pub db: Connection,
    pub identity: Value,
}

#[derive(Default)]
pub struct NewMessage<'a> {
    pub role: &'a str,
    pub text: &'a str,
    pub status: &'a str,
    pub source_request_key: Option<&'a str>,
    pub native_thread_id: Option<&'a str>,
    pub native_turn_id: Option<&'a str>,
    pub native_item_id: Option<&'a str>,
    pub command_id: Option<&'a str>,
    pub generation: Option<&'a str>,
    pub segment_id: Option<&'a str>,
}

pub enum Consumption {
    Bound,
    Unbound(#[allow(dead_code)] &'static str),
}

impl Store {
    pub fn open(path: &Path, seed_identity: &Value) -> Result<Self> {
        let db = Connection::open(path)?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;",
        )?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS source_identity (id INTEGER PRIMARY KEY CHECK(id=1), json TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, json TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS requests (scope TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, phase TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(scope,key));
             CREATE TABLE IF NOT EXISTS events (task TEXT NOT NULL, seq INTEGER NOT NULL, json TEXT NOT NULL, PRIMARY KEY(task,seq));
             CREATE TABLE IF NOT EXISTS public_messages (
               task_id TEXT NOT NULL,
               message_id TEXT NOT NULL,
               sequence INTEGER NOT NULL,
               revision INTEGER NOT NULL,
               role TEXT NOT NULL,
               text TEXT NOT NULL,
               text_truncated INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               source_request_key TEXT,
               native_thread_id TEXT,
               native_turn_id TEXT,
               native_item_id TEXT,
               command_id TEXT,
               generation TEXT,
               segment_id TEXT,
               occurred_at TEXT NOT NULL,
               updated_at TEXT NOT NULL,
               PRIMARY KEY(task_id,message_id),
               UNIQUE(task_id,sequence)
             );
             CREATE TABLE IF NOT EXISTS public_transcript_meta (
               task_id TEXT PRIMARY KEY,
               next_sequence INTEGER NOT NULL,
               transcript_revision INTEGER NOT NULL,
               history_mode TEXT NOT NULL,
               legacy_text TEXT,
               legacy_text_truncated INTEGER NOT NULL DEFAULT 0,
               initialized_at TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS public_messages_user_key
               ON public_messages(task_id,source_request_key)
               WHERE role='user' AND source_request_key IS NOT NULL;
             CREATE UNIQUE INDEX IF NOT EXISTS public_messages_native_item
               ON public_messages(task_id,native_thread_id,native_item_id)
               WHERE native_thread_id IS NOT NULL AND native_item_id IS NOT NULL;",
        )?;
        let prior: Option<String> = db
            .query_row("SELECT json FROM source_identity WHERE id=1", [], |r| {
                r.get(0)
            })
            .optional()?;
        let identity = match prior {
            Some(s) => serde_json::from_str(&s)?,
            None => {
                let v = seed_identity.clone();
                db.execute("INSERT INTO source_identity VALUES(1,?1)", [v.to_string()])?;
                v
            }
        };
        Ok(Self { db, identity })
    }

    /// After a host restart every task that was mid-flight is unproven: the
    /// owning thread died with the process. Mark it UNKNOWN + quiescent and
    /// settle every still-pending request receipt to UNKNOWN.
    pub fn recover(&mut self) -> Result<()> {
        let live = self
            .tasks()?
            .into_iter()
            .filter(|t| {
                t["quiescent"] != true
                    && matches!(
                        t["state"].as_str(),
                        Some("PREPARING" | "QUEUED" | "RUNNING" | "STARTING" | "STOP_REQUESTED")
                    )
            })
            .map(|t| t["taskId"].as_str().unwrap_or_default().to_owned())
            .collect::<Vec<_>>();
        for task in live {
            self.update(&task, |t| {
                t["state"] = json!("UNKNOWN");
                t["quiescent"] = json!(true);
                t["reasonCode"] = json!("NATIVE_WRITER_UNPROVEN");
            })?;
            let _ = self.event(
                &task,
                "task.unproven",
                json!({"reasonCode":"NATIVE_WRITER_UNPROVEN","detail":"host restart while task was live"}),
            );
        }
        self.db.execute(
            "UPDATE requests SET phase='UNKNOWN' WHERE phase IN ('ACCEPTED','DISPATCHING','REQUESTED')",
            [],
        )?;
        Ok(())
    }

    pub fn tasks(&self) -> Result<Vec<Value>> {
        let mut q = self.db.prepare("SELECT json FROM tasks ORDER BY rowid")?;
        let rows = q.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }

    pub fn task(&self, key: &str) -> Result<Value> {
        let s: Option<String> = self
            .db
            .query_row("SELECT json FROM tasks WHERE id=?1", [key], |r| r.get(0))
            .optional()?;
        Ok(serde_json::from_str(
            &s.ok_or_else(|| anyhow::anyhow!("TASK_NOT_FOUND"))?,
        )?)
    }

    pub fn save(&self, v: &Value) -> Result<()> {
        self.db.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![v["taskId"].as_str(), v.to_string()],
        )?;
        Ok(())
    }

    pub fn update(&mut self, task: &str, change: impl FnOnce(&mut Value)) -> Result<Value> {
        let mut v = self.task(task)?;
        change(&mut v);
        let n = v["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        v["stateVersion"] = json!(n.to_string());
        self.save(&v)?;
        Ok(v)
    }

    fn next_event_seq(db: &Connection, task: &str) -> Result<i64> {
        Ok(db.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?)
    }

    pub fn event(&self, task: &str, kind: &str, payload: Value) -> Result<Value> {
        let seq = Self::next_event_seq(&self.db, task)?;
        let at = canon::now();
        let ev = json!({
            "eventId": format!("event-{}", canon::new_id()),
            "cursor": seq.to_string(),
            "seq": seq.to_string(),
            "sequence": seq.to_string(),
            "kind": kind,
            "type": kind,
            "occurredAt": at,
            "taskId": task,
            "payload": payload,
        });
        self.db.execute(
            "INSERT INTO events VALUES(?1,?2,?3)",
            params![task, seq, ev.to_string()],
        )?;
        Ok(ev)
    }

    fn event_tx(tx: &rusqlite::Transaction, task: &str, kind: &str, payload: &Value) -> Result<()> {
        let seq: i64 = tx.query_row(
            "SELECT coalesce(max(seq),0)+1 FROM events WHERE task=?1",
            [task],
            |r| r.get(0),
        )?;
        let at = canon::now();
        let ev = json!({
            "eventId": format!("event-{}", canon::new_id()),
            "cursor": seq.to_string(),
            "seq": seq.to_string(),
            "sequence": seq.to_string(),
            "kind": kind,
            "type": kind,
            "occurredAt": at,
            "taskId": task,
            "payload": payload,
        });
        tx.execute(
            "INSERT INTO events VALUES(?1,?2,?3)",
            params![task, seq, ev.to_string()],
        )?;
        Ok(())
    }

    fn request_accepted_event_tx(
        tx: &rusqlite::Transaction,
        task: &str,
        key: &str,
    ) -> Result<()> {
        Self::event_tx(
            tx,
            task,
            "request.accepted",
            &json!({"requestKey":key}),
        )
    }

    /// Latest durable event for the task, used for the detail projection's
    /// lastEventAt/lastEventKind high-water facts.
    pub fn last_event(&self, task: &str) -> Option<Value> {
        self.db
            .query_row(
                "SELECT json FROM events WHERE task=?1 ORDER BY seq DESC LIMIT 1",
                [task],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
    }

    pub fn events(&self, task: &str, cursor: i64, limit: i64) -> Result<Value> {
        let mut q = self.db.prepare(
            "SELECT seq,json FROM events WHERE task=?1 AND seq>?2 ORDER BY seq LIMIT ?3",
        )?;
        let mut end = cursor;
        let rows = q.query_map(params![task, cursor, limit], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut items = vec![];
        for row in rows {
            let (seq, s) = row?;
            end = seq;
            let mut event: Value = serde_json::from_str(&s)?;
            event["cursor"] = json!(seq.to_string());
            items.push(event);
        }
        Ok(json!({"items":items,"cursor":end.to_string(),"cursorExpired":false}))
    }

    /// Idempotent replay check: same scope+key must carry a canonically-equal
    /// body, else IDEMPOTENCY_CONFLICT. Returns the stored result on hit.
    pub fn existing(&self, scope: &str, key: &str, body: &Value) -> Result<Option<Value>> {
        let row: Option<(String, String)> = self
            .db
            .query_row(
                "SELECT body,result FROM requests WHERE scope=?1 AND key=?2",
                params![scope, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        match row {
            None => Ok(None),
            Some((s, result)) => {
                if s.as_bytes() != canon::canonical(body)? {
                    bail!("IDEMPOTENCY_CONFLICT")
                }
                Ok(Some(serde_json::from_str(&result)?))
            }
        }
    }

    /// Generic request acceptance: ledger row + intake event, one transaction.
    pub fn accept(
        &mut self,
        scope: &str,
        key: &str,
        body: &Value,
        result: &Value,
        task: &str,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'ACCEPTED',?4)",
            params![
                scope,
                key,
                String::from_utf8(canon::canonical(body)?)?,
                result.to_string()
            ],
        )?;
        Self::request_accepted_event_tx(&tx, task, key)?;
        tx.commit()?;
        Ok(())
    }

    /// Atomic user admission (steer / follow-up): request row, intake event,
    /// optional re-engagement task update and exactly one ACCEPTED user
    /// message bound to the request key — one transaction, one commit.
    pub fn accept_user(
        &mut self,
        task: &str,
        key: &str,
        body: &Value,
        result: &Value,
        text: &str,
        mark_engaged: bool,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO requests VALUES(?1,?2,?3,'ACCEPTED',?4)",
            params![
                task,
                key,
                String::from_utf8(canon::canonical(body)?)?,
                result.to_string()
            ],
        )?;
        Self::request_accepted_event_tx(&tx, task, key)?;
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let mut t: Value = serde_json::from_str(&s)?;
        t["currentCommandId"] = json!(key);
        if mark_engaged {
            t["state"] = json!("RUNNING");
            t["quiescent"] = json!(false);
        }
        let n = t["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        t["stateVersion"] = json!(n.to_string());
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![task, t.to_string()],
        )?;
        Self::tx_insert_message(
            &tx,
            task,
            &NewMessage {
                role: "user",
                text,
                status: "ACCEPTED",
                source_request_key: Some(key),
                command_id: Some(key),
                generation: t["generation"].as_str(),
                segment_id: t["segmentId"].as_str(),
                ..NewMessage::default()
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Atomic task admission: task row + create-scope request + transcript
    /// meta + the instruction's ACCEPTED user message + intake event.
    pub fn accept_task(&mut self, task: &Value, key: &str, body: &Value) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2)",
            params![task["taskId"].as_str(), task.to_string()],
        )?;
        tx.execute(
            "INSERT INTO requests VALUES('create',?1,?2,'ACCEPTED',?3)",
            params![key, String::from_utf8(canon::canonical(body)?)?, task.to_string()],
        )?;
        tx.execute(
            "INSERT INTO public_transcript_meta VALUES(?1,1,0,?2,NULL,0,?3)",
            params![task["taskId"].as_str(), HISTORY_STRUCTURED, canon::now()],
        )?;
        Self::tx_insert_message(
            &tx,
            task["taskId"].as_str().unwrap_or(""),
            &NewMessage {
                role: "user",
                text: body["instruction"].as_str().unwrap_or(""),
                status: "ACCEPTED",
                source_request_key: Some(key),
                command_id: Some(key),
                generation: task["generation"].as_str(),
                segment_id: task["segmentId"].as_str(),
                ..NewMessage::default()
            },
        )?;
        Self::request_accepted_event_tx(&tx, task["taskId"].as_str().unwrap_or(""), key)?;
        tx.commit()?;
        Ok(())
    }

    /// Receipt update. A proven APPLIED/terminal is never downgraded by a
    /// later non-terminal phase. Terminal-negative phases project onto the
    /// bound user message row in the same transaction.
    pub fn receipt(&mut self, scope: &str, key: &str, phase: &str, result: &Value) -> Result<()> {
        const TERMINAL: &[&str] = &[
            "APPLIED",
            "CONSUMED",
            "REJECTED",
            "FAILED",
            "CANCELLED",
            "NOT_APPLIED",
            "UNKNOWN",
        ];
        let tx = self.db.transaction()?;
        tx.execute(
            "UPDATE requests SET phase=?3,result=?4 WHERE scope=?1 AND key=?2
               AND NOT (phase IN ('APPLIED','CONSUMED','REJECTED','FAILED','CANCELLED','NOT_APPLIED')
                        AND ?3 NOT IN ('APPLIED','CONSUMED','REJECTED','FAILED','CANCELLED','NOT_APPLIED'))",
            params![scope, key, phase, result.to_string()],
        )?;
        if TERMINAL.contains(&phase)
            && matches!(phase, "REJECTED" | "UNKNOWN" | "FAILED" | "NOT_APPLIED" | "CANCELLED")
        {
            // Resolve the owning task: task-scope requests use the scope;
            // create-scope requests resolve through the task's requestKey.
            let task: Option<String> = if scope == "create" {
                tx.query_row(
                    "SELECT id FROM tasks WHERE json_extract(json,'$.requestKey')=?1",
                    [key],
                    |r| r.get(0),
                )
                .optional()?
            } else {
                Some(scope.to_string())
            };
            if let Some(task) = task {
                let msg_status = if phase == "REJECTED" { "REJECTED" } else { "UNKNOWN" };
                let changed = tx.execute(
                    "UPDATE public_messages SET status=?3,revision=revision+1,updated_at=?4 WHERE task_id=?1 AND source_request_key=?2 AND role='user' AND status IN ('ACCEPTED','UNKNOWN') AND status!=?3",
                    params![task, key, msg_status, canon::now()],
                )?;
                if changed > 0 {
                    tx.execute(
                        "UPDATE public_transcript_meta SET transcript_revision=transcript_revision+1 WHERE task_id=?1",
                        [task],
                    )?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Settle whichever scope row owns `key` for this task (task scope first,
    /// then the create scope). No-op when no row exists.
    pub fn receipt_auto(&mut self, task: &str, key: &str, phase: &str, result: &Value) -> Result<()> {
        let scope: Option<String> = {
            let hit = self
                .db
                .query_row(
                    "SELECT 1 FROM requests WHERE scope=?1 AND key=?2",
                    params![task, key],
                    |r| r.get::<_, i64>(0),
                )
                .optional()?
                .is_some();
            if hit {
                Some(task.to_string())
            } else {
                let create_hit = self
                    .db
                    .query_row(
                        "SELECT 1 FROM requests WHERE scope='create' AND key=?1",
                        [key],
                        |r| r.get::<_, i64>(0),
                    )
                    .optional()?
                    .is_some();
                if create_hit {
                    Some("create".to_string())
                } else {
                    None
                }
            }
        };
        if let Some(scope) = scope {
            self.receipt(&scope, key, phase, result)?;
        }
        Ok(())
    }

    pub fn lookup(&self, scope: &str, key: &str) -> Result<Value> {
        let s: Option<String> = self
            .db
            .query_row(
                "SELECT result FROM requests WHERE scope=?1 AND key=?2",
                params![scope, key],
                |r| r.get(0),
            )
            .optional()?;
        Ok(serde_json::from_str(
            &s.ok_or_else(|| anyhow::anyhow!("REQUEST_NOT_FOUND"))?,
        )?)
    }

    /// Lookup that tolerates either scope for this task's request key.
    pub fn lookup_auto(&self, task: &str, key: &str) -> Result<Value> {
        if let Ok(v) = self.lookup(task, key) {
            return Ok(v);
        }
        self.lookup("create", key)
    }

    // ----- public message index (native-messages/1) -----

    fn message_text(raw: &str) -> (String, bool) {
        let mut text = raw.to_string();
        let mut truncated = false;
        let boundary = |s: &str, mut n: usize| {
            while !s.is_char_boundary(n) {
                n -= 1;
            }
            n
        };
        if text.len() > MESSAGE_TEXT_MAX {
            let n = boundary(&text, MESSAGE_TEXT_MAX);
            text.truncate(n);
            truncated = true;
        }
        while serde_json::to_string(&text)
            .map(|s| s.len() > MESSAGE_ESCAPED_MAX)
            .unwrap_or(false)
        {
            let n = boundary(&text, (text.len() / 2).max(1));
            text.truncate(n);
            truncated = true;
        }
        (text, truncated)
    }

    fn tx_ensure_meta(tx: &rusqlite::Transaction, task: &str) -> Result<()> {
        let exists = tx
            .query_row(
                "SELECT 1 FROM public_transcript_meta WHERE task_id=?1",
                [task],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        if exists {
            return Ok(());
        }
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let t: Value = serde_json::from_str(&s)?;
        tx.execute(
            "INSERT INTO public_transcript_meta VALUES(?1,1,0,?2,?3,0,?4)",
            params![task, "LEGACY_PREFIX", t["output"].as_str().unwrap_or(""), canon::now()],
        )?;
        Ok(())
    }

    fn tx_insert_message(tx: &rusqlite::Transaction, task: &str, m: &NewMessage) -> Result<i64> {
        Self::tx_ensure_meta(tx, task)?;
        let seq: i64 = tx.query_row(
            "SELECT next_sequence FROM public_transcript_meta WHERE task_id=?1",
            [task],
            |r| r.get(0),
        )?;
        let (text, truncated) = Self::message_text(m.text);
        let at = canon::now();
        tx.execute(
            "INSERT INTO public_messages VALUES(?1,?2,?3,1,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![
                task,
                format!("msg-{}", canon::new_id()),
                seq,
                m.role,
                text,
                truncated as i64,
                m.status,
                m.source_request_key,
                m.native_thread_id,
                m.native_turn_id,
                m.native_item_id,
                m.command_id,
                m.generation,
                m.segment_id,
                at,
                at
            ],
        )?;
        tx.execute(
            "UPDATE public_transcript_meta SET next_sequence=next_sequence+1,transcript_revision=transcript_revision+1 WHERE task_id=?1",
            [task],
        )?;
        Ok(seq)
    }

    /// Atomic assistant completion: durable (task,thread,item) row dedup, the
    /// bounded accumulated output and the ASSISTANT_MESSAGE event commit
    /// together. Returns false when the (thread,item) evidence was seen or
    /// the durable binding no longer names this thread+turn.
    pub fn record_agent_message(
        &mut self,
        task: &str,
        thread: &str,
        turn: &str,
        item_id: &str,
        text: &str,
    ) -> Result<bool> {
        let tx = self.db.transaction()?;
        Self::tx_ensure_meta(&tx, task)?;
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let mut t: Value = serde_json::from_str(&s)?;
        if t["nativeSessionId"].as_str() != Some(thread) || t["nativeTurnId"].as_str() != Some(turn) {
            tx.commit()?;
            return Ok(false);
        }
        let seen = tx
            .query_row(
                "SELECT 1 FROM public_messages WHERE task_id=?1 AND native_thread_id=?2 AND native_item_id=?3",
                params![task, thread, item_id],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        if seen {
            tx.commit()?;
            return Ok(false);
        }
        Self::tx_insert_message(
            &tx,
            task,
            &NewMessage {
                role: "assistant",
                text,
                status: "COMPLETED",
                native_thread_id: Some(thread),
                native_turn_id: Some(turn),
                native_item_id: Some(item_id),
                command_id: t["currentCommandId"].as_str(),
                generation: t["generation"].as_str(),
                segment_id: t["segmentId"].as_str(),
                ..NewMessage::default()
            },
        )?;
        let mut output = t["output"].as_str().unwrap_or("").to_owned();
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(text);
        if output.len() > 65536 {
            let mut start = output.len() - 65536;
            while !output.is_char_boundary(start) {
                start += 1;
            }
            output = output[start..].into();
        }
        t["output"] = json!(output);
        match t["publicItemIds"].as_array_mut() {
            Some(ids) => ids.push(json!(item_id)),
            None => t["publicItemIds"] = json!([item_id]),
        }
        let n = t["stateVersion"].as_str().unwrap_or("0").parse::<u64>()? + 1;
        t["stateVersion"] = json!(n.to_string());
        tx.execute(
            "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET json=excluded.json",
            params![task, t.to_string()],
        )?;
        Self::event_tx(
            &tx,
            task,
            "ASSISTANT_MESSAGE",
            &json!({"nativeItemId":item_id,"nativeTurnId":turn,"text":text}),
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// How many assistant rows exist for this native turn (used to decide
    /// whether TurnComplete.last_agent_message must be backfilled).
    pub fn assistant_count_for_turn(&self, task: &str, thread: &str, turn: &str) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM public_messages WHERE task_id=?1 AND native_thread_id=?2 AND native_turn_id=?3 AND role='assistant'",
            params![task, thread, turn],
            |r| r.get(0),
        )?)
    }

    /// Atomic user-input consumption proof: a native userMessage item bound
    /// to one durable request key marks that user row CONSUMED and writes the
    /// APPLIED receipt + input.consumed event in one transaction.
    pub fn record_user_consumption(
        &mut self,
        task: &str,
        key: &str,
        thread: &str,
        turn: &str,
        item_id: &str,
    ) -> Result<Consumption> {
        let tx = self.db.transaction()?;
        Self::tx_ensure_meta(&tx, task)?;
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let t: Value = serde_json::from_str(&s)?;
        if t["nativeSessionId"].as_str() != Some(thread) {
            tx.commit()?;
            return Ok(Consumption::Unbound("THREAD_MISMATCH"));
        }
        let scope = if tx
            .query_row(
                "SELECT 1 FROM requests WHERE scope=?1 AND key=?2",
                params![task, key],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some()
        {
            Some(task.to_string())
        } else if t["requestKey"].as_str() == Some(key)
            && tx
                .query_row(
                    "SELECT 1 FROM requests WHERE scope='create' AND key=?1",
                    [key],
                    |r| r.get::<_, i64>(0),
                )
                .optional()?
                .is_some()
        {
            Some("create".to_string())
        } else {
            None
        };
        let Some(scope) = scope else {
            tx.commit()?;
            return Ok(Consumption::Unbound("REQUEST_NOT_BOUND"));
        };
        let bound_turn = t["nativeTurnId"].as_str();
        if bound_turn != Some(turn) {
            tx.commit()?;
            return Ok(Consumption::Unbound("TURN_MISMATCH"));
        }
        let changed = tx.execute(
            "UPDATE public_messages SET status='CONSUMED',native_thread_id=?3,native_turn_id=?4,native_item_id=?5,revision=revision+1,updated_at=?6 WHERE task_id=?1 AND source_request_key=?2 AND role='user' AND status IN ('ACCEPTED','UNKNOWN')",
            params![task, key, thread, turn, item_id, canon::now()],
        )?;
        if changed > 0 {
            tx.execute(
                "UPDATE public_transcript_meta SET transcript_revision=transcript_revision+1 WHERE task_id=?1",
                [task],
            )?;
        } else {
            let prior: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT status,native_item_id FROM public_messages WHERE task_id=?1 AND source_request_key=?2 AND role='user'",
                    params![task, key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            match prior {
                Some((status, prior_item))
                    if status == "CONSUMED" && prior_item.as_deref() == Some(item_id) =>
                {
                    tx.commit()?;
                    return Ok(Consumption::Bound);
                }
                Some((status, _)) if status == "CONSUMED" || status == "REJECTED" => {
                    tx.commit()?;
                    return Ok(Consumption::Unbound("ALREADY_SETTLED"));
                }
                _ => {}
            }
        }
        let v = json!({"kind":"WORKER_LOCAL","requestKey":key,"state":"APPLIED","detail":"Native user-message item confirms input consumption","consumptionEvidence":{"nativeItemId":item_id,"nativeTurnId":turn,"clientId":key}});
        tx.execute(
            "UPDATE requests SET phase='APPLIED',result=?3 WHERE scope=?1 AND key=?2",
            params![scope, key, v.to_string()],
        )?;
        Self::event_tx(&tx, task, "input.consumed", &v)?;
        tx.commit()?;
        Ok(Consumption::Bound)
    }

    /// Mark the user message bound to `key` CONSUMED without native item
    /// evidence (submission-level confirmation). Receipt is not touched.
    pub fn mark_user_consumed(&mut self, task: &str, key: &str, thread: &str, turn: &str) -> Result<()> {
        let tx = self.db.transaction()?;
        let changed = tx.execute(
            "UPDATE public_messages SET status='CONSUMED',native_thread_id=?3,native_turn_id=?4,revision=revision+1,updated_at=?5 WHERE task_id=?1 AND source_request_key=?2 AND role='user' AND status IN ('ACCEPTED','UNKNOWN')",
            params![task, key, thread, turn, canon::now()],
        )?;
        if changed > 0 {
            tx.execute(
                "UPDATE public_transcript_meta SET transcript_revision=transcript_revision+1 WHERE task_id=?1",
                [task],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn messages(
        &mut self,
        task: &str,
        cursor: Option<i64>,
        before: Option<i64>,
        limit: i64,
    ) -> Result<Value> {
        let tx = self.db.transaction()?;
        Self::tx_ensure_meta(&tx, task)?;
        let (next_sequence, revision, mode, legacy_text, legacy_truncated) = tx.query_row(
            "SELECT next_sequence,transcript_revision,history_mode,legacy_text,legacy_text_truncated FROM public_transcript_meta WHERE task_id=?1",
            [task],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            },
        )?;
        let s: String = tx.query_row("SELECT json FROM tasks WHERE id=?1", [task], |r| r.get(0))?;
        let t: Value = serde_json::from_str(&s)?;
        const COLS: &str = "message_id,sequence,revision,role,text,text_truncated,status,source_request_key,native_thread_id,native_turn_id,native_item_id,command_id,generation,segment_id,occurred_at,updated_at";
        let forward = cursor.is_some();
        let mut rows: Vec<Value> = vec![];
        {
            let mut q = if forward {
                tx.prepare(&format!(
                    "SELECT {COLS} FROM public_messages WHERE task_id=?1 AND sequence>?2 ORDER BY sequence ASC LIMIT ?3"
                ))?
            } else {
                tx.prepare(&format!(
                    "SELECT {COLS} FROM public_messages WHERE task_id=?1 AND sequence<?2 ORDER BY sequence DESC LIMIT ?3"
                ))?
            };
            let bound = if forward {
                cursor.unwrap_or(0)
            } else {
                before.unwrap_or(i64::MAX)
            };
            let fetched = q.query_map(params![task, bound, limit + 1], |r| {
                Self::message_row_json(r)
            })?;
            for row in fetched {
                rows.push(row?);
            }
        }
        let mut items: Vec<Value> = vec![];
        let mut bytes = 2usize;
        for item in rows.iter() {
            if items.len() as i64 >= limit {
                break;
            }
            let need = serde_json::to_vec(item)?.len() + usize::from(!items.is_empty());
            if !items.is_empty() && bytes + need > MESSAGE_PAGE_MAX {
                break;
            }
            bytes += need;
            items.push(item.clone());
        }
        let has_more = rows.len() > items.len();
        if !forward {
            items.reverse();
        }
        let seq_of =
            |v: Option<&Value>| v.map(|i| i["sequence"].as_str().unwrap_or("0").to_owned());
        let first = seq_of(items.first());
        let last = seq_of(items.last());
        let out_cursor = if forward {
            json!(last.or_else(|| cursor.map(|c| c.to_string())))
        } else {
            json!(first)
        };
        let legacy = if mode == "LEGACY_PREFIX" {
            json!({"text":legacy_text.unwrap_or_default(),"textTruncated":legacy_truncated != 0,"kind":"BOUNDED_ACCUMULATED_PUBLIC_OUTPUT"})
        } else {
            Value::Null
        };
        let v = json!({
            "messageSchemaVersion": "native-messages/1",
            "taskId": task,
            "storeId": self.identity["storeId"],
            "transcriptRevision": revision.to_string(),
            "items": items,
            "firstSequence": first,
            "cursor": out_cursor,
            "latestSequence": (next_sequence - 1).to_string(),
            "hasMore": has_more,
            "cursorExpired": false,
            "historyMode": mode,
            "legacy": legacy,
            "text": t["output"],
            "turns": t["turns"],
            "coverage": "BOUNDED_PUBLIC_MESSAGES"
        });
        if serde_json::to_vec(&v)?.len() > MESSAGE_RESPONSE_MAX {
            bail!("MESSAGE_PAGE_TOO_LARGE")
        }
        tx.commit()?;
        Ok(v)
    }

    fn message_row_json(r: &rusqlite::Row) -> rusqlite::Result<Value> {
        let mut v = json!({
            "messageId": r.get::<_, String>(0)?,
            "sequence": r.get::<_, i64>(1)?.to_string(),
            "revision": r.get::<_, i64>(2)?.to_string(),
            "role": r.get::<_, String>(3)?,
            "text": r.get::<_, String>(4)?,
            "textTruncated": r.get::<_, i64>(5)? != 0,
            "status": r.get::<_, String>(6)?,
            "occurredAt": r.get::<_, String>(14)?,
            "updatedAt": r.get::<_, String>(15)?,
        });
        for (i, k) in [
            (7, "sourceRequestKey"),
            (8, "nativeThreadId"),
            (9, "nativeTurnId"),
            (10, "nativeItemId"),
            (11, "commandId"),
            (12, "generation"),
            (13, "segmentId"),
        ] {
            if let Some(s) = r.get::<_, Option<String>>(i)? {
                v[k] = json!(s);
            }
        }
        Ok(v)
    }
}
