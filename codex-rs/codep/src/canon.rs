//! Canonical JSON and shared helpers for the worker-host contract.
//!
//! Canonical form: integers-only numbers (any float anywhere rejects with
//! `INTEGER_REQUIRED`), object keys emitted in sorted order, compact
//! separators. Two canonically-equal bodies produce identical bytes, which is
//! what idempotency replay compares.

use std::collections::BTreeMap;

use anyhow::Result;
use anyhow::bail;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

pub fn canonical(v: &Value) -> Result<Vec<u8>> {
    check_integers(v)?;
    Ok(serde_json::to_vec(&sorted(v))?)
}

fn check_integers(v: &Value) -> Result<()> {
    match v {
        Value::Number(n) if !n.is_i64() && !n.is_u64() => bail!("INTEGER_REQUIRED"),
        Value::Array(items) => {
            for item in items {
                check_integers(item)?;
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                check_integers(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn sorted(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut ordered: BTreeMap<String, Value> = BTreeMap::new();
            for (k, val) in map {
                ordered.insert(k.clone(), sorted(val));
            }
            Value::Object(ordered.into_iter().collect())
        }
        Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
        _ => v.clone(),
    }
}

pub fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn canonical_sha256(v: &Value) -> Result<String> {
    Ok(sha256_hex(canonical(v)?))
}

/// RFC3339 UTC timestamp with milliseconds.
pub fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Random 32-hex id fragment.
pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}
