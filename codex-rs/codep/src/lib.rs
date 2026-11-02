//! codep — worker-host service wrapping codex-core's ThreadManager in-process.
//!
//! The library surface exists so integration tests can reach the durable
//! store, the driver-side projection helpers and the axum dispatch without a
//! live model.

pub mod canon;
pub mod driver;
pub mod host;
pub mod store;
