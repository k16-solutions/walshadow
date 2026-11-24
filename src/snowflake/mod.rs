//! k16 fork: the Snowflake destination. Snowflake delivery, materialization
//! and durable recovery primitives, plus destination selection
//! ([`config`]).
//!
//! The files under `src/snowflake/ext/` are not part of this module. Each
//! extends the upstream module at the same path as its `snowflake` child
//! module; see FORK.md.
pub mod config;
pub mod generation;
pub mod http;
pub mod prewarm;
pub mod runtime;
pub mod sql;
pub mod stage;
pub mod state;
pub mod types;

#[cfg(test)]
mod sql_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod types_tests;
