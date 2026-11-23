//! k16 fork: helpers shared by the fork's own modules.
//!
//! Every other file under `src/fork/` mirrors an upstream module path and is
//! attached to that module as its `fork` child (`#[path = ...] mod fork;`),
//! so it can use the upstream module's private items while the upstream file
//! carries only one-line hooks. See FORK.md.

#[cfg(test)]
pub(crate) mod test_support;
