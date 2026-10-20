// The agent's runtime library. Clients and stores are injected into the
// engine; the eval tooling (crates/evals) reads the same agent.db and
// canonical Langfuse trace shapes. agent-supervisor composes the runtime;
// history lives in .claude/tasks/rust-agent.md.

pub mod agent;
pub mod codex;
pub mod db;
pub mod decisions;
pub mod durable;
pub mod errors;
pub mod flows;
pub mod generation;
pub mod guard;
pub mod langfuse;
pub mod local_recorder;
pub mod mcp;
pub mod models;
pub mod providers;
pub mod schedule;
pub mod session;
pub mod skills;
pub mod tool_results;
pub mod trace_model;
pub mod tracing;

pub mod activity;
pub mod supervisor;
