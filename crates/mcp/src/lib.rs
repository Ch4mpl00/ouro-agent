// mcp-tools: the stateless MCP server. Wraps Gmail / Telegram / Monobank as
// primitive tools and runs the pollers that turn external events into
// signals. One domain per file; `main.rs` (the server) and `src/bin/*` (the
// CLIs) are the composition roots.

pub mod cli;
pub mod db;
pub mod embeddings;
pub mod eval;
pub mod fetch;
pub mod fs;
pub mod gateway;
pub mod gmail;
pub mod knowledge;
pub mod memory;
pub mod monobank;
pub mod news;
pub mod pdf;
pub mod peer;
pub mod pg;
pub mod scheduler;
pub mod server;
pub mod settings;
pub mod signals;
pub mod skills;
pub mod telegram;
pub mod time;
pub mod toolsets;
pub mod userbot;
