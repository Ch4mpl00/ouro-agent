// read_file: a UTF-8 text file from disk, path absolute or relative to the
// repo root (MCP runs from it, so the working directory is the anchor) — and
// which local files the read tools (read_file, read_pdf) may open at all.
//
// They take a path from the model, and the model reads text outsiders wrote
// — mail, channel posts, fetched pages. Unconfined, an injected
// `read_file("/proc/self/environ")` handed it every secret of this process,
// and send_telegram_message / fetch_url could carry them out. A path is
// readable only when its real location — symlinks resolved, `..` collapsed —
// lies under one of the roots.

use std::path::{Component, Path, PathBuf};

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::Deserialize;
use serde_json::json;

use crate::server::{McpTools, ToolResult, respond};

#[derive(Clone, Debug)]
pub struct FileAccess {
    roots: Vec<PathBuf>,
    cwd: PathBuf,
}

// `path.resolve`: absolute against `cwd`, `.` and `..` collapsed lexically.
fn resolve(cwd: &Path, input: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for component in cwd.join(input).components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other),
        }
    }
    out
}

// MCP_READ_ROOTS (path-list, ":"-separated) when set; otherwise where the
// read tools' files actually live: downloaded attachments (STORAGE_DIR) and
// the skill directories.
pub fn default_read_roots(env: &dyn Fn(&str) -> Option<String>, cwd: &Path) -> Vec<PathBuf> {
    let listed: Vec<String> = env("MCP_READ_ROOTS")
        .map(|v| std::env::split_paths(&v).map(|p| p.to_string_lossy().into_owned()).collect())
        .unwrap_or_default();
    let listed: Vec<&String> = listed.iter().filter(|r| !r.trim().is_empty()).collect();
    if !listed.is_empty() {
        return listed.into_iter().map(|r| resolve(cwd, r)).collect();
    }
    let storage = env("STORAGE_DIR").unwrap_or_else(|| "./storage".into());
    vec![resolve(cwd, &storage), resolve(cwd, "skills"), resolve(cwd, "skills.default")]
}

impl FileAccess {
    pub fn new(roots: &[PathBuf], cwd: &Path) -> Self {
        Self { roots: roots.iter().map(|r| resolve(cwd, &r.to_string_lossy())).collect(), cwd: cwd.to_owned() }
    }

    // The real path to open, or an error saying the path is outside the roots
    // (or doesn't exist).
    pub async fn readable_path(&self, input: &str) -> anyhow::Result<PathBuf> {
        let real = tokio::fs::canonicalize(resolve(&self.cwd, input)).await.map_err(|err| {
            anyhow::anyhow!("cannot read {}: {err}", serde_json::to_string(input).unwrap_or_default())
        })?;
        // Roots may appear after startup (the first attachment download), or
        // be replaced: their current real paths are compared on every read.
        for root in &self.roots {
            if let Ok(root) = tokio::fs::canonicalize(root).await
                && real.starts_with(&root)
            {
                return Ok(real);
            }
        }
        let roots: Vec<String> = self.roots.iter().map(|r| r.display().to_string()).collect();
        anyhow::bail!(
            "refusing to read {}: outside the readable roots ({})",
            serde_json::to_string(input).unwrap_or_default(),
            roots.join(", ")
        )
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ReadFileParams {
    /// Absolute path or path relative to the repo root.
    path: String,
}

#[tool_router(router = fs_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "read_file",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "Read a text file",
        description = "Read a UTF-8 text file (markdown, txt, etc) and return its contents. \
            Use this to load skill instructions like `skills/telegram.md` when \
            handling a signal. Path may be absolute or relative to the repo root."
    )]
    async fn read_file(&self, Parameters(p): Parameters<ReadFileParams>) -> ToolResult {
        respond(
            async {
                let resolved = self.deps.files.readable_path(&p.path).await?;
                let content = tokio::fs::read_to_string(&resolved).await?;
                Ok(json!({ "path": resolved, "content": content }))
            }
            .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_roots_are_mcp_read_roots_when_set_resolved_against_cwd() {
        let env = |name: &str| (name == "MCP_READ_ROOTS").then(|| "a:/b".to_owned());
        assert_eq!(default_read_roots(&env, Path::new("/srv")), [PathBuf::from("/srv/a"), PathBuf::from("/b")]);
    }

    #[test]
    fn default_roots_are_storage_and_the_skill_directories_otherwise() {
        let env = |name: &str| (name == "STORAGE_DIR").then(|| "/data/storage".to_owned());
        assert_eq!(
            default_read_roots(&env, Path::new("/app")),
            [PathBuf::from("/data/storage"), PathBuf::from("/app/skills"), PathBuf::from("/app/skills.default"),]
        );
    }
}
