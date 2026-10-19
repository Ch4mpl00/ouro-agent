// Skills: two-layer overlay of markdown files.
//
//   skills/<name>.md          — live (gitignored, a Docker volume). Mutable;
//                               the primary agent and authorized dreaming write here.
//   skills.default/<name>.md  — defaults (git-tracked). The shipped baseline.
//
// read_skill returns the live version if present, else the default, else
// None. save_skill always writes the live overlay — defaults are never touched
// at runtime. Deleting a live file restores its default if one exists;
// a live-only skill is removed instead.
//
// The improver (crates/evals) appends lessons to `skills/<name>.patch.md`; the runtime
// glues them onto the END of a skill's system text with `append_patch`. The
// MCP server's read_skill export (crates/mcp/src/skills.rs) composes it the
// same way — change one, change both.

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use async_trait::async_trait;
use regex::Regex;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const PATCH_MARKER: &str = "<!-- improver-patch -->";

pub fn append_patch(system: &str, patch: &str) -> String {
    let trimmed = patch.trim();
    if trimmed.is_empty() {
        return system.to_owned();
    }
    format!("{}\n\n{PATCH_MARKER}\n{trimmed}\n", system.trim_end())
}

static NAME_PATTERN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^[a-z0-9][a-z0-9_-]*$").unwrap());
static FRONTMATTER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^---\s*\r?\n([\s\S]*?)\r?\n---\s*\r?\n").unwrap());
static TOOLS_WILDCARD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?m)^tools:\s*\*\s*$").unwrap());
static TOOLS_ARRAY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?m)^tools:\s*\[(.*?)\]\s*$").unwrap());
static TOOL_NAME: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-z_][a-z0-9_]*$").unwrap());
static ROUTABLE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?m)^routable:[ \t]*([^\r\n]*)").unwrap());

fn validate_name(name: &str) -> anyhow::Result<()> {
    if !NAME_PATTERN.is_match(name) {
        anyhow::bail!("Invalid skill name \"{name}\". Use [a-z0-9][a-z0-9_-]* only.");
    }
    Ok(())
}

// Every skill declares its MCP tools: `tools: []` (none), `tools: [a, b]`
// (an allow-list) or `tools: *` (all of them).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkillTools {
    All,
    Named(Vec<String>),
}

impl SkillTools {
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            SkillTools::All => "*".into(),
            SkillTools::Named(tools) => tools.clone().into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SkillSource {
    Live,
    Default,
}

impl SkillSource {
    pub fn as_str(self) -> &'static str {
        match self {
            SkillSource::Live => "live",
            SkillSource::Default => "default",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillFile {
    pub body: String,
    pub tools: SkillTools,
    pub source: SkillSource,
    pub routable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillEntry {
    pub name: String,
    pub source: SkillSource,
    pub size_bytes: u64,
    pub modified_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Written {
    pub path: String,
    pub size_bytes: usize,
}

fn parse_skill(name: &str, raw: &str, source: SkillSource) -> anyhow::Result<SkillFile> {
    let Some(m) = FRONTMATTER.captures(raw) else {
        anyhow::bail!(
            "skill \"{name}\" ({}): missing frontmatter. Every skill must start with a `---\\ntools: ...\\n---` block. Use `tools: *` for all MCP tools, `tools: []` for none, or `tools: [a, b, c]` for an explicit allow-list.",
            source.as_str()
        );
    };
    let frontmatter = m.get(1).map_or("", |g| g.as_str());
    let body = raw[m.get(0).map_or(0, |g| g.end())..].to_owned();
    let routable = match ROUTABLE.captures(frontmatter) {
        None => true,
        Some(value) => match value[1].split('#').next().unwrap_or_default().trim() {
            "true" => true,
            "false" => false,
            _ => anyhow::bail!("skill \"{name}\": routable must be true or false"),
        },
    };
    if TOOLS_WILDCARD.is_match(frontmatter) {
        return Ok(SkillFile { body, tools: SkillTools::All, source, routable });
    }
    let Some(t) = TOOLS_ARRAY.captures(frontmatter) else {
        anyhow::bail!(
            "skill \"{name}\" ({}): frontmatter must declare `tools: *`, `tools: []`, or `tools: [a, b, c]`.",
            source.as_str()
        );
    };
    let inner = t.get(1).map_or("", |g| g.as_str()).trim();
    let tools: Vec<String> = if inner.is_empty() {
        Vec::new()
    } else {
        inner.split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_owned).collect()
    };
    for tool in &tools {
        if !TOOL_NAME.is_match(tool) {
            anyhow::bail!("skill \"{name}\" ({}): tool name \"{tool}\" is not a valid identifier", source.as_str());
        }
    }
    Ok(SkillFile { body, tools: SkillTools::Named(tools), source, routable })
}

/// Stable JSON fingerprint, independent of object insertion order.
pub fn hash_value(value: &Value) -> String {
    let mut value = value.clone();
    value.sort_all_objects();
    hex::encode(Sha256::digest(value.to_string()))
}

/// The same live/default resolution and patch composition as execution.
/// Generation and the decision to refresh an entry belong to the workflow.
pub async fn catalog_snapshot(store: &dyn SkillStore) -> anyhow::Result<Value> {
    let mut skills = Vec::new();
    for entry in store.list_skills().await? {
        let Some(mut skill) = store.read_skill(&entry.name).await? else { continue };
        if let Some(patch) = store.read_patch(&entry.name).await? {
            skill.body = append_patch(&skill.body, &patch);
        }
        let basis = json!({"content":skill.body,"tools":skill.tools.to_json(),"routable":skill.routable});
        skills.push(json!({"name":entry.name,"content":skill.body,"tools":skill.tools.to_json(),
            "routable":skill.routable,"content_hash":hash_value(&basis)}));
    }
    skills.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Ok(json!({"skills":skills}))
}

// The tools a skill's content declares, or why it is not a loadable skill —
// what write_skill checks before it saves.
pub fn declared_tools(name: &str, content: &str) -> anyhow::Result<SkillTools> {
    Ok(parse_skill(name, content, SkillSource::Live)?.tools)
}

#[async_trait]
pub trait SkillStore: Send + Sync {
    // Parsed skill, live → default. Fails on broken frontmatter.
    async fn read_skill(&self, name: &str) -> anyhow::Result<Option<SkillFile>>;
    // The improver's patch for a skill: live overlay only, no fallback.
    async fn read_patch(&self, name: &str) -> anyhow::Result<Option<String>>;
    // Always the live overlay; defaults stay intact.
    async fn save_skill(&self, name: &str, content: &str) -> anyhow::Result<Written>;
    // Live ∪ defaults; `source` says which layer is active.
    async fn list_skills(&self) -> anyhow::Result<Vec<SkillEntry>>;
}

pub struct FsSkillStore {
    pub live_dir: PathBuf,
    pub defaults_dir: PathBuf,
}

impl FsSkillStore {
    // `skills/` and `skills.default/` under `root` (the repo root, /app in
    // the image).
    pub fn new(root: &Path) -> FsSkillStore {
        FsSkillStore { live_dir: root.join("skills"), defaults_dir: root.join("skills.default") }
    }

    // Every skill on disk parsed; one combined error listing each broken
    // frontmatter and each tool MCP doesn't have. Run at startup, so a
    // misconfiguration stops the agent up front, not mid-signal. An empty
    // `known_mcp_tools` skips the cross-check.
    pub async fn validate_all(&self, known_mcp_tools: &[String]) -> anyhow::Result<()> {
        let mut known: Vec<&String> = known_mcp_tools.iter().collect();
        known.sort();
        let mut errors = Vec::new();
        for entry in self.list_skills().await? {
            let parsed = match self.read_skill(&entry.name).await {
                Ok(Some(parsed)) => parsed,
                Ok(None) => continue,
                Err(err) => {
                    errors.push(err.to_string());
                    continue;
                }
            };
            if known.is_empty() {
                continue;
            }
            let SkillTools::Named(tools) = &parsed.tools else { continue };
            for tool in tools {
                if !known_mcp_tools.contains(tool) {
                    errors.push(format!(
                        "skill \"{}\" ({}): declares tool \"{tool}\" which is not in the MCP registry. Known tools: {}",
                        entry.name,
                        parsed.source.as_str(),
                        known.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                    ));
                }
            }
        }
        if !errors.is_empty() {
            anyhow::bail!(
                "Skill validation failed ({} issue(s)):\n{}",
                errors.len(),
                errors.iter().map(|e| format!("  - {e}")).collect::<Vec<_>>().join("\n")
            );
        }
        Ok(())
    }

    // The active file's exact text, frontmatter included — the contract the
    // judge scores a node against. Live → default, None when neither exists.
    pub async fn read_skill_raw(&self, name: &str) -> anyhow::Result<Option<String>> {
        validate_name(name)?;
        if let Some(live) = read_if_exists(&self.live_dir.join(format!("{name}.md"))).await? {
            return Ok(Some(live));
        }
        read_if_exists(&self.defaults_dir.join(format!("{name}.md"))).await
    }

    // Write the improver's patch overlay (skills/<name>.patch.md). Deleting
    // the file is the clean revert (the body falls back to its unpatched form).
    pub async fn save_patch(&self, name: &str, content: &str) -> anyhow::Result<Written> {
        validate_name(name)?;
        tokio::fs::create_dir_all(&self.live_dir).await?;
        let target = self.live_dir.join(format!("{name}.patch.md"));
        tokio::fs::write(&target, content).await?;
        Ok(Written { path: target.to_string_lossy().into_owned(), size_bytes: content.len() })
    }

    // Delete the patch overlay entirely — the auto-revert's clean reset to the
    // body. True if a file was removed, false if there was none.
    pub async fn delete_patch(&self, name: &str) -> anyhow::Result<bool> {
        validate_name(name)?;
        match tokio::fs::remove_file(self.live_dir.join(format!("{name}.patch.md"))).await {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(err.into()),
        }
    }
}

async fn read_if_exists(path: &Path) -> anyhow::Result<Option<String>> {
    match tokio::fs::read_to_string(path).await {
        Ok(text) => Ok(Some(text)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn iso(time: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(time).format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

async fn read_dir(dir: &Path, source: SkillSource) -> anyhow::Result<Vec<SkillEntry>> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut out = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let file = entry.file_name().to_string_lossy().into_owned();
        // A `<name>.patch.md` overlay is not a skill of its own.
        let Some(name) = file.strip_suffix(".md") else { continue };
        if file.ends_with(".patch.md") || !NAME_PATTERN.is_match(name) {
            continue;
        }
        let meta = match tokio::fs::metadata(entry.path()).await {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if !meta.is_file() {
            continue;
        }
        out.push(SkillEntry {
            name: name.to_owned(),
            source,
            size_bytes: meta.len(),
            modified_at: meta.modified().map(iso).unwrap_or_default(),
        });
    }
    Ok(out)
}

// JS localeCompare on skill names (ICU root collation, case-insensitive
// first): "_" before "-", punctuation before digits before letters.
pub fn locale_compare(a: &str, b: &str) -> Ordering {
    fn key(c: char) -> (u8, char) {
        match c {
            '_' => (0, c),
            '-' => (1, c),
            '0'..='9' => (2, c),
            c if c.is_alphabetic() => (3, c.to_ascii_lowercase()),
            c => (4, c),
        }
    }
    let primary = a.chars().map(key).cmp(b.chars().map(key));
    // Same letters: lowercase sorts first.
    primary.then_with(|| a.chars().map(|c| c.is_uppercase()).cmp(b.chars().map(|c| c.is_uppercase())))
}

#[async_trait]
impl SkillStore for FsSkillStore {
    async fn read_skill(&self, name: &str) -> anyhow::Result<Option<SkillFile>> {
        validate_name(name)?;
        if let Some(live) = read_if_exists(&self.live_dir.join(format!("{name}.md"))).await? {
            return parse_skill(name, &live, SkillSource::Live).map(Some);
        }
        if let Some(default) = read_if_exists(&self.defaults_dir.join(format!("{name}.md"))).await? {
            return parse_skill(name, &default, SkillSource::Default).map(Some);
        }
        Ok(None)
    }

    async fn read_patch(&self, name: &str) -> anyhow::Result<Option<String>> {
        validate_name(name)?;
        read_if_exists(&self.live_dir.join(format!("{name}.patch.md"))).await
    }

    async fn save_skill(&self, name: &str, content: &str) -> anyhow::Result<Written> {
        validate_name(name)?;
        // Never write what read_skill would refuse: a broken live skill takes
        // the skill down and fails validation at the next startup.
        parse_skill(name, content, SkillSource::Live)?;
        tokio::fs::create_dir_all(&self.live_dir).await?;
        let target = self.live_dir.join(format!("{name}.md"));
        tokio::fs::write(&target, content).await?;
        Ok(Written { path: target.to_string_lossy().into_owned(), size_bytes: content.len() })
    }

    async fn list_skills(&self) -> anyhow::Result<Vec<SkillEntry>> {
        let (live, defaults) = futures::join!(
            read_dir(&self.live_dir, SkillSource::Live),
            read_dir(&self.defaults_dir, SkillSource::Default)
        );
        let mut by_name: Vec<SkillEntry> = defaults?;
        for entry in live? {
            match by_name.iter_mut().find(|e| e.name == entry.name) {
                Some(slot) => *slot = entry,
                None => by_name.push(entry),
            }
        }
        by_name.sort_by(|a, b| locale_compare(&a.name, &b.name));
        Ok(by_name)
    }
}
