// The skills export: the agent's skill files (live overlay `skills/` over the
// shipped `skills.default/`), read-only, for clients that want to see what
// the agent runs — the ChatGPT tunnel, mostly.
//
// Sections:
//   1. catalog — active files, metadata, and the patch composition
//   2. tools   — read-only skills export
//
// The improver writes an append-only overlay at `skills/<name>.patch.md`,
// and the agent glues it onto the end of the body (agent `appendPatch`). The
// two packages may not share code, so `append_patch` here re-implements it
// byte for byte; a test pins the composed output as a literal.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::server::{McpTools, ToolResult, respond};
use crate::time::iso;

// ── 1. catalog ───────────────────────────────────────────────────────────────

const PATCH_MARKER: &str = "<!-- improver-patch -->";

fn re(pattern: &str) -> regex::Regex {
    regex::Regex::new(pattern).expect("valid regex")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SkillSource {
    Live,
    Default,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Tools {
    // `tools: *`
    All(&'static str),
    List(Vec<String>),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillSummary {
    pub id: String,
    pub name: String,
    pub file_name: String,
    pub title: String,
    pub description: String,
    pub tools: Option<Tools>,
    pub source: SkillSource,
    pub size_bytes: usize,
    pub modified_at: String,
    // Whether an improver patch is in force.
    pub patched: bool,
}

#[derive(Debug, Clone)]
pub struct SkillDocument {
    pub summary: SkillSummary,
    // The exact bytes of the active file — the editable source.
    pub content: String,
    // The exact bytes of the overlay, or None.
    pub patch: Option<String>,
    // What the agent runs: body without frontmatter + patch.
    pub effective_instructions: String,
}

impl SkillDocument {
    pub fn routable(&self) -> anyhow::Result<bool> {
        let block = frontmatter_re().captures(&self.content);
        let frontmatter = block.as_ref().and_then(|c| c.get(1)).map_or("", |m| m.as_str());
        let pattern = re(r"(?m)^routable:[ \t]*([^\r\n]*)");
        match pattern.captures(frontmatter) {
            None => Ok(true),
            Some(value) => match value[1].split('#').next().unwrap_or_default().trim() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => anyhow::bail!("skill {}: routable must be true or false", self.summary.name),
            },
        }
    }
    /// Same basis as the agent snapshot; pinned by the cross-crate contract test.
    pub fn content_hash(&self) -> anyhow::Result<String> {
        let mut basis = json!({"content":self.effective_instructions,
            "tools":self.summary.tools,"routable":self.routable()?});
        basis.sort_all_objects();
        Ok(hex::encode(Sha256::digest(basis.to_string())))
    }
}

#[derive(Clone)]
pub struct SkillCatalog {
    live_dir: PathBuf,
    defaults_dir: PathBuf,
}

struct FileRef {
    name: String,
    file_name: String,
    path: PathBuf,
    source: SkillSource,
    modified: Option<std::time::SystemTime>,
}

fn skill_name_ok(name: &str) -> bool {
    re(r"(?i)^[a-z0-9][a-z0-9_-]*$").is_match(name)
}

fn frontmatter_re() -> regex::Regex {
    re(r"^---\s*\r?\n([\s\S]*?)\r?\n---\s*\r?\n")
}

fn body_without_frontmatter(raw: &str) -> &str {
    match frontmatter_re().find(raw) {
        Some(m) => &raw[m.end()..],
        None => raw,
    }
}

// Byte-for-byte the agent's `appendPatch`.
pub fn append_patch(body: &str, patch: &str) -> String {
    let trimmed = patch.trim();
    if trimmed.is_empty() {
        return body.to_owned();
    }
    format!("{}\n\n{PATCH_MARKER}\n{trimmed}\n", body.trim_end())
}

fn fallback_title(name: &str) -> String {
    re(r"[-_]+")
        .split(name)
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut chars = p.chars();
            chars.next().map(|c| c.to_uppercase().chain(chars).collect::<String>()).unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn extract_title(name: &str, raw: &str) -> String {
    re(r"(?m)^#\s+(.+?)\s*$")
        .captures(body_without_frontmatter(raw))
        .map(|c| c[1].trim().to_owned())
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| fallback_title(name))
}

fn clean_markdown(text: &str) -> String {
    let s = re(r"`([^`]+)`").replace_all(text, "$1");
    let s = re(r"\[([^\]]+)]\([^)]+\)").replace_all(&s, "$1");
    let s = re(r"[*_~]").replace_all(&s, "");
    re(r"\s+").replace_all(&s, " ").trim().to_owned()
}

fn extract_description(raw: &str, title: &str) -> String {
    let body = body_without_frontmatter(raw);
    let after_title = match re(r"(?m)^#\s+.+?\s*$").find(body) {
        Some(m) => &body[m.end()..],
        None => body,
    };
    let list_item = re(r"^[-*]\s");
    let paragraph = re(r"\r?\n\s*\r?\n").split(after_title).map(str::trim).find(|block| {
        !block.is_empty()
            && !block.starts_with('#')
            && !block.starts_with("```")
            && !block.starts_with('|')
            && !list_item.is_match(block)
    });
    let description = clean_markdown(&paragraph.map_or_else(|| format!("Instructions for {title}."), str::to_owned));
    if description.chars().count() <= 280 {
        return description;
    }
    format!("{}...", description.chars().take(277).collect::<String>().trim_end())
}

fn extract_tools(raw: &str) -> Option<Tools> {
    let frontmatter = frontmatter_re().captures(raw)?.get(1)?.as_str().to_owned();
    if re(r"(?m)^tools:\s*\*\s*$").is_match(&frontmatter) {
        return Some(Tools::All("*"));
    }
    let array = re(r"(?m)^tools:\s*\[(.*?)\]\s*$").captures(&frontmatter)?.get(1)?.as_str().to_owned();
    Some(Tools::List(array.split(',').map(str::trim).filter(|t| !t.is_empty()).map(str::to_owned).collect()))
}

async fn list_files(dir: &Path, source: SkillSource) -> anyhow::Result<Vec<FileRef>> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut out = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if !entry.file_type().await?.is_file() || !file_name.ends_with(".md") || file_name.ends_with(".patch.md") {
            continue;
        }
        let name = file_name.trim_end_matches(".md").to_owned();
        if !skill_name_ok(&name) {
            continue;
        }
        let modified = entry.metadata().await?.modified().ok();
        out.push(FileRef { name, file_name, path: entry.path(), source, modified });
    }
    Ok(out)
}

impl SkillCatalog {
    pub fn new(live_dir: PathBuf, defaults_dir: PathBuf) -> Self {
        Self { live_dir, defaults_dir }
    }

    // Live overrides default, file by file.
    async fn active_files(&self) -> anyhow::Result<BTreeMap<String, FileRef>> {
        let mut files = BTreeMap::new();
        for file in list_files(&self.defaults_dir, SkillSource::Default).await? {
            files.insert(file.file_name.clone(), file);
        }
        for file in list_files(&self.live_dir, SkillSource::Live).await? {
            files.insert(file.file_name.clone(), file);
        }
        Ok(files)
    }

    // Live layer only: defaults never ship a patch, and a deleted patch must
    // revert the skill rather than linger.
    async fn read_patch(&self, name: &str) -> anyhow::Result<Option<String>> {
        match tokio::fs::read_to_string(self.live_dir.join(format!("{name}.patch.md"))).await {
            Ok(patch) => Ok(Some(patch)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn read_ref(&self, file: &FileRef) -> anyhow::Result<SkillDocument> {
        let content = tokio::fs::read_to_string(&file.path).await?;
        let patch = self.read_patch(&file.name).await?;
        let title = extract_title(&file.name, &content);
        let summary = SkillSummary {
            id: file.name.clone(),
            name: file.name.clone(),
            file_name: file.file_name.clone(),
            description: extract_description(&content, &title),
            title,
            tools: extract_tools(&content),
            source: file.source,
            size_bytes: content.len(),
            modified_at: file.modified.map(|m| iso(m.into())).unwrap_or_default(),
            patched: patch.as_deref().is_some_and(|p| !p.trim().is_empty()),
        };
        let effective_instructions = append_patch(body_without_frontmatter(&content), patch.as_deref().unwrap_or(""));
        Ok(SkillDocument { summary, content, patch, effective_instructions })
    }

    pub async fn list_skills(&self) -> anyhow::Result<Vec<SkillSummary>> {
        let mut out = Vec::new();
        for file in self.active_files().await?.values() {
            out.push(self.read_ref(file).await?.summary);
        }
        out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        Ok(out)
    }

    pub async fn read_skill(&self, file_name: &str) -> anyhow::Result<Option<SkillDocument>> {
        let valid = file_name.strip_suffix(".md").is_some_and(skill_name_ok);
        anyhow::ensure!(
            valid,
            "Invalid skill filename \"{file_name}\". Use an exact fileName returned by list_skills."
        );
        match self.active_files().await?.get(file_name) {
            Some(file) => Ok(Some(self.read_ref(file).await?)),
            None => Ok(None),
        }
    }
}

// ── 2. tools ─────────────────────────────────────────────────────────────────

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ReadSkillParams {
    /// Exact skill fileName returned by list_skills, for example telegram.md.
    file_name: String,
    /// Optional content_hash from find_skills; a mismatch requires searching again.
    expected_hash: Option<String>,
}

#[tool_router(router = skills_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "list_skills",
        annotations(read_only_hint = true),
        title = "List available skills",
        description = "Return the catalog of all available skills. Each entry includes the \
            exact fileName, human-readable title, short description, declared \
            tool access, active source layer, size, modification time, and \
            `patched` — whether an improver patch is currently appended to that \
            skill's instructions. Use \
            this first to choose a skill, then pass its exact fileName to read_skill."
    )]
    async fn list_skills(&self) -> ToolResult {
        respond(
            async {
                let skills = self.deps.skills.list_skills().await?;
                Ok(json!({ "count": skills.len(), "skills": skills }))
            }
            .await,
        )
    }

    #[tool(
        name = "read_skill",
        annotations(read_only_hint = true),
        title = "Read complete skill instructions",
        description = "Return the complete UTF-8 Markdown contents of one active skill file \
            selected from list_skills, including its frontmatter. Pass the exact \
            fileName from the catalog, including the .md extension. Three views \
            come back: `content` is the editable source file; `patch` is the \
            improver's append-only overlay (null when there is none); and \
            `effectiveInstructions` is what the agent actually runs — the body \
            with the frontmatter stripped and the patch appended. When `patch` \
            is non-null, judge the skill's behaviour by effectiveInstructions, \
            not by content alone."
    )]
    async fn read_skill(&self, Parameters(p): Parameters<ReadSkillParams>) -> ToolResult {
        respond(
            async {
                Ok(match self.deps.skills.read_skill(&p.file_name).await? {
                    None => json!({ "found": false, "fileName": p.file_name, "content": null }),
                    Some(skill) => {
                        let content_hash = skill.content_hash()?;
                        anyhow::ensure!(
                            p.expected_hash.as_ref().is_none_or(|h| h == &content_hash),
                            "Skill changed since find_skills; search again"
                        );
                        json!({
                            "found": true,
                            "fileName": p.file_name,
                            "content": skill.content,
                            "patch": skill.patch,
                            "effectiveInstructions": skill.effective_instructions,
                            "metadata": skill.summary,
                            "content_hash":content_hash,
                        })
                    }
                })
            }
            .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dirs {
        _root: tempdir::Root,
        live: PathBuf,
        defaults: PathBuf,
    }

    mod tempdir {
        // A throwaway directory removed on drop; std has no tempdir.
        pub struct Root(pub std::path::PathBuf);
        impl Drop for Root {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }

    fn dirs() -> Dirs {
        let root = std::env::temp_dir().join(format!("mcp-skills-{}", rand::random::<u64>()));
        let (live, defaults) = (root.join("skills"), root.join("skills.default"));
        std::fs::create_dir_all(&live).unwrap();
        std::fs::create_dir_all(&defaults).unwrap();
        Dirs { _root: tempdir::Root(root), live, defaults }
    }

    #[tokio::test]
    async fn lists_the_live_overlay_over_defaults_with_metadata() {
        let d = dirs();
        std::fs::write(
            d.defaults.join("alpha.md"),
            "---\ntools: [search_news]\n---\n\n# Default Alpha\n\nDefault instructions.\n",
        )
        .unwrap();
        std::fs::write(
            d.defaults.join("beta.md"),
            "---\ntools: *\n---\n\n# Beta title\n\nBeta summary for selection.\n",
        )
        .unwrap();
        std::fs::write(
            d.live.join("alpha.md"),
            "---\ntools: []\n---\n\n# Live Alpha\n\nActive overlay instructions.\n",
        )
        .unwrap();
        std::fs::write(d.live.join("alpha.patch.md"), "not a standalone skill\n").unwrap();

        let skills = SkillCatalog::new(d.live.clone(), d.defaults.clone()).list_skills().await.unwrap();
        assert_eq!(skills.iter().map(|s| s.file_name.as_str()).collect::<Vec<_>>(), ["alpha.md", "beta.md"]);
        let alpha = &skills[0];
        assert_eq!(
            (alpha.title.as_str(), alpha.description.as_str(), &alpha.tools, alpha.source, alpha.patched),
            ("Live Alpha", "Active overlay instructions.", &Some(Tools::List(vec![])), SkillSource::Live, true)
        );
        assert_eq!(
            (skills[1].tools.clone(), skills[1].source, skills[1].patched),
            (Some(Tools::All("*")), SkillSource::Default, false)
        );
        assert_eq!(serde_json::to_value(&skills[1].tools).unwrap(), json!("*"));
    }

    #[tokio::test]
    async fn composes_the_patch_exactly_as_the_agent_does() {
        let d = dirs();
        let raw = "---\ntools: []\n---\n\n# Patched\n\nBase instructions.\n";
        std::fs::write(d.defaults.join("patched.md"), raw).unwrap();
        std::fs::write(d.live.join("patched.patch.md"), "Lesson learned.\n").unwrap();
        let skill =
            SkillCatalog::new(d.live.clone(), d.defaults.clone()).read_skill("patched.md").await.unwrap().unwrap();
        assert_eq!(skill.content, raw);
        assert_eq!(skill.patch.as_deref(), Some("Lesson learned.\n"));
        // Must stay in step with the agent's appendPatch.
        assert_eq!(
            skill.effective_instructions,
            "# Patched\n\nBase instructions.\n\n<!-- improver-patch -->\nLesson learned.\n"
        );
    }

    #[tokio::test]
    async fn ignores_patches_in_defaults_and_rejects_paths() {
        let d = dirs();
        std::fs::write(d.defaults.join("plain.md"), "---\ntools: []\n---\n\n# Plain\n\nBase only.\n").unwrap();
        std::fs::write(d.defaults.join("plain.patch.md"), "must be ignored\n").unwrap();
        let catalog = SkillCatalog::new(d.live.clone(), d.defaults.clone());
        let skill = catalog.read_skill("plain.md").await.unwrap().unwrap();
        assert_eq!(
            (skill.patch, skill.summary.patched, skill.effective_instructions.as_str()),
            (None, false, "# Plain\n\nBase only.\n")
        );
        assert!(catalog.read_skill("missing.md").await.unwrap().is_none());
        for bad in ["plain", "../plain.md"] {
            assert!(catalog.read_skill(bad).await.unwrap_err().to_string().contains("exact fileName"));
        }
    }

    #[test]
    fn descriptions_skip_lists_and_code_and_truncate() {
        let raw = "# T\n\n- a list\n\n```\ncode\n```\n\nThe `real` [summary](http://x) *here*.\n";
        assert_eq!(extract_description(raw, "T"), "The real summary here.");
        assert_eq!(extract_description(&format!("# T\n\n{}", "w ".repeat(200)), "T").chars().count(), 280);
        assert_eq!(fallback_title("news-digest_v2"), "News Digest V2");
    }
}
