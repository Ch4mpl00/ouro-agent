// Personal knowledge base (`knowledge_base_notes`): freeform notes the user
// asked the agent to remember, recalled by meaning. Superseded by unified
// memory facts (memory.rs imports these) but still served while skills
// reference add_note / find_notes.
//
// Sections:
//   1. repository — add (store + inline embed), find (vector search), backfill
//   2. tools      — `knowledge` toolset: add_note, find_notes

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::embeddings::{BackfillBatch, SharedEmbedder, backfill_rows, embed_one};
use crate::news::EmbedResult;
use crate::pg::{PgPool, vector_literal};
use crate::server::{McpTools, ToolResult, invalid_params, respond};
use crate::time::iso;

// ── 1. repository ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct KnowledgeRepository {
    pool: PgPool,
    embedder: SharedEmbedder,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteHit {
    id: i64,
    body: String,
    tags: Vec<String>,
    source: Option<String>,
    created_at: String,
    updated_at: String,
    distance: f64,
}

// Trim, drop empties, de-duplicate — keeping the model's own wording.
pub fn normalize_tags(tags: Option<&[String]>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for tag in tags.unwrap_or_default().iter().map(|t| t.trim()) {
        if !tag.is_empty() && !out.iter().any(|t| t == tag) {
            out.push(tag.to_owned());
        }
    }
    out
}

impl KnowledgeRepository {
    pub fn new(pool: PgPool, embedder: SharedEmbedder) -> Self {
        Self { pool, embedder }
    }

    // Only the body is embedded; tags are filter metadata. A failed embed
    // leaves the vector NULL for the backfill.
    async fn embed_rows(&self, rows: &[(i64, String)]) -> anyhow::Result<EmbedResult> {
        if rows.is_empty() {
            return Ok(EmbedResult::default());
        }
        let texts: Vec<String> = rows.iter().map(|(_, body)| body.trim().to_owned()).collect();
        let vectors = match self.embedder.embed_batch(&texts).await {
            Ok(v) => v,
            Err(err) => {
                tracing::error!(notes = rows.len(), error = %format!("{err:#}"), "knowledge embed failed");
                return Ok(EmbedResult { embedded: 0, failed: rows.len() });
            }
        };
        let client = self.pool.get().await?;
        let mut embedded = 0;
        for ((id, _), vector) in rows.iter().zip(vectors) {
            client
                .execute(
                    "UPDATE knowledge_base_notes SET embedding = $1::text::vector, embedded_at = now() WHERE id = $2",
                    &[&vector_literal(&vector), id],
                )
                .await?;
            embedded += 1;
        }
        Ok(EmbedResult { embedded, failed: 0 })
    }

    pub async fn add_note(
        &self,
        body: &str,
        tags: Option<&[String]>,
        source: Option<&str>,
    ) -> anyhow::Result<serde_json::Value> {
        let tags = normalize_tags(tags);
        let row = self
            .pool
            .get()
            .await?
            .query_one(
                "INSERT INTO knowledge_base_notes (body, tags, source) VALUES ($1, $2, $3) RETURNING id, body",
                &[&body, &tags, &source],
            )
            .await?;
        let id: i64 = row.get(0);
        let result = self.embed_rows(&[(id, row.get(1))]).await?;
        Ok(json!({ "id": id, "embedded": result.embedded > 0, "tags": tags }))
    }

    pub async fn find_notes(&self, query: &str, k: i64, tags: Option<&[String]>) -> anyhow::Result<Vec<NoteHit>> {
        let vector = embed_one(self.embedder.as_ref(), query).await?;
        let tags = normalize_tags(tags);
        let tag_filter = if tags.is_empty() { "" } else { "AND tags && $3" };
        let sql = format!(
            "SELECT id, body, tags, source, created_at, updated_at, (embedding <=> $1::text::vector) AS distance
               FROM knowledge_base_notes WHERE embedding IS NOT NULL {tag_filter}
              ORDER BY distance LIMIT $2"
        );
        let client = self.pool.get().await?;
        let literal = vector_literal(&vector);
        let rows = if tags.is_empty() {
            client.query(&sql, &[&literal, &k]).await?
        } else {
            client.query(&sql, &[&literal, &k, &tags]).await?
        };
        Ok(rows
            .iter()
            .map(|r| NoteHit {
                id: r.get("id"),
                body: r.get("body"),
                tags: r.get("tags"),
                source: r.get("source"),
                created_at: iso(r.get("created_at")),
                updated_at: iso(r.get("updated_at")),
                distance: r.get("distance"),
            })
            .collect())
    }

    // Rows a failed inline embed left NULL, past `after_id` in id order —
    // one batch of embeddings::drain_missing.
    pub async fn embed_missing_batch(&self, batch: i64, after_id: i64) -> anyhow::Result<BackfillBatch> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT id, body FROM knowledge_base_notes WHERE embedding IS NULL AND id > $2 ORDER BY id LIMIT $1",
                &[&batch, &after_id],
            )
            .await?;
        let rows: Vec<(i64, String)> =
            rows.iter().map(|r| (r.get(0), r.get::<_, String>(1).trim().to_owned())).collect();
        let pool = &self.pool;
        backfill_rows(&*self.embedder, &rows, |id, vector| async move {
            pool.get()
                .await?
                .execute(
                    "UPDATE knowledge_base_notes SET embedding = $1::text::vector, embedded_at = now() WHERE id = $2",
                    &[&vector_literal(&vector), &id],
                )
                .await?;
            Ok(())
        })
        .await
    }

    // Every note, for the one-shot import into memory facts.
    pub async fn all_notes(&self) -> anyhow::Result<Vec<(i64, String, Vec<String>, Option<String>)>> {
        let rows = self.pool.get().await?.query("SELECT id, body, tags, source FROM knowledge_base_notes", &[]).await?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))).collect())
    }
}

// ── 2. tools ─────────────────────────────────────────────────────────────────

#[derive(Deserialize, schemars::JsonSchema)]
struct AddNoteParams {
    /// The fact to remember, as a self-contained sentence including its subject. This text is what semantic recall matches against.
    body: String,
    /// 3–6 short lowercase topical tags you generate for this note. Used for the optional overlap filter in find_notes, not for semantic recall.
    tags: Option<Vec<String>>,
    /// Optional provenance, e.g. "telegram".
    source: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct FindNotesParams {
    /// Natural-language description of what to recall.
    query: String,
    /// Max notes to return. Default 10.
    #[schemars(range(min = 1, max = 50))]
    limit: Option<i64>,
    /// Restrict to notes sharing at least one of these tags (array overlap). Lowercase to match how tags are stored.
    tags: Option<Vec<String>>,
}

#[tool_router(router = knowledge_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "add_note",
        title = "Save a note to the personal knowledge base",
        description = "Persist a freeform fact the user asked you to remember (\"remember \
            that …\", \"write down …\", \"note: …\"). The note becomes semantically \
            searchable later via find_notes.\n\n\
            YOU generate the tags: pick 3–6 short, lowercase topical tags of your \
            choosing — the things you'd later search this note by (people, topics, \
            objects), e.g. [\"router\", \"password\", \"wifi\"]. Tags are metadata \
            only: they help filtering and scanning, but recall runs over the note \
            TEXT, so write a self-contained `body` that names its subject \
            explicitly (\"Lyosha pays for the internet on the 1st\", not \"pays \
            on the 1st\"). Returns the new note id."
    )]
    async fn add_note(&self, Parameters(p): Parameters<AddNoteParams>) -> ToolResult {
        if p.body.is_empty() || p.tags.as_ref().is_some_and(|t| t.len() > 12 || t.iter().any(String::is_empty)) {
            return Err(invalid_params("body must be non-empty; at most 12 non-empty tags"));
        }
        let knowledge = &self.deps.knowledge;
        respond(knowledge.add_note(&p.body, p.tags.as_deref(), p.source.as_deref()).await)
    }

    #[tool(
        name = "find_notes",
        annotations(read_only_hint = true),
        title = "Semantic search over the personal knowledge base",
        description = "Recall notes saved with add_note by meaning, not exact wording \
            (\"what do you remember about the router?\", \"when does Lyosha pay for the internet?\", \
            \"remind me of the router password\"). Returns the closest notes by semantic \
            similarity to `query`, each with body, tags, source, created_at and \
            distance (lower = closer). Optionally pass `tags` to additionally \
            restrict to notes sharing at least one tag. This is the ONLY way to \
            read the knowledge base — use it whenever the user asks what you \
            know/remember about something personal."
    )]
    async fn find_notes(&self, Parameters(p): Parameters<FindNotesParams>) -> ToolResult {
        if p.query.is_empty() || p.limit.is_some_and(|l| !(1..=50).contains(&l)) {
            return Err(invalid_params("query must be non-empty; limit 1–50"));
        }
        let knowledge = &self.deps.knowledge;
        respond(
            async {
                let notes = knowledge.find_notes(&p.query, p.limit.unwrap_or(10), p.tags.as_deref()).await?;
                Ok(json!({ "count": notes.len(), "notes": notes }))
            }
            .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pg_adds_and_finds_notes_with_tag_filter() {
        let Some(pool) = crate::pg::test_pool().await else { return };
        let repo = KnowledgeRepository::new(pool, crate::embeddings::testing::FakeEmbedder::with_dims(1536));
        let tag = format!("t{}", rand::random::<u32>());
        let added = repo
            .add_note(
                "the router password is on the sticker underneath",
                Some(&[tag.clone(), tag.clone()]),
                Some("telegram"),
            )
            .await
            .unwrap();
        assert_eq!(added["embedded"], true);
        assert_eq!(added["tags"], json!([tag]));
        let hits = repo.find_notes("router password", 5, Some(std::slice::from_ref(&tag))).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].body, "the router password is on the sticker underneath");
        assert!(repo.find_notes("router password", 5, Some(&["no-such-tag-xyz".into()])).await.unwrap().is_empty());
    }

    #[test]
    fn normalizes_tags_without_imposing_a_scheme() {
        let raw = vec![" router ".to_owned(), "".into(), "WiFi".into(), "router".into()];
        assert_eq!(normalize_tags(Some(&raw)), ["router", "WiFi"]);
        assert!(normalize_tags(None).is_empty());
    }
}
