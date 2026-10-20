// Text → vector, and the vector math every retrieval path shares.
//
// Sections:
//   1. embedder  — the `Embedder` port + the OpenAI implementation
//   2. retrieval — cosine distance and near-duplicate filtering
//   3. backfill  — draining the NULL-vector backlog a failed inline embed left
//
// Generic infrastructure: nothing here knows a table. Domains (news,
// knowledge, memory) compose the text they embed and store the vectors.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

// ── 1. embedder ──────────────────────────────────────────────────────────────

#[async_trait]
pub trait Embedder: Send + Sync {
    // One vector per input, in order.
    async fn embed_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>>;
}

pub type SharedEmbedder = Arc<dyn Embedder>;

pub async fn embed_one(embedder: &dyn Embedder, text: &str) -> anyhow::Result<Vec<f32>> {
    embedder
        .embed_batch(&[text.to_owned()])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("OpenAI embeddings returned no vector for input"))
}

// 8000 chars sits under the ~8191-token limit of text-embedding-3-small at
// ~4 chars/token. Only the head of a long text is embedded; multi-chunk
// documents are chunked by their own domain (memory) before they get here.
pub const DEFAULT_MAX_CHARS: usize = 8000;
// OpenAI accepts 2048 inputs per request; 100 keeps the payload modest.
const BATCH_SIZE: usize = 100;
// The openai SDK's default: two retries on 429 / 5xx / connection errors.
const MAX_RETRIES: u32 = 2;

pub struct OpenAiEmbedder {
    http: reqwest::Client,
    api_key: String,
    model: String,
    dimensions: u32,
    max_chars: usize,
}

impl OpenAiEmbedder {
    pub fn new(http: reqwest::Client, api_key: String) -> Self {
        Self::with_model(http, api_key, "text-embedding-3-small", 1536, DEFAULT_MAX_CHARS)
    }

    pub fn with_model(http: reqwest::Client, api_key: String, model: &str, dimensions: u32, max_chars: usize) -> Self {
        Self { http, api_key, model: model.to_owned(), dimensions, max_chars }
    }

    pub fn from_env(http: reqwest::Client) -> anyhow::Result<Self> {
        let key = std::env::var("OPENAI_API_KEY").ok().filter(|k| !k.is_empty());
        let key =
            key.ok_or_else(|| anyhow::anyhow!("OPENAI_API_KEY is not set. Embeddings need it; see .env.mcp.example."))?;
        Ok(Self::new(http, key))
    }

    async fn request(&self, input: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        #[derive(Deserialize)]
        struct Item {
            index: usize,
            embedding: Vec<f32>,
        }
        #[derive(Deserialize)]
        struct Response {
            data: Vec<Item>,
        }

        let body = serde_json::json!({ "model": self.model, "input": input, "dimensions": self.dimensions });
        let mut attempt = 0;
        loop {
            let sent = self
                .http
                .post("https://api.openai.com/v1/embeddings")
                .bearer_auth(&self.api_key)
                .timeout(Duration::from_secs(60))
                .json(&body)
                .send()
                .await;
            let retryable = match &sent {
                Ok(res) => res.status().as_u16() == 429 || res.status().is_server_error(),
                Err(err) => err.is_connect() || err.is_timeout() || err.is_request(),
            };
            if retryable && attempt < MAX_RETRIES {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(500 * 2u64.pow(attempt))).await;
                continue;
            }
            let res = sent?;
            if !res.status().is_success() {
                let status = res.status();
                let detail = res.text().await.unwrap_or_default();
                anyhow::bail!("OpenAI embeddings failed ({status}): {detail}");
            }
            let mut parsed: Response = res.json().await?;
            parsed.data.sort_by_key(|item| item.index);
            return Ok(parsed.data.into_iter().map(|item| item.embedding).collect());
        }
    }
}

pub fn truncate_chars(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((at, _)) => text[..at].to_owned(),
        None => text.to_owned(),
    }
}

#[async_trait]
impl Embedder for OpenAiEmbedder {
    async fn embed_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let heads: Vec<String> = texts.iter().map(|t| truncate_chars(t, self.max_chars)).collect();
        let batches = heads.chunks(BATCH_SIZE).map(|batch| self.request(batch));
        let results = futures::future::try_join_all(batches).await?;
        Ok(results.into_iter().flatten().collect())
    }
}

// ── 2. retrieval ─────────────────────────────────────────────────────────────

// text-embedding-3 and pgvector vectors are unit-normalised, so cosine
// distance is 1 - dot. No re-normalisation.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    1.0 - a.iter().zip(b).map(|(x, y)| f64::from(*x) * f64::from(*y)).sum::<f64>()
}

// Catches exact and near-exact copies (a headline reposted to several
// feeds, channel-noise repeated by one source) without merging distinct
// posts on one topic. Tuned on the RAG eval golden set
// (eval configs/baseline-dedup-003.json).
pub const DEFAULT_DEDUP_THRESHOLD: f64 = 0.03;

// Walks items in input order (callers sort by ascending distance first) and
// keeps one only if it is at least `threshold` away from everything kept.
// Items without a vector are dropped, or kept in place with `keep_null`.
pub fn dedup_by_pairwise_cosine<T>(
    items: Vec<T>,
    vector: impl Fn(&T) -> Option<&[f32]>,
    threshold: f64,
    keep_null: bool,
) -> Vec<T> {
    if threshold <= 0.0 {
        return items;
    }
    let mut kept_vectors: Vec<Vec<f32>> = Vec::new();
    let mut kept = Vec::with_capacity(items.len());
    for item in items {
        let Some(candidate) = vector(&item) else {
            if keep_null {
                kept.push(item);
            }
            continue;
        };
        if kept_vectors.iter().all(|k| cosine_distance(candidate, k) >= threshold) {
            kept_vectors.push(candidate.to_vec());
            kept.push(item);
        }
    }
    kept
}

// ── 3. backfill ──────────────────────────────────────────────────────────────

// Draining the NULL-vector backlog a store keeps after a failed inline embed
// (`embed-backfill`). Store-agnostic: each store selects its rows in id
// order past a cursor, embeds them with `embed_isolating` and writes back.

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct BackfillBatch {
    pub embedded: usize,
    pub failed: usize,
    // The last id this batch looked at; None when nothing is left past the
    // cursor.
    pub last_id: Option<i64>,
    // The embedder itself is down: a batch failed and so did a known-good
    // probe. Nothing later in the backlog would fare better.
    pub unavailable: bool,
}

// A text any embedder accepts, to tell "the embedder is down" from "it
// rejects one of these inputs".
const PROBE: &str = "probe";

// One request for the batch. When it fails: if a probe fails too, the
// embedder is down (None); otherwise some input is rejected — one bad input
// fails a whole request — so each text is tried on its own and only the
// rejected ones come back None.
pub async fn embed_isolating(embedder: &dyn Embedder, texts: &[String]) -> Option<Vec<Option<Vec<f32>>>> {
    if texts.is_empty() {
        return Some(Vec::new());
    }
    match embedder.embed_batch(texts).await {
        Ok(vectors) => {
            let mut vectors = vectors.into_iter();
            Some(texts.iter().map(|_| vectors.next()).collect())
        }
        Err(_) => {
            embed_one(embedder, PROBE).await.ok()?;
            let mut out = Vec::with_capacity(texts.len());
            for text in texts {
                out.push(embed_one(embedder, text).await.ok());
            }
            Some(out)
        }
    }
}

// One batch of a store's backlog: `rows` are (id, text) in id order; `write`
// stores one vector. Shared by the stores' `embed_missing_batch`, which only
// select and write.
pub async fn backfill_rows<W, F>(
    embedder: &dyn Embedder,
    rows: &[(i64, String)],
    mut write: W,
) -> anyhow::Result<BackfillBatch>
where
    W: FnMut(i64, Vec<f32>) -> F,
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    let last_id = rows.iter().map(|(id, _)| *id).max();
    if rows.is_empty() {
        return Ok(BackfillBatch::default());
    }
    let texts: Vec<String> = rows.iter().map(|(_, text)| text.clone()).collect();
    let Some(vectors) = embed_isolating(embedder, &texts).await else {
        return Ok(BackfillBatch { embedded: 0, failed: rows.len(), last_id, unavailable: true });
    };
    let mut embedded = 0;
    for ((id, _), vector) in rows.iter().zip(vectors) {
        if let Some(vector) = vector {
            write(*id, vector).await?;
            embedded += 1;
        }
    }
    Ok(BackfillBatch { embedded, failed: rows.len() - embedded, last_id, unavailable: false })
}

// One pass over the backlog in id order: every row is tried once, so a row
// the embedder rejects for good can't hold up the rows behind it. Stops early
// only when the embedder is down. Returns (embedded, failed).
pub async fn drain_missing<N, F>(
    label: &str,
    batch_size: i64,
    log: &dyn Fn(&str),
    mut next: N,
) -> anyhow::Result<(usize, usize)>
where
    N: FnMut(i64, i64) -> F,
    F: std::future::Future<Output = anyhow::Result<BackfillBatch>>,
{
    let (mut embedded, mut failed, mut after_id) = (0, 0, 0);
    loop {
        let batch = next(batch_size, after_id).await?;
        let Some(last_id) = batch.last_id else { break };
        embedded += batch.embedded;
        failed += batch.failed;
        log(&format!(
            "[embed-backfill:{label}] batch: embedded={}, failed={} (running totals: {embedded}/{failed})",
            batch.embedded, batch.failed
        ));
        if batch.unavailable {
            log(&format!("[embed-backfill:{label}] the embedder is unavailable; giving up"));
            break;
        }
        after_id = last_id;
    }
    log(&format!("[embed-backfill:{label}] done: embedded={embedded}, failed={failed}"));
    Ok((embedded, failed))
}

#[cfg(test)]
pub mod testing {
    use super::*;

    // Deterministic stand-in for text-embedding-3-small: a bag-of-words
    // vector, so distance tracks word overlap. Normalised like the real one.
    pub struct FakeEmbedder {
        pub down: std::sync::atomic::AtomicBool,
        dims: usize,
    }

    impl FakeEmbedder {
        pub fn new() -> Arc<Self> {
            Self::with_dims(64)
        }

        // 1536 to fit the real vector(1536) columns in Postgres tests.
        pub fn with_dims(dims: usize) -> Arc<Self> {
            Arc::new(Self { down: std::sync::atomic::AtomicBool::new(false), dims })
        }

        pub fn set_down(&self, down: bool) {
            self.down.store(down, std::sync::atomic::Ordering::SeqCst);
        }
    }

    pub fn fake_embed(text: &str) -> Vec<f32> {
        fake_embed_dims(text, 64)
    }

    pub fn fake_embed_dims(text: &str, dims: usize) -> Vec<f32> {
        let mut v = vec![0f32; dims];
        let lower = text.to_lowercase();
        for token in lower.split(|c: char| !c.is_alphanumeric()).filter(|t| !t.is_empty()) {
            let mut hash: u64 = 0;
            for ch in token.chars() {
                hash = (hash * 31 + u64::from(ch)) % dims as u64;
            }
            v[hash as usize] += 1.0;
        }
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            v.iter_mut().for_each(|x| *x /= norm);
        }
        v
    }

    #[async_trait]
    impl Embedder for FakeEmbedder {
        async fn embed_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
            if self.down.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("provider unreachable");
            }
            Ok(texts.iter().map(|t| fake_embed_dims(t, self.dims)).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(items: &[(u32, Option<Vec<f32>>)]) -> Vec<u32> {
        items.iter().map(|(id, _)| *id).collect()
    }

    fn dedup(items: Vec<(u32, Option<Vec<f32>>)>, keep_null: bool) -> Vec<(u32, Option<Vec<f32>>)> {
        dedup_by_pairwise_cosine(items, |(_, v)| v.as_deref(), 0.05, keep_null)
    }

    #[test]
    fn drops_exact_and_near_duplicates_keeping_input_order() {
        let theta = std::f32::consts::PI / 12.0;
        let out = dedup(
            vec![
                (1, Some(vec![1.0, 0.0, 0.0])),
                (2, Some(vec![theta.cos(), theta.sin(), 0.0])),
                (3, Some(vec![(2.0 * theta).cos(), (2.0 * theta).sin(), 0.0])),
                (4, Some(vec![1.0, 0.0, 0.0])),
            ],
            false,
        );
        // 15° apart collapses, 30° apart survives — a transitive chain keeps
        // its endpoints.
        assert_eq!(ids(&out), [1, 3]);
    }

    #[test]
    fn null_vectors_are_dropped_or_kept_in_place() {
        let items = || vec![(1, Some(vec![1.0, 0.0])), (2, None), (3, Some(vec![1.0, 0.0])), (4, Some(vec![0.0, 1.0]))];
        assert_eq!(ids(&dedup(items(), false)), [1, 4]);
        assert_eq!(ids(&dedup(items(), true)), [1, 2, 4]);
    }

    #[test]
    fn threshold_zero_is_a_passthrough() {
        let items = vec![(1, Some(vec![1.0, 0.0])), (2, Some(vec![1.0, 0.0]))];
        assert_eq!(ids(&dedup_by_pairwise_cosine(items, |(_, v)| v.as_deref(), 0.0, false)), [1, 2]);
    }

    #[test]
    fn truncates_on_characters_not_bytes() {
        assert_eq!(truncate_chars("héllo wörld", 3), "hél");
        assert_eq!(truncate_chars("hi", 10), "hi");
    }
}
