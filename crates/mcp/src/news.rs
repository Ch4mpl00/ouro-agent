// The news / RAG store: HN, Habr and harvested Telegram channel posts in one
// `news_items` table, embedded on the way in, searched by meaning.
//
// Sections:
//   1. items      — the unified item and the shapes the tools return
//   2. article    — URL → readable plain text (Readability)
//   3. providers  — Hacker News, Habr, Telegram channels
//   4. ranking    — merging per-query pools: min distance, then dedup
//   5. repository — save / upsert / list / vector search over Postgres
//   6. poller     — one cadence loop over every provider
//   7. tools      — `news-read` toolset: search_news, list_news, fetch_article
//   8. tests      — ranking, extraction, tool responses and Postgres

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio_postgres::Row;
use tokio_util::sync::CancellationToken;

use crate::embeddings::{
    BackfillBatch, DEFAULT_DEDUP_THRESHOLD, SharedEmbedder, backfill_rows, dedup_by_pairwise_cosine,
};
use crate::fetch::{SharedFetch, read_capped};
use crate::pg::{PgPool, Query, parse_vector, storable_json, storable_text, vector_literal, where_clause};
use crate::server::{McpTools, ToolResult, invalid_params, respond};
use crate::time::{iso, parse_js_date, require_js_date};
use crate::userbot::SharedAccount;

// ── 1. items ─────────────────────────────────────────────────────────────────

// `external_id` is the natural key in the source: the URL for HN/Habr,
// "<chat_id>:<tg_message_id>" for channel posts.
#[derive(Debug, Clone, PartialEq)]
pub struct NewsItem {
    pub source: String,
    pub external_id: String,
    pub title: Option<String>,
    pub url: Option<String>,
    pub body: String,
    pub metadata: Map<String, Value>,
    pub posted_at: Option<DateTime<Utc>>,
}

impl NewsItem {
    fn to_json(&self) -> Value {
        json!({
            "source": self.source,
            "externalId": self.external_id,
            "title": self.title,
            "url": self.url,
            "body": self.body,
            "metadata": self.metadata,
            "postedAt": self.posted_at.map(iso),
        })
    }
}

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct SaveResult {
    pub saved: usize,
    pub embedded: usize,
    pub failed: usize,
}

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct EmbedResult {
    pub embedded: usize,
    pub failed: usize,
}

#[derive(Debug, Default, Clone)]
pub struct Filter {
    pub source: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    // Point-in-time replay for evals: what was searchable (embedded_at) or
    // stored (fetched_at) at this instant. Not the same as `until`, which
    // bounds posted_at.
    pub as_of: Option<DateTime<Utc>>,
    // Channel posts only: metadata.chat_username OR chat_id.
    pub channel: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchOrdering {
    Approximate,
    Exact,
}

// The HNSW walk filters the candidates it finds, so it only works for a
// filter that cuts across topics. Time does: the newest week holds every
// subject. A source or a channel does not — it is a topic and a language
// (Habr is Russian tech, channels are Russian news), so for a query far from
// that sub-population the walk runs out of budget before it reaches it.
// Measured on prod (117k rows): habr/hackernews top-10 recall fell to
// 0.72–0.9 with short pools at any ef_search. Those filters keep the exact
// ordering, which the source index narrows; see
// .claude/tasks/done/news-search-ann.md.
pub fn search_ordering(filter: &Filter) -> SearchOrdering {
    if filter.source.is_some() || filter.channel.is_some() {
        SearchOrdering::Exact
    } else {
        SearchOrdering::Approximate
    }
}

// hnsw.ef_search is the candidate list the graph walk keeps: 4× the pool,
// floor 200, capped at 600 — past that the planner prices the index above a
// sequential scan. Measured on prod: see .claude/tasks/done/news-search-ann.md.
pub fn ann_ef_search(pool_size: usize) -> usize {
    (pool_size * 4).clamp(200, 600)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub id: i64,
    pub source: String,
    pub title: Option<String>,
    pub url: Option<String>,
    pub snippet: String,
    pub posted_at: Option<String>,
    pub distance: f64,
    pub metadata: Map<String, Value>,
    // Batch path only: indices of the queries that surfaced this item.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_queries: Option<Vec<usize>>,
}

#[derive(Debug, Clone)]
pub struct PoolRow {
    pub id: i64,
    pub source: String,
    pub title: Option<String>,
    pub url: Option<String>,
    pub body: String,
    pub metadata: Map<String, Value>,
    pub posted_at: Option<DateTime<Utc>>,
    pub distance: f64,
    pub embedding: Option<Vec<f32>>,
}

// ── 2. article ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct Article {
    pub url: String,
    pub title: String,
    pub text: String,
    pub site: Option<String>,
    // Unparseable dates are dropped rather than poisoning the row.
    pub published_at: Option<DateTime<Utc>>,
    pub author: Option<String>,
}

// No article page is bigger; a link to a huge file must not be pulled into
// memory (the HN poller follows whatever the front page links to).
const MAX_ARTICLE_BYTES: usize = 8_000_000;

// What can hold an article. No content type at all is given the benefit of
// the doubt; anything else (PDF, image, archive, video) is refused unread.
fn is_article_type(content_type: &str) -> bool {
    regex::Regex::new(r"(?i)^(text/|application/(xhtml\+xml|xml)\b)").expect("valid regex").is_match(content_type)
}

// Article URLs come from the model and from feeds, so pages are fetched
// through the SSRF guard.
#[derive(Clone)]
pub struct ArticleFetcher {
    fetch: SharedFetch,
}

impl ArticleFetcher {
    pub fn new(fetch: SharedFetch) -> Self {
        Self { fetch }
    }

    pub async fn fetch(&self, url: &str) -> anyhow::Result<Article> {
        let res = self.fetch.get(url, Duration::from_secs(30)).await?;
        if !(200..300).contains(&res.status) {
            anyhow::bail!("fetching {url} failed: HTTP {}", res.status);
        }
        if let Some(content_type) = res.content_type.as_deref()
            && !is_article_type(content_type)
        {
            anyhow::bail!("{url} is not an article page ({content_type})");
        }
        let (html, _, _) = read_capped(res.body, MAX_ARTICLE_BYTES).await?;
        let url = url.to_owned();
        tokio::task::spawn_blocking(move || extract_article(&url, &html)).await?
    }

    // Three attempts with a short backoff; None on terminal failure so a
    // poller simply drops the item.
    pub async fn fetch_with_retry(&self, url: &str) -> Option<Article> {
        let mut last = None;
        for attempt in 0..3u64 {
            match self.fetch(url).await {
                Ok(article) => return Some(article),
                Err(err) => last = Some(err),
            }
            if attempt < 2 {
                tokio::time::sleep(Duration::from_millis(500 * (attempt + 1))).await;
            }
        }
        if let Some(err) = last {
            tracing::warn!(url, error = %format!("{err:#}"), "article fetch failed after 3 attempts");
        }
        None
    }
}

pub fn extract_article(url: &str, html: &str) -> anyhow::Result<Article> {
    let mut readability = dom_smoothie::Readability::new(html, Some(url), None)?;
    let article = readability.parse().map_err(|err| anyhow::anyhow!("No article extracted from {url}: {err}"))?;
    let host = url::Url::parse(url).ok().and_then(|u| u.host_str().map(|h| h.trim_start_matches("www.").to_owned()));
    Ok(Article {
        url: url.to_owned(),
        title: article.title,
        text: strip_html(&article.content),
        site: article.site_name.filter(|s| !s.is_empty()).or(host),
        published_at: article.published_time.as_deref().and_then(parse_js_date),
        author: article.byline.filter(|s| !s.is_empty()),
    })
}

// A compact text blob for the LLM: whatever markup the extractor left, gone.
pub fn strip_html(html: &str) -> String {
    let re = |p: &str| regex::Regex::new(p).expect("valid regex");
    let s = re(r"(?is)<script[\s\S]*?</script>").replace_all(html, " ");
    let s = re(r"(?is)<style[\s\S]*?</style>").replace_all(&s, " ");
    let s = re(r"<[^>]+>").replace_all(&s, " ");
    let s = s
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'");
    re(r"\s+").replace_all(&s, " ").trim().to_owned()
}

// ── 3. providers ─────────────────────────────────────────────────────────────

// A self-contained source. The poller ticks it every `cadence`, stores what
// it returns; quirks (endpoints, watermarks) stay inside the provider.
#[async_trait]
pub trait NewsProvider: Send + Sync {
    fn source(&self) -> &'static str;
    fn cadence(&self) -> Duration;
    async fn fetch(&self) -> anyhow::Result<Vec<NewsItem>>;
}

#[derive(Debug, Clone)]
pub struct Headline {
    pub title: String,
    pub url: String,
    pub author: Option<String>,
    pub posted_at: Option<DateTime<Utc>>,
    // Source-specific extras, written into metadata ahead of author/site.
    pub extra: Map<String, Value>,
}

// The shared article mapping. Keys whose value is absent are omitted, as
// JSON.stringify dropped `undefined`; the headline's author wins. So does its
// time (HN's `time`, the feed's date): a page's own date is often wrong
// (a misread "26/09/26" became the year 2609) and is only a fallback, never
// one in the future, which would pass every "since" filter forever.
pub fn article_item(source: &str, headline: &Headline, article: Option<Article>) -> Option<NewsItem> {
    let article = article.filter(|a| !a.text.trim().is_empty())?;
    let mut metadata = headline.extra.clone();
    if let Some(author) = headline.author.clone().or(article.author.clone()) {
        metadata.insert("author".into(), author.into());
    }
    if let Some(site) = article.site.clone() {
        metadata.insert("site".into(), site.into());
    }
    Some(NewsItem {
        source: source.to_owned(),
        external_id: headline.url.clone(),
        title: Some(if article.title.is_empty() { headline.title.clone() } else { article.title.clone() }),
        url: Some(headline.url.clone()),
        body: article.text,
        metadata,
        posted_at: headline
            .posted_at
            .or(article.published_at.filter(|at| *at <= Utc::now() + chrono::Duration::days(1))),
    })
}

async fn fetch_articles(source: &str, articles: &ArticleFetcher, headlines: Vec<Headline>) -> Vec<NewsItem> {
    let fetched = futures::future::join_all(headlines.iter().map(|h| articles.fetch_with_retry(&h.url))).await;
    headlines.iter().zip(fetched).filter_map(|(h, a)| article_item(source, h, a)).collect()
}

const HEADLINE_LIMIT: usize = 30;

pub struct HackerNews {
    http: reqwest::Client,
    articles: ArticleFetcher,
}

impl HackerNews {
    // `http` reads the feed; article pages go through `fetch`.
    pub fn new(http: reqwest::Client, fetch: SharedFetch) -> Self {
        Self { articles: ArticleFetcher::new(fetch), http }
    }

    async fn headlines(&self, limit: usize) -> anyhow::Result<Vec<Headline>> {
        const API: &str = "https://hacker-news.firebaseio.com/v0";
        #[derive(Deserialize)]
        struct Item {
            title: Option<String>,
            url: Option<String>,
            score: Option<i64>,
            descendants: Option<i64>,
            by: Option<String>,
            time: Option<i64>,
        }
        let res = self.http.get(format!("{API}/topstories.json")).send().await?;
        anyhow::ensure!(res.status().is_success(), "HN topstories failed: {}", res.status().as_u16());
        let ids: Vec<i64> = res.json().await?;
        let items = futures::future::join_all(ids.into_iter().take(limit).map(|id| async move {
            let item: Item = self.http.get(format!("{API}/item/{id}.json")).send().await.ok()?.json().await.ok()?;
            let mut extra = Map::new();
            extra.insert("hn_id".into(), id.into());
            if let Some(score) = item.score {
                extra.insert("score".into(), score.into());
            }
            if let Some(comments) = item.descendants {
                extra.insert("comments".into(), comments.into());
            }
            Some(Headline {
                title: item.title?,
                url: item.url.unwrap_or_else(|| format!("https://news.ycombinator.com/item?id={id}")),
                author: item.by,
                posted_at: item.time.and_then(|t| DateTime::from_timestamp(t, 0)),
                extra,
            })
        }))
        .await;
        Ok(items.into_iter().flatten().collect())
    }
}

#[async_trait]
impl NewsProvider for HackerNews {
    fn source(&self) -> &'static str {
        "hackernews"
    }

    fn cadence(&self) -> Duration {
        Duration::from_secs(15 * 60)
    }

    async fn fetch(&self) -> anyhow::Result<Vec<NewsItem>> {
        Ok(fetch_articles(self.source(), &self.articles, self.headlines(HEADLINE_LIMIT).await?).await)
    }
}

// The overall feed; downstream filters by interest.
pub struct Habr {
    http: reqwest::Client,
    articles: ArticleFetcher,
}

impl Habr {
    // `http` reads the feed; article pages go through `fetch`.
    pub fn new(http: reqwest::Client, fetch: SharedFetch) -> Self {
        Self { articles: ArticleFetcher::new(fetch), http }
    }

    async fn headlines(&self, limit: usize) -> anyhow::Result<Vec<Headline>> {
        let bytes = self
            .http
            .get("https://habr.com/ru/rss/all/")
            .timeout(Duration::from_secs(15))
            .send()
            .await?
            .bytes()
            .await?;
        let feed = feed_rs::parser::parse(bytes.as_ref())?;
        Ok(feed
            .entries
            .into_iter()
            .take(limit)
            .filter_map(|entry| {
                Some(Headline {
                    title: entry.title.map(|t| t.content).filter(|t| !t.is_empty())?,
                    url: entry.links.first().map(|l| l.href.clone()).filter(|u| !u.is_empty())?,
                    author: entry.authors.first().and_then(|a| a.name.clone()).filter(|n| !n.is_empty()),
                    posted_at: entry.published.or(entry.updated),
                    extra: Map::new(),
                })
            })
            .collect())
    }
}

#[async_trait]
impl NewsProvider for Habr {
    fn source(&self) -> &'static str {
        "habr"
    }

    fn cadence(&self) -> Duration {
        Duration::from_secs(30 * 60)
    }

    async fn fetch(&self) -> anyhow::Result<Vec<NewsItem>> {
        Ok(fetch_articles(self.source(), &self.articles, self.headlines(HEADLINE_LIMIT).await?).await)
    }
}

// Every channel dialog the userbot follows. Per-channel watermark = the max
// tg_message_id already stored for that chat. A new channel gets its last
// BOOTSTRAP posts, then DELTA at a time; a short pause between channels
// keeps clear of FLOOD_WAIT. No userbot session → nothing, not an error.
pub struct TelegramChannels {
    account: SharedAccount,
    pool: PgPool,
    inter_channel_delay: Duration,
}

const CHANNEL_SOURCE: &str = "channel";
const BOOTSTRAP_LIMIT: usize = 50;
const DELTA_LIMIT: usize = 200;

impl TelegramChannels {
    pub fn new(account: SharedAccount, pool: PgPool) -> Self {
        Self { account, pool, inter_channel_delay: Duration::from_millis(200) }
    }

    pub fn with_delay(mut self, inter_channel_delay: Duration) -> Self {
        self.inter_channel_delay = inter_channel_delay;
        self
    }
}

// The highest tg_message_id already stored for a channel.
pub async fn channel_watermark(pool: &PgPool, chat_id: &str) -> anyhow::Result<Option<i64>> {
    let row = pool
        .get()
        .await?
        .query_one(
            "SELECT max((metadata ->> 'tg_message_id')::int)::bigint FROM news_items
              WHERE source = $1 AND metadata ->> 'chat_id' = $2",
            &[&CHANNEL_SOURCE, &chat_id],
        )
        .await?;
    Ok(row.get(0))
}

pub fn channel_item(
    chat_id: &str,
    title: Option<&str>,
    username: Option<&str>,
    m: &crate::userbot::ChannelMessage,
) -> NewsItem {
    let mut metadata = Map::new();
    metadata.insert("chat_id".into(), chat_id.into());
    metadata.insert("chat_title".into(), title.into());
    metadata.insert("chat_username".into(), username.into());
    metadata.insert("tg_message_id".into(), m.id.into());
    metadata.insert("views".into(), m.views.into());
    metadata.insert("forwards".into(), m.forwards.into());
    NewsItem {
        source: CHANNEL_SOURCE.into(),
        external_id: format!("{chat_id}:{}", m.id),
        // Always null: the channel name repeated on every post would only
        // pollute the embeddings. It lives in metadata.chat_title.
        title: None,
        url: username.map(|u| format!("https://t.me/{u}/{}", m.id)),
        body: m.text.clone(),
        metadata,
        posted_at: Some(m.date),
    }
}

#[async_trait]
impl NewsProvider for TelegramChannels {
    fn source(&self) -> &'static str {
        CHANNEL_SOURCE
    }

    fn cadence(&self) -> Duration {
        Duration::from_secs(30 * 60)
    }

    async fn fetch(&self) -> anyhow::Result<Vec<NewsItem>> {
        if !self.account.has_session().await {
            tracing::warn!("no saved userbot session — run `userbot-auth`. Skipping channels tick.");
            return Ok(Vec::new());
        }
        let mut collected = Vec::new();
        for channel in crate::userbot::list_channels(&*self.account).await? {
            let watermark = channel_watermark(&self.pool, &channel.chat_id).await?;
            let limit = if watermark.is_none() { BOOTSTRAP_LIMIT } else { DELTA_LIMIT };
            match crate::userbot::fetch_messages(&*self.account, &channel, watermark, limit).await {
                Ok(messages) => {
                    collected.extend(messages.iter().map(|m| {
                        channel_item(&channel.chat_id, channel.title.as_deref(), channel.username.as_deref(), m)
                    }))
                }
                Err(err) => {
                    let name = channel.title.as_deref().unwrap_or(&channel.chat_id);
                    tracing::warn!(channel = name, error = %format!("{err:#}"), "channel fetch failed");
                }
            }
            tokio::time::sleep(self.inter_channel_delay).await;
        }
        Ok(collected)
    }
}

// ── 4. ranking ───────────────────────────────────────────────────────────────

const SNIPPET_CHARS: usize = 400;

fn snippet(body: &str) -> String {
    match body.char_indices().nth(SNIPPET_CHARS) {
        Some((at, _)) => format!("{}…", &body[..at]),
        None => body.to_owned(),
    }
}

struct QueryMatch {
    best: PoolRow,
    matched_queries: Vec<usize>,
}

impl QueryMatch {
    fn into_result(self, annotate_matches: bool) -> SearchResult {
        let row = self.best;
        SearchResult {
            id: row.id,
            snippet: snippet(&row.body),
            source: row.source,
            title: row.title,
            url: row.url,
            posted_at: row.posted_at.map(iso),
            distance: row.distance,
            metadata: row.metadata,
            matched_queries: annotate_matches.then_some(self.matched_queries),
        }
    }
}

// Each pool is one query's rows, ascending by distance. An item several
// queries found is kept once at its best distance; `matched_queries` says
// which facets surfaced it. Pure, so the ranking contract is testable
// without a database.
pub fn merge_ranked_pools(pools: Vec<Vec<PoolRow>>, k: usize, threshold: f64, annotate: bool) -> Vec<SearchResult> {
    let mut first_seen_ids = Vec::new();
    let mut matches_by_id: HashMap<i64, QueryMatch> = HashMap::new();

    for (query_index, rows) in pools.into_iter().enumerate() {
        for row in rows {
            match matches_by_id.get_mut(&row.id) {
                Some(found) => {
                    found.matched_queries.push(query_index);
                    if row.distance < found.best.distance {
                        found.best = row;
                    }
                }
                None => {
                    first_seen_ids.push(row.id);
                    matches_by_id.insert(row.id, QueryMatch { best: row, matched_queries: vec![query_index] });
                }
            }
        }
    }

    // A stable sort preserves discovery order when distances are equal.
    let mut ranked: Vec<QueryMatch> = first_seen_ids.into_iter().filter_map(|id| matches_by_id.remove(&id)).collect();
    ranked.sort_by(|a, b| a.best.distance.total_cmp(&b.best.distance));
    let distinct = dedup_by_pairwise_cosine(ranked, |found| found.best.embedding.as_deref(), threshold, false);
    distinct.into_iter().take(k).map(|found| found.into_result(annotate)).collect()
}

// ── 5. repository ────────────────────────────────────────────────────────────

const ITEM_COLUMNS: &str = "id, source, external_id, title, url, body, metadata, posted_at";

#[derive(Clone)]
pub struct NewsRepository {
    pool: PgPool,
    embedder: SharedEmbedder,
}

struct EmbedRow {
    id: i64,
    title: Option<String>,
    body: String,
}

impl EmbedRow {
    fn from_row(row: &Row) -> Self {
        Self { id: row.get("id"), title: row.get("title"), body: row.get("body") }
    }

    fn text(&self) -> String {
        let title = self.title.as_deref().unwrap_or("").trim();
        let body = self.body.trim();
        if title.is_empty() {
            return body.to_owned();
        }
        if body.is_empty() {
            return title.to_owned();
        }
        format!("{title}\n\n{body}")
    }
}

struct StoredItem {
    item: NewsItem,
    embedding: Option<Vec<f32>>,
}

fn embedding_of(row: &Row) -> Option<Vec<f32>> {
    let vector: Option<String> = row.get("embedding");
    vector.as_deref().and_then(parse_vector)
}

fn metadata_of(row: &Row, column: &str) -> Map<String, Value> {
    match row.get::<_, Option<Value>>(column) {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    }
}

fn item_of(row: &Row) -> NewsItem {
    NewsItem {
        source: row.get("source"),
        external_id: row.get("external_id"),
        title: row.get("title"),
        url: row.get("url"),
        body: row.get("body"),
        metadata: metadata_of(row, "metadata"),
        posted_at: row.get("posted_at"),
    }
}

fn pool_row(row: &Row) -> PoolRow {
    PoolRow {
        id: row.get("id"),
        source: row.get("source"),
        title: row.get("title"),
        url: row.get("url"),
        body: row.get("body"),
        metadata: metadata_of(row, "metadata"),
        posted_at: row.get("posted_at"),
        distance: row.get("distance"),
        embedding: embedding_of(row),
    }
}

// What Postgres can store: text and jsonb refuse a NUL, and one such item
// fails the whole multi-row insert — a poller would lose its batch on every
// tick, a channel stall on the post for good. Scraped and forwarded text does
// carry NULs.
fn storable(item: &NewsItem) -> NewsItem {
    NewsItem {
        source: storable_text(&item.source),
        external_id: storable_text(&item.external_id),
        title: item.title.as_deref().map(storable_text),
        url: item.url.as_deref().map(storable_text),
        body: storable_text(&item.body),
        metadata: item.metadata.iter().map(|(k, v)| (storable_text(k), storable_json(v))).collect(),
        posted_at: item.posted_at,
    }
}

fn bind_item(query: &mut Query, item: &NewsItem) -> String {
    let item = &storable(item);
    let source = query.bind(item.source.clone());
    let external_id = query.bind(item.external_id.clone());
    let title = query.bind(item.title.clone());
    let url = query.bind(item.url.clone());
    let body = query.bind(item.body.clone());
    let metadata = query.bind(Value::Object(item.metadata.clone()));
    let posted_at = query.bind(item.posted_at);
    format!("({source}, {external_id}, {title}, {url}, {body}, {metadata}, {posted_at})")
}

impl NewsRepository {
    pub fn new(pool: PgPool, embedder: SharedEmbedder) -> Self {
        Self { pool, embedder }
    }

    // Embedding failures never fail a write: rows keep a NULL vector and the
    // backfill picks them up.
    async fn embed(&self, rows: &[EmbedRow]) -> anyhow::Result<EmbedResult> {
        if rows.is_empty() {
            return Ok(EmbedResult::default());
        }
        let texts: Vec<String> = rows.iter().map(EmbedRow::text).collect();
        let vectors = match self.embedder.embed_batch(&texts).await {
            Ok(vectors) => vectors,
            Err(err) => {
                tracing::error!(rows = rows.len(), error = %format!("{err:#}"), "news embed failed");
                return Ok(EmbedResult { embedded: 0, failed: rows.len() });
            }
        };
        let client = self.pool.get().await?;
        let mut embedded = 0;
        for (row, vector) in rows.iter().zip(vectors) {
            client
                .execute(
                    "UPDATE news_items SET embedding = $1::text::vector, embedded_at = now() WHERE id = $2",
                    &[&vector_literal(&vector), &row.id],
                )
                .await?;
            embedded += 1;
        }
        Ok(EmbedResult { embedded, failed: 0 })
    }

    pub async fn save(&self, items: &[NewsItem]) -> anyhow::Result<SaveResult> {
        if items.is_empty() {
            return Ok(SaveResult::default());
        }
        let mut query = Query::default();
        let values: Vec<String> = items.iter().map(|item| bind_item(&mut query, item)).collect();
        let sql = format!(
            "INSERT INTO news_items (source, external_id, title, url, body, metadata, posted_at) VALUES {}
             ON CONFLICT (source, external_id) DO NOTHING RETURNING id, title, body",
            values.join(", ")
        );
        let rows = {
            let client = self.pool.get().await?;
            client.query(&sql, &query.params()).await?
        };
        let inserted: Vec<EmbedRow> = rows.iter().map(EmbedRow::from_row).collect();
        if inserted.is_empty() {
            return Ok(SaveResult::default());
        }
        let result = self.embed(&inserted).await?;
        Ok(SaveResult { saved: inserted.len(), embedded: result.embedded, failed: result.failed })
    }

    // Insert-or-replace by (source, external_id); a replaced body is
    // re-embedded, so the old vector is cleared first.
    pub async fn upsert(&self, item: &NewsItem) -> anyhow::Result<SaveResult> {
        let sql = "INSERT INTO news_items (source, external_id, title, url, body, metadata, posted_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (source, external_id) DO UPDATE SET
                     title = EXCLUDED.title, body = EXCLUDED.body, metadata = EXCLUDED.metadata,
                     embedding = NULL, embedded_at = NULL
                   RETURNING id, title, body";
        let item = &storable(item);
        let metadata = Value::Object(item.metadata.clone());
        let row = {
            let client = self.pool.get().await?;
            client
                .query_one(
                    sql,
                    &[&item.source, &item.external_id, &item.title, &item.url, &item.body, &metadata, &item.posted_at],
                )
                .await?
        };
        let result = self.embed(&[EmbedRow::from_row(&row)]).await?;
        Ok(SaveResult { saved: 1, embedded: result.embedded, failed: result.failed })
    }

    pub async fn find_by_external_id(&self, source: &str, external_id: &str) -> anyhow::Result<Option<NewsItem>> {
        let sql = format!("SELECT {ITEM_COLUMNS} FROM news_items WHERE source = $1 AND external_id = $2 LIMIT 1");
        let client = self.pool.get().await?;
        let row = client.query_opt(&sql, &[&source, &external_id]).await?;
        Ok(row.as_ref().map(item_of))
    }

    fn filters(query: &mut Query, filter: &Filter, as_of_column: &str) -> Vec<String> {
        let mut conditions = Vec::new();
        if let Some(source) = &filter.source {
            conditions.push(format!("source = {}", query.bind(source.clone())));
        }
        if let Some(since) = filter.since {
            conditions.push(format!("posted_at > {}", query.bind(since)));
        }
        if let Some(until) = filter.until {
            conditions.push(format!("posted_at <= {}", query.bind(until)));
        }
        if let Some(as_of) = filter.as_of {
            conditions.push(format!("{as_of_column} <= {}", query.bind(as_of)));
        }
        if let Some(channel) = &filter.channel {
            let channel_param = query.bind(channel.clone());
            conditions.push(format!(
                "(metadata ->> 'chat_username' = {channel_param} OR metadata ->> 'chat_id' = {channel_param})"
            ));
        }
        conditions
    }

    // Chronological: ascending when `since` is set, descending otherwise.
    // as_of bounds fetched_at here — list needs no embedding, so "was it in
    // the store" is the visibility question.
    pub async fn list(&self, filter: &Filter, limit: usize, dedup_threshold: f64) -> anyhow::Result<Vec<NewsItem>> {
        // A 2× pool leaves room for near-duplicates to drop out.
        let fetch_limit = if dedup_threshold > 0.0 { (limit * 2).min(2000) } else { limit };
        let mut query = Query::default();
        let conditions = Self::filters(&mut query, filter, "fetched_at");
        let where_sql = where_clause(&conditions);
        let order = if filter.since.is_some() { "ASC" } else { "DESC" };
        let limit_param = query.bind(fetch_limit as i64);
        let sql = format!(
            "SELECT {ITEM_COLUMNS}, embedding::text AS embedding FROM news_items
             {where_sql} ORDER BY posted_at {order} LIMIT {limit_param}"
        );
        let client = self.pool.get().await?;
        let rows = client.query(&sql, &query.params()).await?;
        let stored: Vec<StoredItem> =
            rows.iter().map(|row| StoredItem { item: item_of(row), embedding: embedding_of(row) }).collect();
        let distinct = dedup_by_pairwise_cosine(stored, |row| row.embedding.as_deref(), dedup_threshold, true);
        Ok(distinct.into_iter().take(limit).map(|row| row.item).collect())
    }

    // One batch embed for every query, then one vector search per query
    // (each with its own pool), merged and de-duplicated across the batch.
    // A single query is the N=1 case.
    pub async fn search(
        &self,
        queries: &[String],
        k: usize,
        filter: &Filter,
        annotate: bool,
    ) -> anyhow::Result<Vec<SearchResult>> {
        let pool_size = (k * 2).max(30);
        let vectors = self.embedder.embed_batch(queries).await?;
        let searches = vectors.iter().map(|vector| self.search_vector(vector, pool_size, filter));
        let pools = futures::future::try_join_all(searches).await?;
        Ok(merge_ranked_pools(pools, k, DEFAULT_DEDUP_THRESHOLD, annotate))
    }

    async fn search_vector(&self, vector: &[f32], pool_size: usize, filter: &Filter) -> anyhow::Result<Vec<PoolRow>> {
        let ordering = search_ordering(filter);
        let (sql, query) = Self::pool_query(vector, pool_size, filter, ordering);
        let mut client = self.pool.get().await?;
        let rows = match ordering {
            SearchOrdering::Exact => client.query(&sql, &query.params()).await?,
            SearchOrdering::Approximate => {
                let tx = client.transaction().await?;
                // Transaction-local, so the pool's connection goes back
                // untouched. relaxed_order keeps walking the graph past
                // ef_search when a time filter drops candidates (the merge
                // re-sorts by the exact distance anyway). A sequential scan is
                // never the plan we want here — it reads every embedding — but
                // the planner picks it once ef_search makes the index look
                // dear, so it is priced out; a time filter still gets its
                // posted_at index.
                tx.execute(
                    "SELECT set_config('hnsw.ef_search', $1, true), set_config('hnsw.iterative_scan', 'relaxed_order', true),
                            set_config('enable_seqscan', 'off', true)",
                    &[&ann_ef_search(pool_size).to_string()],
                )
                .await?;
                let rows = tx.query(&sql, &query.params()).await?;
                tx.commit().await?;
                rows
            }
        };
        Ok(rows.iter().map(pool_row).collect())
    }

    // "Approximate" orders by exactly the expression news_items_embedding_hnsw
    // indexes (migration 0007) — anything else is a sequential scan over
    // every embedding. Either way the distance the caller sees is the exact
    // one on the full vector. Public for the test that pins the plan.
    pub fn pool_query(vector: &[f32], pool_size: usize, filter: &Filter, ordering: SearchOrdering) -> (String, Query) {
        let mut query = Query::default();
        let vector_param = query.bind(vector_literal(vector));
        let mut conditions = vec!["embedding IS NOT NULL".to_owned()];
        // A row embedded after as_of wasn't searchable then, whenever it was posted.
        conditions.extend(Self::filters(&mut query, filter, "embedded_at"));
        let where_sql = where_clause(&conditions);
        let limit_param = query.bind(pool_size as i64);
        let order = match ordering {
            SearchOrdering::Exact => "distance".to_owned(),
            SearchOrdering::Approximate => {
                format!("(embedding::halfvec(1536)) <=> ({vector_param}::text::halfvec(1536))")
            }
        };
        let sql = format!(
            "SELECT id, source, title, url, body, metadata, posted_at,
                    (embedding <=> {vector_param}::text::vector) AS distance, embedding::text AS embedding
             FROM news_items {where_sql} ORDER BY {order} LIMIT {limit_param}"
        );
        (sql, query)
    }

    // Rows a failed inline embed left NULL, past `after_id` in id order —
    // one batch of embeddings::drain_missing.
    pub async fn embed_missing_batch(&self, batch: i64, after_id: i64) -> anyhow::Result<BackfillBatch> {
        let sql = "SELECT id, title, body FROM news_items WHERE embedding IS NULL AND id > $2 ORDER BY id LIMIT $1";
        let rows = {
            let client = self.pool.get().await?;
            client.query(sql, &[&batch, &after_id]).await?
        };
        let pending: Vec<(i64, String)> = rows.iter().map(EmbedRow::from_row).map(|r| (r.id, r.text())).collect();
        let pool = &self.pool;
        backfill_rows(&*self.embedder, &pending, |id, vector| async move {
            pool.get()
                .await?
                .execute(
                    "UPDATE news_items SET embedding = $1::text::vector, embedded_at = now() WHERE id = $2",
                    &[&vector_literal(&vector), &id],
                )
                .await?;
            Ok(())
        })
        .await
    }
}

// ── 6. poller ────────────────────────────────────────────────────────────────

const POLL_TICK: Duration = Duration::from_secs(30);
const BOOT_DELAY: Duration = Duration::from_secs(10);

async fn poll_provider(provider: &dyn NewsProvider, repository: &NewsRepository) -> anyhow::Result<()> {
    let items = provider.fetch().await?;
    if items.is_empty() {
        return Ok(());
    }
    let result = repository.save(&items).await?;
    tracing::info!(
        source = provider.source(),
        fetched = items.len(),
        saved = result.saved,
        embedded = result.embedded,
        failed = result.failed,
        "news tick"
    );
    Ok(())
}

// One loop; each tick fires every provider whose cadence has elapsed. One
// provider failing never stops the others. Starts after a boot delay so
// transport startup finishes first.
pub async fn run_poller(providers: Vec<Box<dyn NewsProvider>>, repository: NewsRepository, cancel: CancellationToken) {
    let names: Vec<String> =
        providers.iter().map(|p| format!("{}({}min)", p.source(), p.cadence().as_secs() / 60)).collect();
    tracing::info!(providers = names.join(", "), "news poller starting");
    tokio::select! {
        _ = cancel.cancelled() => return,
        _ = tokio::time::sleep(BOOT_DELAY) => {}
    }
    let mut last_polled: HashMap<&'static str, tokio::time::Instant> = HashMap::new();
    let mut interval = tokio::time::interval(POLL_TICK);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = interval.tick() => {}
        }
        for provider in &providers {
            let now = tokio::time::Instant::now();
            if last_polled.get(provider.source()).is_some_and(|at| now.duration_since(*at) < provider.cadence()) {
                continue;
            }
            last_polled.insert(provider.source(), now);
            if let Err(err) = poll_provider(provider.as_ref(), &repository).await {
                tracing::error!(source = provider.source(), error = %format!("{err:#}"), "news tick failed");
            }
        }
    }
}

// ── 7. tools ─────────────────────────────────────────────────────────────────

// Exactly n contiguous parts, sizes differing by at most one (trailing ones
// may be empty) — a workflow references ${bind.chunks.0} … statically, and
// neighbouring items (time- or relevance-ordered) are the ones most likely to
// describe the same event.
pub fn split_chunks<T: Clone>(items: &[T], n: usize) -> Vec<Vec<T>> {
    let (base, extra) = (items.len() / n, items.len() % n);
    let mut offset = 0;
    (0..n)
        .map(|i| {
            let size = base + usize::from(i < extra);
            let chunk = items[offset..offset + size].to_vec();
            offset += size;
            chunk
        })
        .collect()
}

#[derive(Deserialize, schemars::JsonSchema, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum KnownSource {
    Hackernews,
    Habr,
    Channel,
}

impl KnownSource {
    fn name(self) -> &'static str {
        match self {
            KnownSource::Hackernews => "hackernews",
            KnownSource::Habr => "habr",
            KnownSource::Channel => "channel",
        }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SearchParams {
    /// Natural-language search query. Use for a single facet.
    query: Option<String>,
    /// Batch of 1–8 independent queries for a multi-facet ask. Mutually exclusive with `query`.
    queries: Option<Vec<String>>,
    /// Number of results to return. Default 10. Up to 50 when an LLM reads them directly; up to 150 only when a relevance filter runs over them first.
    // 150 so a cheap classifier step (the workflow's decide filter) can see a
    // wide net; anything an LLM reads directly should stay ≤ 50.
    #[schemars(range(min = 1, max = 150))]
    k: Option<usize>,
    /// Restrict results to one source.
    source: Option<KnownSource>,
    /// Only items with posted_at > this ISO timestamp.
    #[serde(rename = "sinceISO")]
    since_iso: Option<String>,
    /// Only items with posted_at <= this ISO timestamp.
    #[serde(rename = "untilISO")]
    until_iso: Option<String>,
    /// Eval/judge point-in-time replay: only items already searchable at this ISO instant (embedded_at <= asOfISO). Reconstructs what search could have returned at a past moment, excluding rows embedded later (poller/backfill lag). Normal runs omit this.
    #[serde(rename = "asOfISO")]
    as_of_iso: Option<String>,
    /// For source='channel' only: restrict to one Telegram channel by chat_username or chat_id.
    channel: Option<String>,
    /// Map-reduce mode: split the result into EXACTLY this many contiguous chunks and return { count, chunks: [...] } instead of a flat list — one chunk per parallel map step. The chunk count is fixed so a workflow can reference ${bind.chunks.0}, ${bind.chunks.1}, … statically; trailing chunks may be empty when there are few items.
    #[schemars(range(min = 2, max = 8))]
    chunks: Option<usize>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ListParams {
    /// Restrict to one source.
    source: Option<KnownSource>,
    /// Only items with posted_at > this ISO timestamp. Typical use: now - 24h for a daily digest.
    #[serde(rename = "sinceISO")]
    since_iso: Option<String>,
    /// Only items with posted_at <= this ISO timestamp.
    #[serde(rename = "untilISO")]
    until_iso: Option<String>,
    /// Eval/judge point-in-time replay: only items already in the store at this ISO instant (fetched_at <= asOfISO). Reconstructs what the store held at a past moment, excluding rows fetched later (poller lag). Normal runs omit this.
    #[serde(rename = "asOfISO")]
    as_of_iso: Option<String>,
    /// For source='channel' only: restrict to one Telegram channel by chat_username or chat_id.
    channel: Option<String>,
    /// Max rows. Default 500.
    #[schemars(range(min = 1, max = 2000))]
    limit: Option<usize>,
    /// Map-reduce mode: split the result into EXACTLY this many contiguous chunks and return { count, chunks: [...] } instead of a flat list — one chunk per parallel map step. The chunk count is fixed so a workflow can reference ${bind.chunks.0}, ${bind.chunks.1}, … statically; trailing chunks may be empty when there are few items.
    #[schemars(range(min = 2, max = 8))]
    chunks: Option<usize>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct FetchArticleParams {
    /// Article URL to fetch and extract.
    url: String,
}

fn build_filter(
    source: Option<KnownSource>,
    since: Option<&str>,
    until: Option<&str>,
    as_of: Option<&str>,
    channel: Option<String>,
) -> anyhow::Result<Filter> {
    let date = |field, value: Option<&str>| value.map(|value| require_js_date(field, value)).transpose();
    Ok(Filter {
        source: source.map(|s| s.name().to_owned()),
        since: date("sinceISO", since)?,
        until: date("untilISO", until)?,
        as_of: date("asOfISO", as_of)?,
        channel,
    })
}

fn chunks_in_range(chunks: Option<usize>) -> bool {
    chunks.is_none_or(|c| (2..=8).contains(&c))
}

fn source_for_url(url: &str) -> &'static str {
    match url::Url::parse(url).ok().and_then(|u| u.host_str().map(str::to_owned)) {
        Some(host) if host.ends_with("habr.com") => "habr",
        Some(host) if host.ends_with("ycombinator.com") => "hackernews",
        _ => "external",
    }
}

async fn search_news_response(news: &NewsRepository, params: SearchParams) -> anyhow::Result<Value> {
    let (queries, annotate_matches) = match (params.query, params.queries) {
        (Some(query), None) => (vec![query], false),
        (None, Some(queries)) => (queries, true),
        _ => return Ok(json!({ "error": "Pass exactly one of `query` or `queries`." })),
    };
    let filter = build_filter(
        params.source,
        params.since_iso.as_deref(),
        params.until_iso.as_deref(),
        params.as_of_iso.as_deref(),
        params.channel,
    )?;
    let results = news.search(&queries, params.k.unwrap_or(10), &filter, annotate_matches).await?;
    Ok(match params.chunks {
        Some(count) => json!({ "count": results.len(), "chunks": split_chunks(&results, count) }),
        None => json!({ "count": results.len(), "results": results }),
    })
}

async fn list_news_response(news: &NewsRepository, params: ListParams) -> anyhow::Result<Value> {
    let filter = build_filter(
        params.source,
        params.since_iso.as_deref(),
        params.until_iso.as_deref(),
        params.as_of_iso.as_deref(),
        params.channel,
    )?;
    let items = news.list(&filter, params.limit.unwrap_or(500), DEFAULT_DEDUP_THRESHOLD).await?;
    let items: Vec<Value> = items.iter().map(NewsItem::to_json).collect();
    Ok(match params.chunks {
        Some(count) => json!({ "count": items.len(), "chunks": split_chunks(&items, count) }),
        None => json!({ "count": items.len(), "items": items }),
    })
}

fn cached_article_json(item: &NewsItem, url: &str) -> Value {
    let mut response = json!({
        "url": url,
        "title": item.title.as_deref().unwrap_or_default(),
        "text": item.body,
        "source": item.source,
        "sizeChars": item.body.encode_utf16().count(),
        "cached": true,
    });
    if let Some(at) = item.posted_at {
        response["publishedAt"] = iso(at).into();
    }
    response
}

fn fetched_article_json(article: &Article, source: &str) -> Value {
    let mut response = json!({ "url": article.url, "title": article.title, "text": article.text });
    if let Some(site) = &article.site {
        response["site"] = site.clone().into();
    }
    if let Some(at) = article.published_at {
        response["publishedAt"] = iso(at).into();
    }
    if let Some(author) = &article.author {
        response["author"] = author.clone().into();
    }
    response["source"] = source.into();
    response["sizeChars"] = article.text.encode_utf16().count().into();
    response["cached"] = false.into();
    response
}

async fn fetch_article_response(news: &NewsRepository, fetcher: &ArticleFetcher, url: &str) -> anyhow::Result<Value> {
    for source in ["hackernews", "habr", "external"] {
        if let Some(cached) = news.find_by_external_id(source, url).await?
            && !cached.body.trim().is_empty()
        {
            return Ok(cached_article_json(&cached, url));
        }
    }

    let article = fetcher.fetch(url).await?;
    let source = source_for_url(url);
    let mut metadata = Map::new();
    if let Some(author) = &article.author {
        metadata.insert("author".into(), author.clone().into());
    }
    if let Some(site) = &article.site {
        metadata.insert("site".into(), site.clone().into());
    }
    let item = NewsItem {
        source: source.into(),
        external_id: url.to_owned(),
        title: Some(article.title.clone()).filter(|title| !title.is_empty()),
        url: Some(url.to_owned()),
        body: article.text.clone(),
        metadata,
        posted_at: article.published_at,
    };
    if let Err(err) = news.upsert(&item).await {
        tracing::error!(error = %format!("{err:#}"), "fetch_article: save step failed");
    }
    Ok(fetched_article_json(&article, source))
}

#[tool_router(router = news_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "search_news",
        annotations(read_only_hint = true),
        title = "Semantic search over the news store",
        description = "Vector search across every news item the pollers have ingested \
            (Hacker News, Habr, harvested Telegram channels). Returns the \
            closest matches by semantic similarity. Use this when the user \
            asks about a topic — the background pollers keep the store fresh, \
            so there is no need to fetch articles before searching. Returns \
            id, source, title, url, snippet (first ~400 chars of the body), \
            posted_at, distance (lower = closer), and source-specific metadata.\n\n\
            For a multi-facet ask (e.g. one topic spanning several distinct \
            subjects), pass `queries: [...]` — one entry per facet — instead \
            of calling this tool N times or blurring everything into one \
            `query`. Each query is searched independently; results are merged \
            and already de-duplicated across the batch (an item's `distance` \
            is its best match across the facets, `matchedQueries` lists which \
            facets surfaced it), so do NOT re-query per facet or re-dedup. \
            Pass exactly one of `query` or `queries`."
    )]
    async fn search_news(&self, Parameters(params): Parameters<SearchParams>) -> ToolResult {
        let invalid_query = params.query.as_deref().is_some_and(str::is_empty);
        let invalid_batch = params
            .queries
            .as_ref()
            .is_some_and(|queries| queries.is_empty() || queries.len() > 8 || queries.iter().any(String::is_empty));
        let invalid_k = params.k.is_some_and(|k| !(1..=150).contains(&k));
        if invalid_query || invalid_batch || invalid_k || !chunks_in_range(params.chunks) {
            return Err(invalid_params("query must be non-empty, queries 1–8 non-empty strings, k 1–150, chunks 2–8"));
        }
        respond(search_news_response(&self.deps.news, params).await)
    }

    #[tool(
        name = "list_news",
        // Writes only a cache of what it reads: repeating it changes nothing.
        annotations(read_only_hint = true),
        title = "List news items chronologically",
        description = "Read items from the news store ordered by posted_at. Use when \
            you need everything in a time window (e.g. a 24h channel digest) \
            rather than a topical match. Ascending when sinceISO is provided, \
            descending otherwise."
    )]
    async fn list_news(&self, Parameters(params): Parameters<ListParams>) -> ToolResult {
        if params.limit.is_some_and(|l| !(1..=2000).contains(&l)) || !chunks_in_range(params.chunks) {
            return Err(invalid_params("limit must be 1–2000, chunks 2–8"));
        }
        respond(list_news_response(&self.deps.news, params).await)
    }

    #[tool(
        name = "fetch_article",
        // Writes only a cache of what it reads: repeating it changes nothing.
        annotations(read_only_hint = true),
        title = "Fetch and store an arbitrary article URL",
        description = "Download a web article (Mozilla Readability) and save it to the \
            news store so it becomes searchable. Manual override — the HN \
            and Habr pollers already cover their feeds. Use this for ad-hoc \
            URLs the user shares. Returns clean plaintext (title + body). \
            If the URL is already cached, returns the cached row without \
            re-fetching."
    )]
    async fn fetch_article(&self, Parameters(params): Parameters<FetchArticleParams>) -> ToolResult {
        if url::Url::parse(&params.url).is_err() {
            return Err(invalid_params("url must be a valid URL"));
        }
        let fetcher = ArticleFetcher::new(self.deps.fetcher.clone());
        respond(fetch_article_response(&self.deps.news, &fetcher, &params.url).await)
    }
}

// ── 8. tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(xs: &[f32]) -> Vec<f32> {
        let n = xs.iter().map(|x| x * x).sum::<f32>().sqrt();
        xs.iter().map(|x| x / n).collect()
    }

    fn row(id: i64, distance: f64, embedding: Vec<f32>) -> PoolRow {
        PoolRow {
            id,
            source: "hackernews".into(),
            title: Some(format!("t{id}")),
            url: None,
            body: "body".into(),
            metadata: Map::new(),
            posted_at: None,
            distance,
            embedding: Some(embedding),
        }
    }

    fn ids(results: &[SearchResult]) -> Vec<i64> {
        results.iter().map(|r| r.id).collect()
    }

    #[test]
    fn keeps_a_multiply_retrieved_item_once_at_its_min_distance() {
        let (ex, ey, ez) = (unit(&[1., 0., 0.]), unit(&[0., 1., 0.]), unit(&[0., 0., 1.]));
        let out = merge_ranked_pools(
            vec![vec![row(1, 0.2, ex.clone()), row(2, 0.5, ey)], vec![row(1, 0.1, ex), row(3, 0.4, ez)]],
            10,
            DEFAULT_DEDUP_THRESHOLD,
            true,
        );
        assert_eq!(ids(&out), [1, 3, 2]);
        assert!((out[0].distance - 0.1).abs() < 1e-9);
        assert_eq!(out[0].matched_queries, Some(vec![0, 1]));
    }

    #[test]
    fn collapses_cross_query_near_duplicates_keeping_the_closer_one() {
        let out = merge_ranked_pools(
            vec![vec![row(1, 0.2, unit(&[1., 0., 0.]))], vec![row(2, 0.15, unit(&[1., 0.1, 0.]))]],
            10,
            DEFAULT_DEDUP_THRESHOLD,
            true,
        );
        assert_eq!(ids(&out), [2]);
        // Threshold 0 disables dedup: both survive.
        let out = merge_ranked_pools(
            vec![vec![row(1, 0.2, unit(&[1., 0., 0.]))], vec![row(2, 0.15, unit(&[1., 0.1, 0.]))]],
            10,
            0.0,
            true,
        );
        assert_eq!(ids(&out), [2, 1]);
    }

    #[test]
    fn k_caps_after_dedup_and_single_query_omits_matches() {
        let out = merge_ranked_pools(
            vec![
                vec![row(1, 0.5, unit(&[1., 0., 0.])), row(2, 0.1, unit(&[0., 1., 0.]))],
                vec![
                    row(3, 0.3, unit(&[0., 0., 1.])),
                    row(4, 0.2, unit(&[1., 1., 0.])),
                    row(5, 0.9, unit(&[0., 1., 1.])),
                ],
            ],
            2,
            DEFAULT_DEDUP_THRESHOLD,
            true,
        );
        assert_eq!(ids(&out), [2, 4]);
        let single =
            merge_ranked_pools(vec![vec![row(1, 0.3, unit(&[1., 0.])), row(2, 0.1, unit(&[0., 1.]))]], 10, 0.03, false);
        assert_eq!(ids(&single), [2, 1]);
        assert!(serde_json::to_value(&single[0]).unwrap().get("matchedQueries").is_none());
    }

    #[test]
    fn equal_distances_preserve_discovery_order_and_first_match() {
        let first = row(2, 0.1, unit(&[1., 0.]));
        let mut later = first.clone();
        later.title = Some("later title".into());
        let results = merge_ranked_pools(
            vec![vec![first, row(1, 0.1, unit(&[0., 1.]))], vec![later]],
            10,
            DEFAULT_DEDUP_THRESHOLD,
            true,
        );
        assert_eq!(ids(&results), [2, 1]);
        assert_eq!(results[0].title.as_deref(), Some("t2"));
        assert_eq!(results[0].matched_queries, Some(vec![0, 1]));
    }

    #[test]
    fn snippets_cut_at_400_characters() {
        assert_eq!(snippet(&"я".repeat(401)).chars().count(), 401);
        assert!(snippet(&"я".repeat(401)).ends_with('…'));
        assert_eq!(snippet("short"), "short");
    }

    #[test]
    fn splits_into_exactly_n_contiguous_chunks() {
        let items: Vec<i32> = (0..10).collect();
        assert_eq!(split_chunks(&items, 3), vec![vec![0, 1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]);
        assert_eq!(split_chunks(&[1, 2], 4), vec![vec![1], vec![2], vec![], vec![]]);
        assert_eq!(split_chunks::<i32>(&[], 3), vec![Vec::<i32>::new(), vec![], vec![]]);
    }

    fn headline(extra: Map<String, Value>) -> Headline {
        Headline {
            title: "Headline title".into(),
            url: "https://example.com/a".into(),
            author: Some("alice".into()),
            posted_at: parse_js_date("2026-01-01T00:00:00Z"),
            extra,
        }
    }

    fn article(title: &str, text: &str) -> Article {
        Article {
            url: "https://example.com/a".into(),
            title: title.into(),
            text: text.into(),
            site: Some("example.com".into()),
            published_at: parse_js_date("2026-01-02T00:00:00Z"),
            author: Some("bob".into()),
        }
    }

    #[test]
    fn maps_an_article_with_the_headline_author_winning() {
        let mut extra = Map::new();
        extra.insert("hn_id".into(), 1.into());
        let item =
            article_item("hackernews", &headline(extra), Some(article("Article title", "body content"))).unwrap();
        assert_eq!(item.title.as_deref(), Some("Article title"));
        assert_eq!(
            item.metadata,
            json!({ "hn_id": 1, "author": "alice", "site": "example.com" }).as_object().unwrap().clone()
        );
        assert_eq!(item.posted_at, parse_js_date("2026-01-01T00:00:00Z"), "the listing's time wins");
        // Empty extracted title falls back to the headline's.
        assert_eq!(
            article_item("habr", &headline(Map::new()), Some(article("", "b"))).unwrap().title.as_deref(),
            Some("Headline title")
        );
    }

    #[test]
    fn a_page_date_is_only_a_fallback_and_never_in_the_future() {
        let undated = Headline { posted_at: None, ..headline(Map::new()) };
        let item = article_item("hackernews", &undated, Some(article("t", "b"))).unwrap();
        assert_eq!(item.posted_at, parse_js_date("2026-01-02T00:00:00Z"));
        let future = Article { published_at: parse_js_date("2609-09-26T00:00:00Z"), ..article("t", "b") };
        assert_eq!(article_item("hackernews", &undated, Some(future)).unwrap().posted_at, None);
    }

    #[test]
    fn drops_failed_or_blank_extractions() {
        assert!(article_item("habr", &headline(Map::new()), None).is_none());
        assert!(article_item("habr", &headline(Map::new()), Some(article("t", "   \n "))).is_none());
    }

    #[test]
    fn channel_posts_have_no_title_and_link_when_public() {
        let m = crate::userbot::ChannelMessage {
            id: 7,
            date: parse_js_date("2026-01-01T00:00:00Z").unwrap(),
            text: "body text".into(),
            views: Some(123),
            forwards: Some(4),
        };
        let item = channel_item("100", Some("Channel One"), Some("channel_one"), &m);
        assert_eq!(item.external_id, "100:7");
        assert_eq!(item.title, None);
        assert_eq!(item.url.as_deref(), Some("https://t.me/channel_one/7"));
        assert_eq!(
            Value::Object(item.metadata),
            json!({ "chat_id": "100", "chat_title": "Channel One", "chat_username": "channel_one", "tg_message_id": 7, "views": 123, "forwards": 4 })
        );
        assert_eq!(channel_item("100", None, None, &m).url, None);
    }

    #[test]
    fn strips_markup_and_entities_to_compact_text() {
        assert_eq!(
            strip_html("<p>Hello&nbsp;<b>world</b></p><script>x()</script>\n<style>.a{}</style> &amp; more"),
            "Hello world & more"
        );
    }

    #[test]
    fn extracts_a_readable_article() {
        let body = "Graphs (графы) are a set of vertices and edges. ".repeat(40);
        let html = format!(
            "<html><head><title>About graphs</title><meta property=\"og:site_name\" content=\"Хабр\">\
             <meta property=\"article:published_time\" content=\"2026-01-02T00:00:00Z\"></head>\
             <body><nav>menu</nav><article><h1>About graphs</h1><p>{body}</p><p>{body}</p></article></body></html>"
        );
        let article = extract_article("https://habr.com/ru/articles/1/", &html).unwrap();
        assert!(article.text.contains("(графы) are a set of vertices"));
        assert_eq!(article.site.as_deref(), Some("Хабр"));
        assert_eq!(article.published_at, parse_js_date("2026-01-02T00:00:00Z"));
    }

    #[test]
    fn cached_article_response_preserves_legacy_shape_and_utf16_length() {
        let mut cached = item("external", "cached", "ignored", "я😀", "invalid date");
        cached.title = None;
        cached.metadata = json!({ "site": "example.com", "author": "alice" }).as_object().unwrap().clone();
        assert_eq!(
            cached_article_json(&cached, "https://example.com/a"),
            json!({
                "url": "https://example.com/a", "title": "", "text": "я😀",
                "source": "external", "sizeChars": 3, "cached": true,
            })
        );
        cached.posted_at = parse_js_date("2026-01-02T00:00:00Z");
        assert_eq!(cached_article_json(&cached, "url")["publishedAt"], "2026-01-02T00:00:00.000Z");
    }

    #[test]
    fn fetched_article_response_includes_only_present_metadata() {
        let extracted = article("title", "я😀");
        assert_eq!(
            fetched_article_json(&extracted, "external"),
            json!({
                "url": "https://example.com/a", "title": "title", "text": "я😀",
                "site": "example.com", "publishedAt": "2026-01-02T00:00:00.000Z", "author": "bob",
                "source": "external", "sizeChars": 3, "cached": false,
            })
        );
        let no_metadata = Article { site: None, published_at: None, author: None, ..extracted };
        assert_eq!(
            fetched_article_json(&no_metadata, "external"),
            json!({
                "url": "https://example.com/a", "title": "title", "text": "я😀",
                "source": "external", "sizeChars": 3, "cached": false,
            })
        );
    }

    // ── against Postgres (TEST_DATABASE_URL) ────────────────────────────────

    fn item(source: &str, id: &str, title: &str, body: &str, posted: &str) -> NewsItem {
        NewsItem {
            source: source.into(),
            external_id: id.into(),
            title: Some(title.into()),
            url: Some(format!("https://example.com/{id}")),
            body: body.into(),
            metadata: Map::new(),
            posted_at: parse_js_date(posted),
        }
    }

    #[tokio::test]
    async fn pg_saves_lists_and_searches() {
        let Some(pool) = crate::pg::test_pool().await else { return };
        let embedder = crate::embeddings::testing::FakeEmbedder::with_dims(1536);
        let repo = NewsRepository::new(pool.clone(), embedder.clone());
        let source = format!("test-{}", rand::random::<u32>());
        let items = vec![
            item(&source, "a", "Rust async runtimes", "tokio executor scheduling futures", "2026-01-01T00:00:00Z"),
            item(&source, "b", "Postgres vector search", "pgvector cosine distance ivfflat", "2026-01-02T00:00:00Z"),
            item(&source, "c", "Odesa news", "drone strikes on Odesa overnight", "2026-01-03T00:00:00Z"),
        ];
        assert_eq!(repo.save(&items).await.unwrap(), SaveResult { saved: 3, embedded: 3, failed: 0 });
        // Re-saving the same natural keys is a no-op.
        assert_eq!(repo.save(&items).await.unwrap().saved, 0);

        let filter = Filter { source: Some(source.clone()), ..Filter::default() };
        let listed: Vec<String> =
            repo.list(&filter, 10, 0.03).await.unwrap().into_iter().map(|i| i.external_id).collect();
        assert_eq!(listed, ["c", "b", "a"]);
        let since = Filter { since: parse_js_date("2026-01-01T12:00:00Z"), ..filter.clone() };
        let ascending: Vec<String> =
            repo.list(&since, 10, 0.03).await.unwrap().into_iter().map(|i| i.external_id).collect();
        assert_eq!(ascending, ["b", "c"]);

        let hits = repo.search(&["pgvector cosine distance".into()], 2, &filter, false).await.unwrap();
        assert_eq!(hits[0].url.as_deref(), Some("https://example.com/b"));
        assert!(hits[0].matched_queries.is_none());
        let multi = repo.search(&["tokio futures".into(), "Odesa drones".into()], 5, &filter, true).await.unwrap();
        assert_eq!(multi.len(), 3);
        assert!(multi.iter().all(|h| h.matched_queries.is_some()));

        // Upsert replaces the body and re-embeds it.
        let mut changed = items[0].clone();
        changed.body = "completely new body about borrow checker".into();
        assert_eq!(repo.upsert(&changed).await.unwrap().embedded, 1);
        assert_eq!(repo.find_by_external_id(&source, "a").await.unwrap().unwrap().body, changed.body);

        // A failed inline embed leaves a NULL vector for the backfill.
        embedder.set_down(true);
        let late = item(&source, "d", "late", "embedder was down", "2026-01-04T00:00:00Z");
        assert_eq!(
            repo.save(std::slice::from_ref(&late)).await.unwrap(),
            SaveResult { saved: 1, embedded: 0, failed: 1 }
        );
        embedder.set_down(false);
        assert!(repo.embed_missing_batch(1000, 0).await.unwrap().embedded >= 1);
        assert_eq!(
            repo.search(&["embedder was down".into()], 1, &filter, false).await.unwrap()[0].url.as_deref(),
            Some("https://example.com/d")
        );
    }

    #[tokio::test]
    async fn pg_channel_posts_filter_and_watermark() {
        let Some(pool) = crate::pg::test_pool().await else { return };
        let repo = NewsRepository::new(pool.clone(), crate::embeddings::testing::FakeEmbedder::with_dims(1536));
        let chat = format!("{}", rand::random::<u32>());
        let post = |id: i64, text: &str| crate::userbot::ChannelMessage {
            id,
            date: Utc::now(),
            text: text.into(),
            views: Some(1),
            forwards: None,
        };
        assert_eq!(channel_watermark(&pool, &chat).await.unwrap(), None);
        let items = vec![
            channel_item(&chat, Some("Chan"), Some("chan_user"), &post(7, "first post")),
            channel_item(&chat, Some("Chan"), None, &post(12, "second post")),
        ];
        repo.save(&items).await.unwrap();
        assert_eq!(channel_watermark(&pool, &chat).await.unwrap(), Some(12));
        let by_chat = Filter { source: Some("channel".into()), channel: Some(chat.clone()), ..Filter::default() };
        assert_eq!(repo.list(&by_chat, 10, 0.0).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn writes_release_the_connection_before_embedding() {
        let Some(pool) = crate::pg::test_pool().await else { return };
        pool.resize(1);
        let embedder = crate::embeddings::testing::FakeEmbedder::with_dims(1536);
        let repo = NewsRepository::new(pool, embedder.clone());
        let source = format!("test-single-connection-{}", rand::random::<u32>());
        let news = item(&source, "a", "title", "body", "2026-01-01T00:00:00Z");
        tokio::time::timeout(Duration::from_secs(10), async {
            assert_eq!(repo.save(std::slice::from_ref(&news)).await.unwrap().embedded, 1);
            assert_eq!(repo.upsert(&news).await.unwrap().embedded, 1);
            embedder.set_down(true);
            let unembedded = NewsItem { external_id: "b".into(), ..news };
            assert_eq!(repo.save(&[unembedded]).await.unwrap().failed, 1);
            embedder.set_down(false);
            assert!(repo.embed_missing_batch(1000, 0).await.unwrap().embedded >= 1);
        })
        .await
        .expect("save, upsert and backfill must work with a single pooled connection");
    }

    #[test]
    fn classifies_urls_by_host() {
        assert_eq!(source_for_url("https://habr.com/ru/articles/1/"), "habr");
        assert_eq!(source_for_url("https://news.ycombinator.com/item?id=1"), "hackernews");
        assert_eq!(source_for_url("https://example.com"), "external");
    }
}

// ── tests: vector search on Postgres ─────────────────────────────────────────
// A port of packages/mcp/src/services/news/core/repository.pg.test.ts and the
// ordering half of repository.test.ts: the pool is ordered by the expression
// the HNSW index covers (an exact `<=>` on the full vector is a sequential
// scan over every embedding — 9.5 s at 117k rows on prod), while the distance
// a caller sees stays exact.

#[cfg(test)]
mod search_tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::embeddings::Embedder;

    const DIMS: usize = 1536;

    // Deterministic unit vectors: a seeded LCG, so a failure reproduces.
    fn unit_vectors(count: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut state = seed;
        let mut next = move || {
            state = (state * 1_103_515_245 + 12_345) % (1 << 31);
            state as f64 / f64::from(1u32 << 31) - 0.5
        };
        (0..count)
            .map(|_| {
                let v: Vec<f64> = (0..DIMS).map(|_| next()).collect();
                let norm = v.iter().map(|x| x * x).sum::<f64>().sqrt();
                v.iter().map(|x| (x / norm) as f32).collect()
            })
            .collect()
    }

    fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
        1.0 - a.iter().zip(b).map(|(x, y)| f64::from(*x) * f64::from(*y)).sum::<f64>()
    }

    struct Fixed(Vec<f32>);

    #[async_trait]
    impl Embedder for Fixed {
        async fn embed_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
            Ok(texts.iter().map(|_| self.0.clone()).collect())
        }
    }

    async fn setup() -> Option<(NewsRepository, PgPool, Vec<Vec<f32>>, Vec<f32>)> {
        let pool = crate::pg::fresh_test_pool().await?;
        let vectors = unit_vectors(60, 7);
        let query = unit_vectors(1, 99).remove(0);
        let client = pool.get().await.unwrap();
        for (i, v) in vectors.iter().enumerate() {
            client
                .execute(
                    "INSERT INTO news_items (source, external_id, title, body, embedding, embedded_at)
                     VALUES ($1, $2, $3, $4, $5::text::vector, now())",
                    &[
                        &(if i % 2 == 0 { "habr" } else { "hackernews" }),
                        &format!("item-{i}"),
                        &format!("item {i}"),
                        &format!("body {i}"),
                        &vector_literal(v),
                    ],
                )
                .await
                .unwrap();
        }
        let repo = NewsRepository::new(pool.clone(), Arc::new(Fixed(query.clone())));
        Some((repo, pool, vectors, query))
    }

    fn expected(vectors: &[Vec<f32>], query: &[f32], keep: impl Fn(usize) -> bool, k: usize) -> Vec<(String, f64)> {
        let mut all: Vec<(String, f64)> = vectors
            .iter()
            .enumerate()
            .filter(|(i, _)| keep(*i))
            .map(|(i, v)| (format!("item {i}"), cosine_distance(v, query)))
            .collect();
        all.sort_by(|a, b| a.1.total_cmp(&b.1));
        all.truncate(k);
        all
    }

    #[tokio::test]
    async fn returns_the_nearest_items_in_order_with_the_exact_full_vector_distance() {
        let Some((repo, _pool, vectors, query)) = setup().await else { return };
        let results = repo.search(&["anything".into()], 5, &Filter::default(), false).await.unwrap();
        let want = expected(&vectors, &query, |_| true, 5);
        assert_eq!(
            results.iter().map(|r| r.title.clone().unwrap_or_default()).collect::<Vec<_>>(),
            want.iter().map(|w| w.0.clone()).collect::<Vec<_>>()
        );
        for (r, w) in results.iter().zip(&want) {
            assert!((r.distance - w.1).abs() < 1e-6, "{} vs {}", r.distance, w.1);
        }
    }

    #[tokio::test]
    async fn returns_a_sources_exact_nearest_items_under_a_source_filter() {
        let Some((repo, _pool, vectors, query)) = setup().await else { return };
        let filter = Filter { source: Some("habr".into()), ..Filter::default() };
        let results = repo.search(&["anything".into()], 10, &filter, false).await.unwrap();
        let want = expected(&vectors, &query, |i| i % 2 == 0, 10);
        assert_eq!(
            results.iter().map(|r| r.title.clone().unwrap_or_default()).collect::<Vec<_>>(),
            want.iter().map(|w| w.0.clone()).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn orders_the_pool_by_the_expression_the_hnsw_index_covers() {
        let Some((_repo, pool, _vectors, query)) = setup().await else { return };
        let (sql, q) = NewsRepository::pool_query(&query, 30, &Filter::default(), SearchOrdering::Approximate);
        let mut client = pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        // With the sequential scan priced out, a plan without the index means
        // the ORDER BY no longer matches the indexed expression.
        tx.batch_execute("SET LOCAL enable_seqscan = off").await.unwrap();
        let plan: Vec<String> =
            tx.query(&format!("EXPLAIN {sql}"), &q.params()).await.unwrap().iter().map(|r| r.get(0)).collect();
        assert!(plan.join("\n").contains("news_items_embedding_hnsw"), "{}", plan.join("\n"));
    }

    #[test]
    fn walks_the_index_unfiltered_and_under_time_filters() {
        let now = Utc::now();
        assert_eq!(search_ordering(&Filter::default()), SearchOrdering::Approximate);
        assert_eq!(
            search_ordering(&Filter { since: Some(now), until: Some(now), ..Filter::default() }),
            SearchOrdering::Approximate
        );
        assert_eq!(search_ordering(&Filter { as_of: Some(now), ..Filter::default() }), SearchOrdering::Approximate);
    }

    #[test]
    fn keeps_the_exact_ordering_under_a_source_or_a_channel_filter() {
        assert_eq!(
            search_ordering(&Filter { source: Some("habr".into()), ..Filter::default() }),
            SearchOrdering::Exact
        );
        assert_eq!(
            search_ordering(&Filter {
                source: Some("channel".into()),
                channel: Some("ai_newz".into()),
                ..Filter::default()
            }),
            SearchOrdering::Exact
        );
    }

    #[test]
    fn ef_search_is_4x_the_pool_between_200_and_600() {
        assert_eq!(ann_ef_search(30), 200);
        assert_eq!(ann_ef_search(100), 400);
        assert_eq!(ann_ef_search(300), 600);
    }
}
