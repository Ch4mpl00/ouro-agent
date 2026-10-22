// fetch_article over MCP on a real server and Postgres, with the web played
// by a stand-in fetcher that serves articles, binaries, errors and bodies far
// bigger than any article — counting every byte the server pulls. A port of
// packages/mcp/test/fetch-article.test.ts. The URL comes from the model (or a
// ChatGPT user over the tunnel); the HN and Habr pollers run the same
// extraction on whatever the front page links to.
//
// Rules, from the page served:
//   - no response is read past a bound, whatever its size: one huge link
//     must not take the server's memory with it;
//   - a binary (PDF, image, archive, video) is refused without being read,
//     and nothing is stored;
//   - an error status is a tool error, and nothing is stored;
//   - an HTML article comes back as its text, is stored once and becomes
//     searchable; asking again answers from the store without fetching;
//   - an article the store refuses still comes back, marked unsaved.

mod support;

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use mcp_tools::fetch::{Fetch, Fetched};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;
use support::{
    Client, Outcome, Server, TestDatabase, call, connect, property_runs, runtime, start_test_server_with_fetch,
};

const CHUNK: usize = 64 * 1024;
// What any page may cost: the article cap plus one chunk of read-ahead.
const READ_BOUND: usize = 8 * 1024 * 1024 + CHUNK;

#[derive(Debug, Clone)]
enum Page {
    Article { words: String, pad_kb: usize, content_type: Option<&'static str> },
    Binary { content_type: &'static str, size_kb: usize },
    Error { status: u16 },
    Endless { content_type: &'static str },
}

const WORDS: [&str; 8] = ["kestrel", "basalt", "quokka", "fjord", "zephyr", "lichen", "osprey", "tundra"];

fn page() -> impl Strategy<Value = Page> {
    prop_oneof![
        4 => (
            Just(WORDS.to_vec()).prop_shuffle().prop_map(|ws| ws[..3].join(" ")),
            prop_oneof![4 => 0usize..=50, 1 => 9_000usize..=20_000],
            proptest::sample::select(vec![Some("text/html; charset=utf-8"), Some("text/html"), Some("application/xhtml+xml"), None]),
        )
            .prop_map(|(words, pad_kb, content_type)| Page::Article { words, pad_kb, content_type }),
        2 => (
            proptest::sample::select(vec!["application/pdf", "image/png", "application/zip", "video/mp4", "application/octet-stream"]),
            1usize..=30_000,
        )
            .prop_map(|(content_type, size_kb)| Page::Binary { content_type, size_kb }),
        1 => proptest::sample::select(vec![404u16, 410, 500, 503]).prop_map(|status| Page::Error { status }),
        1 => proptest::sample::select(vec!["text/html", "application/octet-stream"]).prop_map(|content_type| Page::Endless { content_type }),
    ]
}

fn paragraph(words: &str, i: usize) -> String {
    format!(
        "<p>Paragraph {i} about the {words}. This sentence is long enough for a reader to count it as prose, and it keeps going for a while so that Readability sees a real article body here.</p>"
    )
}

fn html(words: &str, pad_kb: usize) -> String {
    let head = format!(
        "<!doctype html><html><head><title>On the {words}</title></head><body><article><h1>On the {words}</h1>"
    );
    let paras: String = (0..6).map(|i| paragraph(words, i)).collect();
    let pad = format!("<p>{}</p>", "filler text ".repeat((pad_kb * 1024).div_ceil(12)));
    format!("{head}{paras}</article><aside>{pad}</aside></body></html>")
}

// The stand-in web: pages by URL, every fetch counted, every pulled byte
// counted.
#[derive(Default)]
struct Web {
    pages: Mutex<HashMap<String, Page>>,
    pulled: Mutex<HashMap<String, Arc<AtomicUsize>>>,
    hits: Mutex<HashMap<String, usize>>,
}

// A body streamed in chunks, counting what the reader pulls.
fn streamed(
    chunks: impl Iterator<Item = Vec<u8>> + Send + 'static,
    pulled: Arc<AtomicUsize>,
) -> futures::stream::BoxStream<'static, anyhow::Result<Vec<u8>>> {
    Box::pin(futures::stream::iter(chunks).map(move |chunk| {
        pulled.fetch_add(chunk.len(), Ordering::SeqCst);
        Ok(chunk)
    }))
}

use futures::StreamExt;

#[async_trait]
impl Fetch for Web {
    async fn get(&self, url: &str, _timeout: Duration) -> anyhow::Result<Fetched> {
        *self.hits.lock().unwrap().entry(url.to_owned()).or_default() += 1;
        let count = Arc::new(AtomicUsize::new(0));
        self.pulled.lock().unwrap().insert(url.to_owned(), count.clone());
        let page = self.pages.lock().unwrap().get(url).cloned();
        let respond = |status: u16, content_type: Option<&str>, body| Fetched {
            url: url.to_owned(),
            status,
            content_type: content_type.map(str::to_owned),
            body,
        };
        Ok(match page {
            None => respond(404, None, streamed(std::iter::once(b"not found".to_vec()), count)),
            Some(Page::Error { status }) => respond(status, None, streamed(std::iter::once(b"gone".to_vec()), count)),
            Some(Page::Binary { content_type, size_kb }) => {
                let bytes = size_kb * 1024;
                let chunks = (0..bytes).step_by(CHUNK).map(move |sent| vec![0u8; CHUNK.min(bytes - sent)]);
                respond(200, Some(content_type), streamed(chunks, count))
            }
            Some(Page::Endless { content_type }) => {
                let para = paragraph("endless", 0);
                let chunk: Vec<u8> = para.repeat(CHUNK / para.len() + 1).into_bytes()[..CHUNK].to_vec();
                respond(200, Some(content_type), streamed(std::iter::repeat(chunk), count))
            }
            Some(Page::Article { words, pad_kb, content_type }) => {
                let bytes = html(&words, pad_kb).into_bytes();
                let chunks: Vec<Vec<u8>> = bytes.chunks(CHUNK).map(<[u8]>::to_vec).collect();
                respond(200, content_type, streamed(chunks.into_iter(), count))
            }
        })
    }
}

struct Env {
    _db: TestDatabase,
    server: Server,
    client: Client,
    web: Arc<Web>,
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        runtime().block_on(async {
            let db = support::test_database().await;
            let web = Arc::new(Web::default());
            let server = start_test_server_with_fetch(&db, None, web.clone()).await.expect("server starts");
            let client = connect(&server.url).await;
            Env { _db: db, server, client, web }
        })
    })
}

async fn count_items(env: &Env) -> i64 {
    env.server.pool.get().await.unwrap().query_one("SELECT count(*) FROM news_items", &[]).await.unwrap().get(0)
}

#[test]
fn bounds_what_it_reads_refuses_binaries_and_stores_each_article_once() {
    let env = env();
    let witnessed = Mutex::new(BTreeSet::new());
    let n = AtomicUsize::new(0);
    TestRunner::new(Config::with_cases(property_runs(40)))
        .run(&(page(), any::<bool>()), |(mut p, again)| {
            runtime().block_on(async {
                let i = n.fetch_add(1, Ordering::SeqCst) + 1;
                let url = format!("https://example.org/a/{i}");
                // A word of its own, so search can tell it from another article
                // drawn with the same words.
                if let Page::Article { words, .. } = &mut p {
                    words.push_str(&format!(" ref{i}"));
                }
                env.web.pages.lock().unwrap().insert(url.clone(), p.clone());
                let before = count_items(env).await;
                let out = call(&env.client, "fetch_article", json!({ "url": url })).await;
                let read = env.web.pulled.lock().unwrap().get(&url).map_or(0, |c| c.load(Ordering::SeqCst));
                let stored: Vec<String> = env
                    .server
                    .pool
                    .get()
                    .await
                    .unwrap()
                    .query("SELECT body FROM news_items WHERE external_id = $1", &[&url])
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| r.get(0))
                    .collect();
                let at = format!("{url} {p:?}: {}", format!("{out:?}").chars().take(200).collect::<String>());

                assert!(read <= READ_BOUND, "{at}: read {read} bytes");
                let bigger = match &p {
                    Page::Endless { .. } => true,
                    Page::Binary { size_kb, .. } => size_kb * 1024 > READ_BOUND,
                    Page::Article { pad_kb, .. } => pad_kb * 1024 > READ_BOUND,
                    Page::Error { .. } => false,
                };
                if bigger {
                    witnessed.lock().unwrap().insert("a body bigger than the bound");
                }
                let binary = matches!(p, Page::Binary { .. })
                    || matches!(p, Page::Endless { content_type } if content_type != "text/html");
                if binary {
                    witnessed.lock().unwrap().insert("a binary");
                    assert!(matches!(out, Outcome::ToolError(_)), "{at}: a binary is refused");
                    assert!(read <= CHUNK, "{at}: …without reading it");
                    assert!(stored.is_empty(), "{at}: nothing stored");
                    return;
                }
                if let Page::Error { .. } = p {
                    assert!(matches!(out, Outcome::ToolError(_)), "{at}: an error status is a tool error");
                    assert!(stored.is_empty(), "{at}: nothing stored");
                    return;
                }
                let Page::Article { words, .. } = &p else { return }; // HTML that never ends: bounded is all we ask
                witnessed.lock().unwrap().insert("an article");
                let Outcome::Ok(value) = out else { panic!("{at}") };
                let text = value["text"].as_str().unwrap_or_default().to_owned();
                assert!(text.contains(&format!("Paragraph 0 about the {words}")), "{at}: the article's own text");
                assert_eq!(stored.len(), 1, "{at}: stored once");
                assert_eq!(count_items(env).await, before + 1, "{at}");
                let found = call(&env.client, "search_news", json!({ "query": words, "limit": 50 })).await;
                assert!(format!("{found:?}").contains(&url), "{at}: searchable");
                if again {
                    let fetches = env.web.hits.lock().unwrap().get(&url).copied();
                    let second = call(&env.client, "fetch_article", json!({ "url": url })).await;
                    let Outcome::Ok(second) = second else { panic!("{at}: answered from the store") };
                    assert_eq!(second["cached"], true, "{at}: answered from the store");
                    assert_eq!(second["text"], json!(text), "{at}: the same text");
                    assert_eq!(
                        env.web.hits.lock().unwrap().get(&url).copied(),
                        fetches,
                        "{at}: without fetching again"
                    );
                }
            });
            Ok(())
        })
        .unwrap();
    assert_eq!(
        witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(),
        ["a binary", "a body bigger than the bound", "an article"]
    );
}

#[test]
fn an_article_the_store_refuses_comes_back_marked_unsaved() {
    let env = env();
    runtime().block_on(async {
        let client = env.server.pool.get().await.unwrap();
        client
            .batch_execute(
                "CREATE OR REPLACE FUNCTION refuse_broken() RETURNS trigger AS $$
                 BEGIN RAISE EXCEPTION 'store refused'; END $$ LANGUAGE plpgsql;
                 DROP TRIGGER IF EXISTS refuse_broken ON news_items;
                 CREATE TRIGGER refuse_broken BEFORE INSERT ON news_items FOR EACH ROW
                 WHEN (NEW.external_id LIKE '%/broken/%') EXECUTE FUNCTION refuse_broken();",
            )
            .await
            .unwrap();
        let url = "https://example.org/broken/1".to_owned();
        let page = Page::Article { words: "kestrel basalt refused".into(), pad_kb: 0, content_type: Some("text/html") };
        env.web.pages.lock().unwrap().insert(url.clone(), page);
        let out = call(&env.client, "fetch_article", json!({ "url": url })).await;
        let Outcome::Ok(value) = out else { panic!("the text still comes back: {out:?}") };
        assert!(value["text"].as_str().unwrap_or_default().contains("kestrel basalt refused"));
        assert_eq!(value["saved"], false);
        assert!(value["saveError"].as_str().unwrap_or_default().contains("store refused"), "{value}");
        let stored = client.query("SELECT 1 FROM news_items WHERE external_id = $1", &[&url]).await.unwrap();
        assert!(stored.is_empty());
    });
}
