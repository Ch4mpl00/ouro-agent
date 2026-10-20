// The Gmail poller against a fake Gmail API (support/fake_gmail.rs) and the
// real state database: mails arrive, the poller ticks — sometimes a fetch or a
// signal write fails midway — and once it has caught up, every matching mail
// that arrived after the bootstrap must have become exactly one signal, in
// arrival order, carrying its attachments; nothing else may. A port of
// packages/mcp/test/gmail-poller.test.ts.
//
// PROPERTY_RUNS raises the number of generated timelines.

mod support;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use async_trait::async_trait;
use mcp_tools::db::Db;
use mcp_tools::gmail::{GmailModule, SUBSCRIPTIONS};
use mcp_tools::signals::SignalSink;
use mcp_tools::signals::Signals;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use support::fake_gmail::{self, FakeAttachment, FakeGmail, FakeMail};
use support::{TestDatabase, property_runs, runtime};

#[derive(Debug, Clone)]
struct Mail {
    matches: bool,
    gap_ms: i64,
    pdfs: usize,
    others: usize,
    // A subject carrying a NUL (a decoded RFC 2047 word can), which Postgres
    // text can't hold.
    nul: bool,
}

#[derive(Debug, Clone)]
enum Fault {
    None,
    Get(usize),
    Record(usize),
}

#[derive(Debug, Clone)]
enum Step {
    Arrive(Vec<Mail>),
    Tick(Fault),
}

fn mail() -> impl Strategy<Value = Mail> {
    (
        prop_oneof![4 => Just(true), 1 => Just(false)],
        1i64..=4_000,
        0usize..=2,
        0usize..=1,
        prop_oneof![6 => Just(false), 1 => Just(true)],
    )
        .prop_map(|(matches, gap_ms, pdfs, others, nul)| Mail { matches, gap_ms, pdfs, others, nul })
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => proptest::collection::vec(mail(), 1..=4).prop_map(Step::Arrive),
        // A burst after an outage: more than one page of the list.
        1 => (51usize..=70).prop_map(|n| Step::Arrive(vec![Mail { matches: true, gap_ms: 1_000, pdfs: 1, others: 0, nul: false }; n])),
        5 => prop_oneof![
            3 => Just(Step::Tick(Fault::None)),
            1 => (0usize..(i32::MAX as usize)).prop_map(|pick| Step::Tick(Fault::Get(pick))),
            1 => proptest::sample::select(vec![1usize, 1, 2, 3]).prop_map(|at| Step::Tick(Fault::Record(at))),
        ],
    ]
}

// Signals whose `at`-th record fails, as a dropped connection would.
struct FailingAt<'a> {
    inner: &'a Signals,
    at: usize,
    n: AtomicUsize,
}

#[async_trait]
impl SignalSink for FailingAt<'_> {
    async fn record(&self, source: &str, content: &str) -> anyhow::Result<()> {
        anyhow::ensure!(self.n.fetch_add(1, Ordering::SeqCst) + 1 != self.at, "injected: connection terminated");
        SignalSink::record(self.inner, source, content).await
    }
}

struct Env {
    _db: TestDatabase,
    state: Db,
    fake: FakeGmail,
    gmail: GmailModule,
    signals: Signals,
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        runtime().block_on(async {
            let db = support::test_database().await;
            let state = Db::connect_state(&db.url, &db.state_url).await.expect("state database");
            let fake = fake_gmail::start().await;
            let expires =
                (chrono::Utc::now() + chrono::Duration::hours(24)).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            state
                .client()
                .await
                .unwrap()
                .execute(
                    "INSERT INTO integration_account (provider, account_key, access_token, refresh_token, expires_at)
                     VALUES ('gmail', 'me@example.com', 'test-token', 'test-refresh', $1)",
                    &[&expires],
                )
                .await
                .unwrap();
            let gmail = GmailModule::with_api_root(state.clone(), reqwest::Client::new(), &fake.root_url);
            let signals = Signals::new(state.clone());
            Env { _db: db, state, fake, gmail, signals }
        })
    })
}

#[test]
fn turns_every_matching_mail_into_exactly_one_signal_in_order_across_failures_and_bursts() {
    let env = env();
    let sub = &SUBSCRIPTIONS[0];
    let witnessed = Mutex::new(BTreeSet::new());
    TestRunner::new(Config::with_cases(property_runs(25)))
        .run(&proptest::collection::vec(step(), 1..=12), |steps| {
            runtime().block_on(async {
                let client = env.state.client().await.unwrap();
                client.batch_execute("TRUNCATE signals, gmail_kv RESTART IDENTITY").await.unwrap();
                env.fake.reset();

                // Bootstrap: the first poll only sets the watermark.
                env.gmail.poll_subscription_once(sub, &env.signals).await.unwrap();
                let watermark: i64 = client
                    .query_one("SELECT value FROM gmail_kv", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0)
                    .parse()
                    .expect("bootstrap sets a watermark");
                let signalled: i64 = client.query_one("SELECT count(*) FROM signals", &[]).await.unwrap().get(0);
                assert_eq!(signalled, 0, "bootstrap emits nothing");

                let mut clock = watermark + 1_000;
                let mut n = 0;
                for s in &steps {
                    match s {
                        Step::Arrive(mails) => {
                            if mails.len() > 50 {
                                witnessed.lock().unwrap().insert("a burst of more than one page");
                            }
                            for m in mails {
                                n += 1;
                                clock += m.gap_ms;
                                let pdf = (0..m.pdfs).map(|i| FakeAttachment {
                                    attachment_id: format!("att-{n}-pdf{i}"),
                                    filename: format!("bill-{n}-{i}.pdf"),
                                    mime_type: "application/pdf".into(),
                                    size: 1000,
                                    content: None,
                                    nested: false,
                                });
                                let other = (0..m.others).map(|i| FakeAttachment {
                                    attachment_id: format!("att-{n}-img{i}"),
                                    filename: format!("logo-{n}.png"),
                                    mime_type: "image/png".into(),
                                    size: 10,
                                    content: None,
                                    nested: false,
                                });
                                if m.nul && m.matches {
                                    witnessed.lock().unwrap().insert("a subject with a NUL");
                                }
                                env.fake.lock().mails.push(FakeMail {
                                    id: format!("m{n}"),
                                    internal_date: clock,
                                    subject: if m.nul { format!("Bill {n}\0") } else { format!("Bill {n}") },
                                    matches: m.matches,
                                    attachments: pdf.chain(other).collect(),
                                });
                            }
                        }
                        Step::Tick(fault) => {
                            if let Fault::Get(pick) = fault {
                                // One of the newest matching mails: likely not
                                // signalled yet, so the poll really reaches it
                                // and fails there.
                                let pending: Vec<String> =
                                    env.fake.lock().mails.iter().filter(|m| m.matches).map(|m| m.id.clone()).collect();
                                let span = pending.len().clamp(1, 3);
                                if let Some(target) =
                                    pending.len().checked_sub(1 + pick % span).and_then(|i| pending.get(i))
                                {
                                    env.fake.fail_get(target, 1);
                                }
                            }
                            let failed = match fault {
                                Fault::Record(at) => {
                                    let failing = FailingAt { inner: &env.signals, at: *at, n: AtomicUsize::new(0) };
                                    env.gmail.poll_subscription_once(sub, &failing).await.is_err()
                                }
                                _ => env.gmail.poll_subscription_once(sub, &env.signals).await.is_err(),
                            };
                            if failed {
                                witnessed.lock().unwrap().insert("a poll fails midway");
                            }
                        }
                    }
                }
                // Catch up on healthy ticks.
                for _ in 0..6 {
                    env.gmail.poll_subscription_once(sub, &env.signals).await.unwrap();
                }

                let emitted: Vec<(String, String, String)> = client
                    .query("SELECT source, content FROM signals ORDER BY id", &[])
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| {
                        let content: String = r.get(1);
                        let id =
                            regex::Regex::new(r"messageId: (\S+)").unwrap().captures(&content).map(|c| c[1].to_owned());
                        (
                            id.unwrap_or_else(|| panic!("a Gmail signal without a messageId: {content:.200}")),
                            r.get(0),
                            content,
                        )
                    })
                    .collect();
                let mails = env.fake.lock().mails.clone();
                let mut matching: Vec<&FakeMail> = mails.iter().filter(|m| m.matches).collect();
                matching.sort_by_key(|m| m.internal_date);
                let want: Vec<&str> = matching.iter().map(|m| m.id.as_str()).collect();
                let at = format!("{:.600}", format!("{steps:?}"));
                assert_eq!(
                    emitted.iter().map(|e| e.0.as_str()).collect::<Vec<_>>(),
                    want,
                    "{at}: one signal per matching mail, in arrival order"
                );
                for (id, source, content) in &emitted {
                    assert_eq!(source, sub.signal_source, "{at}");
                    let m = mails.iter().find(|x| x.id == *id).unwrap();
                    for a in &m.attachments {
                        assert_eq!(
                            content.contains(&a.attachment_id),
                            a.mime_type == "application/pdf",
                            "{at}: {id} lists its PDFs, and only them ({})",
                            a.filename
                        );
                    }
                }
            });
            Ok(())
        })
        .unwrap();
    assert_eq!(
        witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(),
        ["a burst of more than one page", "a poll fails midway", "a subject with a NUL"]
    );
}
