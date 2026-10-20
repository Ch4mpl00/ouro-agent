// download_gmail_attachment over MCP against the fake Gmail and the real state
// database, then read_file on the storage root — the way the bill flow uses
// them. A port of packages/mcp/test/gmail-attachments.test.ts. The ids and the
// filename come from the model, so they are generated hostile as often as
// not; sometimes the fake Gmail answers any attachment path at all, so the
// tool cannot lean on Gmail rejecting a malformed id. Whatever is asked: every
// file the tool writes sits in a directory named exactly after its message,
// inside the account's directory, and holds the bytes Gmail sent for it;
// nothing lands anywhere else.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use mcp_tools::fs::FileAccess;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;
use support::fake_gmail::{self, FakeAttachment, FakeGmail, FakeMail, attachment_bytes};
use support::{
    Client, Outcome, Overrides, Server, TestDatabase, call, connect, property_runs, runtime, start_test_server_with,
};

const ACCOUNT: &str = "me@example.com";
const MAILS: [(&str, &[&str]); 2] =
    [("19e0794763036c82", &["ANGjdJcn2zNfQ1", "ANGjdJ8BO3C4Q2"]), ("19df30756e0b1641", &["ANGjdJloZQ6bQ3"])];

fn mails() -> Vec<FakeMail> {
    MAILS
        .iter()
        .map(|(id, atts)| FakeMail {
            id: (*id).into(),
            internal_date: 1_790_000_000_000,
            subject: "Bill".into(),
            matches: true,
            attachments: atts
                .iter()
                .enumerate()
                .map(|(i, a)| FakeAttachment {
                    attachment_id: (*a).into(),
                    filename: format!("bill-{i}.pdf"),
                    mime_type: "application/pdf".into(),
                    size: 100,
                    content: None,
                    nested: false,
                })
                .collect(),
        })
        .collect()
}

const HOSTILE: [&str; 11] =
    ["..", ".", "../..", "../outside", "a/b", "a\\b", "", "%2e%2e", "x\u{0}y", "/abs", "..\\.."];

#[derive(Debug, Clone)]
struct Download {
    message_id: String,
    attachment_id: String,
    filename: Option<&'static str>,
}

fn download() -> impl Strategy<Value = Download> {
    let real_mail = proptest::sample::select(MAILS.iter().map(|m| m.0).collect::<Vec<_>>()).prop_map(str::to_owned);
    let real_attachment = proptest::sample::select(MAILS.iter().flat_map(|m| m.1.iter().copied()).collect::<Vec<_>>())
        .prop_map(str::to_owned);
    let hostile = || proptest::sample::select(HOSTILE.to_vec()).prop_map(str::to_owned);
    let filename = proptest::option::weighted(
        5.0 / 6.0,
        proptest::sample::select(vec![
            "bill.pdf",
            "may_bill_ДГ.pdf",
            "../../evil.pdf",
            "..",
            ".",
            "/etc/passwd",
            "a\\b.pdf",
            "",
            " ",
            "a/../../b.pdf",
        ]),
    );
    (prop_oneof![2 => real_mail, 2 => hostile()], prop_oneof![3 => real_attachment, 1 => hostile()], filename)
        .prop_map(|(message_id, attachment_id, filename)| Download { message_id, attachment_id, filename })
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else { return out };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(files_under(&p));
        } else {
            out.push(p);
        }
    }
    out
}

struct Env {
    _db: TestDatabase,
    fake: FakeGmail,
    base: PathBuf,
    storage: PathBuf,
    _server: Server,
    client: Client,
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        runtime().block_on(async {
            let db = support::test_database().await;
            let fake = fake_gmail::start().await;
            // base/storage is the storage root; base/outside is where an
            // escape lands.
            let base = std::env::temp_dir().join(format!("gmail-attachments-{:016x}", rand::random::<u64>()));
            std::fs::create_dir_all(&base).unwrap();
            let base = std::fs::canonicalize(base).unwrap();
            let storage = base.join("storage");
            let overrides = Overrides {
                gmail_api_root: Some(fake.root_url.clone()),
                storage_dir: Some(storage.clone()),
                files: Some(FileAccess::new(std::slice::from_ref(&storage), &base)),
                ..Overrides::default()
            };
            let server = start_test_server_with(&db, Some("gmail,fs"), overrides).await.expect("server starts");
            let expires =
                (chrono::Utc::now() + chrono::Duration::hours(24)).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            server
                .db
                .client()
                .await
                .unwrap()
                .execute(
                    "INSERT INTO integration_account (provider, account_key, access_token, refresh_token, expires_at)
                     VALUES ('gmail', $1, 'test-token', 'test-refresh', $2)",
                    &[&ACCOUNT, &expires],
                )
                .await
                .unwrap();
            let client = connect(&server.url).await;
            Env { _db: db, fake, base, storage, _server: server, client }
        })
    })
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn writes_only_inside_the_messages_own_directory_the_bytes_gmail_sent_readable_back() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    env.fake.lock().mails = mails();
    let account_dir = env.storage.join("gmail").join(ACCOUNT);
    let witnessed = Mutex::new(BTreeSet::new());
    TestRunner::new(Config::with_cases(120))
        .run(&(proptest::collection::vec(download(), 1..=8), any::<bool>()), |(downloads, lenient)| {
            runtime().block_on(async {
                let _ = std::fs::remove_dir_all(&env.base);
                std::fs::create_dir_all(&env.base).unwrap();
                env.fake.lock().lenient = lenient;
                // The bytes each written path must hold: the last download to it.
                let mut want: BTreeMap<PathBuf, String> = BTreeMap::new();
                for d in &downloads {
                    let at = format!("{d:?} lenient={lenient}");
                    let mut args = json!({ "messageId": d.message_id, "attachmentId": d.attachment_id });
                    if let Some(f) = d.filename {
                        args["filename"] = json!(f);
                    }
                    let out = call(&env.client, "download_gmail_attachment", args).await;
                    let known =
                        MAILS.iter().any(|(id, atts)| *id == d.message_id && atts.contains(&d.attachment_id.as_str()));
                    let Outcome::Ok(value) = out else {
                        // A refusal: a tool error, or invalid params before the tool.
                        assert!(!known, "{at}: a real attachment downloads ({out:?})");
                        witnessed.lock().unwrap().insert("a bad id refused");
                        continue;
                    };
                    let file_path = PathBuf::from(value["filePath"].as_str().expect("filePath"));
                    assert_eq!(
                        file_path.parent(),
                        Some(account_dir.join(&d.message_id).as_path()),
                        "{at}: in the message's own directory"
                    );
                    assert_eq!(
                        file_path.parent().and_then(Path::file_name).and_then(|n| n.to_str()),
                        Some(d.message_id.as_str()),
                        "{at}: named exactly after the message"
                    );
                    let bytes = if known {
                        attachment_bytes(&d.attachment_id, None)
                    } else {
                        format!("%PDF lenient {}/{}", d.message_id, d.attachment_id)
                    };
                    want.insert(file_path, bytes);
                    witnessed.lock().unwrap().insert(if known {
                        "a real attachment lands"
                    } else {
                        "a lenient Gmail answers a bad id"
                    });
                }
                // Nothing anywhere but the files the tool reported, each holding
                // its latest bytes and readable through read_file.
                let mut found = files_under(&env.base);
                found.sort();
                assert_eq!(found, want.keys().cloned().collect::<Vec<_>>(), "{downloads:?}: no stray files");
                for (file_path, bytes) in &want {
                    assert_eq!(std::fs::read_to_string(file_path).unwrap(), *bytes);
                    let read = call(&env.client, "read_file", json!({ "path": file_path })).await;
                    let Outcome::Ok(v) = &read else { panic!("read_file {}: {read:?}", file_path.display()) };
                    assert_eq!(v["content"], json!(bytes));
                }
            });
            Ok(())
        })
        .unwrap();
    let seen = witnessed.into_inner().unwrap();
    for w in ["a bad id refused", "a real attachment lands", "a lenient Gmail answers a bad id"] {
        assert!(seen.contains(w), "{w}");
    }
}

// Paging through list_nashdom_mails with nextPageToken: every matching mail
// exactly once, newest first, nothing else; each listing exactly its PDFs —
// by type or by a .pdf name in any case — wherever in the MIME tree they sit.
#[test]
fn pages_through_every_nashdom_mail_once_newest_first_with_exactly_its_pdfs() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    let attachment =
        (proptest::sample::select(vec!["pdf", "octet-pdf-name", "upper-name", "image", "doc"]), any::<bool>());
    let mail = (prop_oneof![3 => Just(true), 1 => Just(false)], proptest::collection::vec(attachment, 0..=3));
    let witnessed = Mutex::new(BTreeSet::new());
    TestRunner::new(Config::with_cases(property_runs(40)))
        .run(&(proptest::collection::vec(mail, 0..=30), 1u32..=12), |(spec, limit)| {
            runtime().block_on(async {
                let t0 = 1_790_000_000_000i64;
                let made: Vec<FakeMail> = spec
                    .iter()
                    .enumerate()
                    .map(|(i, (matches, atts))| FakeMail {
                        id: format!("m{i:04x}"),
                        internal_date: t0 + i as i64 * 60_000,
                        subject: format!("Bill {i}"),
                        matches: *matches,
                        attachments: atts
                            .iter()
                            .enumerate()
                            .map(|(j, (kind, nested))| {
                                let (filename, mime) = match *kind {
                                    "pdf" => (format!("bill-{i}-{j}.pdf"), "application/pdf"),
                                    "octet-pdf-name" => (format!("bill-{i}-{j}.pdf"), "application/octet-stream"),
                                    "upper-name" => (format!("BILL-{i}-{j}.PDF"), "application/octet-stream"),
                                    "image" => (format!("logo-{i}-{j}.png"), "image/png"),
                                    _ => (
                                        format!("terms-{i}-{j}.docx"),
                                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                    ),
                                };
                                FakeAttachment {
                                    attachment_id: format!("ANGjdJ{i}x{j}"),
                                    filename,
                                    mime_type: mime.into(),
                                    size: 100,
                                    content: None,
                                    nested: *nested,
                                }
                            })
                            .collect(),
                    })
                    .collect();
                env.fake.lock().mails = made.clone();
                let mut seen: Vec<(String, Vec<String>)> = Vec::new();
                let mut token: Option<String> = None;
                for _ in 0..100 {
                    let mut args = json!({ "limit": limit });
                    if let Some(t) = &token {
                        args["pageToken"] = json!(t);
                    }
                    let out = call(&env.client, "list_nashdom_mails", args).await;
                    let Outcome::Ok(value) = out else { panic!("{out:?}") };
                    let messages = value["messages"].as_array().expect("messages");
                    assert!(messages.len() <= limit as usize, "a page holds at most `limit`");
                    for m in messages {
                        let mut atts: Vec<String> = m["attachments"]
                            .as_array()
                            .map(|a| {
                                a.iter().map(|x| x["attachmentId"].as_str().unwrap_or_default().to_owned()).collect()
                            })
                            .unwrap_or_default();
                        atts.sort();
                        seen.push((m["messageId"].as_str().unwrap_or_default().to_owned(), atts));
                    }
                    match value["nextPageToken"].as_str() {
                        Some(next) if !next.is_empty() => {
                            token = Some(next.to_owned());
                            witnessed.lock().unwrap().insert("more than one page");
                        }
                        _ => break,
                    }
                }
                let mut matching: Vec<&FakeMail> = made.iter().filter(|m| m.matches).collect();
                matching.sort_by_key(|m| std::cmp::Reverse(m.internal_date));
                let want: Vec<(String, Vec<String>)> = matching
                    .iter()
                    .map(|m| {
                        let mut atts: Vec<String> = m
                            .attachments
                            .iter()
                            .filter(|a| a.mime_type == "application/pdf" || a.filename.to_lowercase().ends_with(".pdf"))
                            .map(|a| a.attachment_id.clone())
                            .collect();
                        atts.sort();
                        (m.id.clone(), atts)
                    })
                    .collect();
                if made
                    .iter()
                    .any(|m| m.attachments.iter().any(|a| a.nested && a.filename.to_lowercase().ends_with(".pdf")))
                {
                    witnessed.lock().unwrap().insert("a nested PDF");
                }
                assert_eq!(seen, want, "{spec:?} limit={limit}");
            });
            Ok(())
        })
        .unwrap();
    assert_eq!(witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(), ["a nested PDF", "more than one page"]);
}
