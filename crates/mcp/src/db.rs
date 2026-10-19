// MCP-owned state: the `mcp_state` database in the same Postgres cluster as
// the news store. OAuth tokens and the userbot session, the signal queue,
// scheduled tasks, settings, the Telegram chat log and poller cursors.
// (It used to be the sqlite file `tokens.db`; `import-sqlite-state` copies
// one over.)
//
// Sections:
//   1. handle  — `Db`, a cloneable, injected pool on `mcp_state`
//   2. locate  — which database, and creating it on first boot
//   3. schema  — versioned migrations under an advisory lock
//   4. wake-ups — LISTEN for other instances' inserts
//   5. format  — timestamps as the agent has always seen them
//   6. import  — one-shot copy of the old sqlite `tokens.db`
//
// A separate database rather than tables next to news_items: different
// lifecycle (small, hot, transactional vs. large and append-mostly), and it
// can be backed up or moved on its own.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::Serialize;
use tokio::sync::Notify;
use tokio_postgres::{AsyncMessage, Config, NoTls};
use tokio_util::sync::CancellationToken;

use crate::pg::{self, PgPool};

// ── 1. handle ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
    // Kept for the LISTEN connection, which can't be a pooled one.
    config: Config,
}

pub type Client = deadpool_postgres::Object;

impl Db {
    // Creates the database if missing and migrates it.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let config = state_config(database_url)?;
        ensure_database(database_url, &config).await?;
        Self::open(config).await
    }

    async fn open(config: Config) -> anyhow::Result<Self> {
        let db = Self { pool: pg::connect_config(config.clone())?, config };
        let client = db.client().await?;
        pg::lock(&client, STATE_LOCK).await?;
        let result = migrate(&client).await;
        pg::unlock(&client, STATE_LOCK).await?;
        result?;
        Ok(db)
    }

    pub async fn client(&self) -> anyhow::Result<Client> {
        Ok(self.pool.get().await?)
    }

    // The state database at an explicit URL, created if missing — for a
    // test that puts the news store and the state of one server in a fresh
    // pair of databases.
    pub async fn connect_state(database_url: &str, state_database_url: &str) -> anyhow::Result<Self> {
        let config: Config = state_database_url.parse()?;
        ensure_database(database_url, &config).await?;
        Self::open(config).await
    }

    // A fresh schema per test inside the test database (TEST_DATABASE_URL or
    // a container, see pg::test_pool), selected through search_path, so
    // parallel tests never see each other's rows. None (the test returns
    // early) without Docker or a named database.
    #[cfg(test)]
    pub async fn test() -> Option<Self> {
        let url = pg::test_database_url()?;
        let schema = format!("t_{:016x}", rand::random::<u64>());
        let admin = pg::connect(&url).expect("TEST_DATABASE_URL must parse");
        admin.get().await.unwrap().batch_execute(&format!("CREATE SCHEMA {schema}")).await.unwrap();
        let mut config: Config = url.parse().unwrap();
        config.options(format!("-c search_path={schema}"));
        Some(Self::open(config).await.expect("state migrations apply"))
    }
}

// ── 2. locate ────────────────────────────────────────────────────────────────

const DEFAULT_STATE_DB: &str = "mcp_state";

// STATE_DATABASE_URL wins; otherwise DATABASE_URL with the database name
// swapped, so compose needs no extra secret.
fn state_config(database_url: &str) -> anyhow::Result<Config> {
    if let Some(url) = std::env::var("STATE_DATABASE_URL").ok().filter(|v| !v.is_empty()) {
        return Ok(url.parse()?);
    }
    let mut config: Config = database_url.parse()?;
    config.dbname(DEFAULT_STATE_DB);
    Ok(config)
}

// CREATE DATABASE through the news database's connection. Both MCP
// instances may try at once; the loser's duplicate_database is success.
async fn ensure_database(admin_url: &str, state: &Config) -> anyhow::Result<()> {
    let Some(name) = state.get_dbname().map(str::to_owned) else { return Ok(()) };
    let admin = pg::connect(admin_url)?;
    let client = admin.get().await?;
    if client.query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&name]).await?.is_some() {
        return Ok(());
    }
    let quoted = format!("\"{}\"", name.replace('"', "\"\""));
    match client.batch_execute(&format!("CREATE DATABASE {quoted}")).await {
        Ok(()) => {
            tracing::info!(database = name, "created state database");
            Ok(())
        }
        // The loser of a race to create it: duplicate_database — or the
        // unique violation on pg_database's name index, which is what Postgres
        // raises instead when the two CREATEs overlap.
        Err(err)
            if err.code() == Some(&tokio_postgres::error::SqlState::DUPLICATE_DATABASE)
                || err.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) =>
        {
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

// ── 3. schema ────────────────────────────────────────────────────────────────

const STATE_LOCK: i64 = 0x006d_6370_5f73_7461; // "mcp_sta"

// Append-only: a change is a new entry, never an edit to an applied one.
const MIGRATIONS: &[(i32, &str)] = &[
    (
        1,
        r#"
-- OAuth / session credentials per integration: Gmail tokens (expires_at is
-- an ISO string, as googleapis wrote it), the userbot's MTProto session.
CREATE TABLE integration_account (
  provider      text NOT NULL,
  account_key   text NOT NULL,
  access_token  text,
  refresh_token text,
  expires_at    text,
  metadata      text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, account_key)
);

-- Every message the bot sees or sends: role 'user' incoming, 'assistant'
-- outgoing. thread_id is the forum topic (NULL for non-topic / General).
CREATE TABLE telegram_messages (
  id            bigserial PRIMARY KEY,
  chat_id       bigint NOT NULL,
  tg_message_id bigint,
  thread_id     bigint,
  role          text NOT NULL CHECK (role IN ('user', 'assistant')),
  text          text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX telegram_messages_chat_id_id ON telegram_messages (chat_id, id);
CREATE INDEX telegram_messages_chat_thread_id ON telegram_messages (chat_id, thread_id, id);

-- Poller cursors: Telegram's last_update_id, Gmail's per-subscription
-- watermarks.
CREATE TABLE telegram_kv (key text PRIMARY KEY, value text NOT NULL);
CREATE TABLE gmail_kv (key text PRIMARY KEY, value text NOT NULL);

-- User-facing settings: `timezone` (IANA), the system-task seed flag.
CREATE TABLE settings (
  key        text PRIMARY KEY,
  value      text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Cron-driven tasks in the user's timezone. One-shots retire once
-- last_run_at (unix seconds of the slot fired for) is set; cancel = DELETE.
-- source NULL fires as `scheduler`.
CREATE TABLE scheduled_tasks (
  id          bigserial PRIMARY KEY,
  cron_expr   text NOT NULL,
  recurring   boolean NOT NULL,
  prompt      text NOT NULL,
  source      text,
  last_run_at bigint,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX scheduled_tasks_pending ON scheduled_tasks (recurring, last_run_at);

-- The signal queue: pollers enqueue, the agent pops via get_next_signal.
CREATE TABLE signals (
  id          bigserial PRIMARY KEY,
  source      text NOT NULL,
  content     text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  consumed_at timestamptz
);
CREATE INDEX signals_pending ON signals (id) WHERE consumed_at IS NULL;
"#,
    ),
    (
        2,
        r#"
-- Conversations with other agents (peer.rs): a peer's message is 'in', ouro's
-- answer 'out'. actor is the instance that wrote the row (MCP_MEMORY_ACTOR);
-- sender is the name a peer gave itself, a label only.
CREATE TABLE peer_threads (
  id         bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE peer_messages (
  id         bigserial PRIMARY KEY,
  thread_id  bigint NOT NULL REFERENCES peer_threads (id),
  direction  text NOT NULL CHECK (direction IN ('in', 'out')),
  actor      text NOT NULL,
  sender     text,
  text       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX peer_messages_thread_id ON peer_messages (thread_id, id);

-- An insert notifies the channel named after its table, so a waiter in
-- another instance wakes at once (section 4).
CREATE FUNCTION notify_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify(TG_TABLE_NAME, '');
  RETURN NULL;
END $$;
CREATE TRIGGER signals_notify AFTER INSERT ON signals
  FOR EACH STATEMENT EXECUTE FUNCTION notify_insert();
CREATE TRIGGER peer_messages_notify AFTER INSERT ON peer_messages
  FOR EACH STATEMENT EXECUTE FUNCTION notify_insert();
"#,
    ),
];

async fn migrate(client: &Client) -> anyhow::Result<()> {
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS state_migrations (
               version    integer PRIMARY KEY,
               applied_at timestamptz NOT NULL DEFAULT now()
             )",
        )
        .await?;
    let current: Option<i32> = client.query_one("SELECT max(version) FROM state_migrations", &[]).await?.get(0);
    for (version, sql) in MIGRATIONS.iter().filter(|(v, _)| current.is_none_or(|c| *v > c)) {
        client.batch_execute("BEGIN").await?;
        let applied = async {
            client.batch_execute(sql).await?;
            client.execute("INSERT INTO state_migrations (version) VALUES ($1)", &[version]).await?;
            anyhow::Ok(())
        }
        .await;
        match applied {
            Ok(()) => client.batch_execute("COMMIT").await?,
            Err(err) => {
                client.batch_execute("ROLLBACK").await?;
                return Err(err.context(format!("state migration {version}")));
            }
        }
        tracing::info!(version, "state migration applied");
    }
    Ok(())
}

// ── 4. wake-ups ──────────────────────────────────────────────────────────────

// A waiter here (the supervisor's wait_for_signal, a peer's read_replies) must
// wake on a row another instance inserted — the tunnel's message, the
// supervisor's reply — which an in-process Notify never hears. The insert
// triggers NOTIFY the table's channel; this LISTENs on one dedicated
// connection and wakes the matching Notify. A lost connection is reopened, and
// every waiter is woken when it is: a notification sent while nobody listened
// is gone, and a woken waiter re-checks its table anyway.
impl Db {
    pub async fn listen(self, channels: Vec<(&'static str, Arc<Notify>)>, cancel: CancellationToken) {
        while !cancel.is_cancelled() {
            if let Err(err) = self.listen_once(&channels, &cancel).await {
                tracing::warn!(error = %format!("{err:#}"), "state LISTEN connection lost");
            }
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    async fn listen_once(
        &self,
        channels: &[(&'static str, Arc<Notify>)],
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let (client, mut connection) = self.config.connect(NoTls).await?;
        // The connection only makes progress while polled, so the LISTEN runs
        // alongside the loop that drains it.
        let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        let sql: String = channels.iter().map(|(name, _)| format!("LISTEN {name};")).collect();
        let listen = client.batch_execute(&sql);
        tokio::pin!(listen);
        let mut listening = false;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                done = &mut listen, if !listening => {
                    done?;
                    listening = true;
                    channels.iter().for_each(|(_, notify)| notify.notify_waiters());
                }
                message = messages.next() => match message {
                    Some(Ok(AsyncMessage::Notification(n))) => {
                        if let Some((_, notify)) = channels.iter().find(|(name, _)| *name == n.channel()) {
                            notify.notify_waiters();
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                    None => anyhow::bail!("connection closed"),
                },
            }
        }
    }
}

// ── 5. format ────────────────────────────────────────────────────────────────

// "2026-10-02 09:00:00", UTC — sqlite's datetime('now'), which is what
// created_at / consumed_at have always looked like to the agent.
pub fn sql_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S").to_string()
}

// ── 6. import ────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct ImportReport {
    pub integration_accounts: usize,
    pub telegram_messages: usize,
    pub telegram_kv: usize,
    pub gmail_kv: usize,
    pub settings: usize,
    pub scheduled_tasks: usize,
    pub signals: usize,
}

// sqlite's datetime('now') text, read as UTC.
fn sqlite_time(raw: Option<String>) -> Option<DateTime<Utc>> {
    raw.as_deref().and_then(crate::time::parse_js_date)
}

fn has_table(conn: &rusqlite::Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1", [table], |r| {
        r.get::<_, i64>(0)
    })
    .map(|n| n > 0)
}

type Row = Vec<rusqlite::types::Value>;

fn read_all(conn: &rusqlite::Connection, table: &str, columns: &str) -> anyhow::Result<Vec<Row>> {
    if !has_table(conn, table)? {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(&format!("SELECT {columns} FROM {table} ORDER BY rowid"))?;
    let width = stmt.column_count();
    let rows = stmt.query_map([], |r| (0..width).map(|i| r.get::<_, rusqlite::types::Value>(i)).collect())?;
    Ok(rows.collect::<Result<_, _>>()?)
}

fn text(v: &rusqlite::types::Value) -> Option<String> {
    match v {
        rusqlite::types::Value::Text(s) => Some(s.clone()),
        rusqlite::types::Value::Integer(i) => Some(i.to_string()),
        rusqlite::types::Value::Real(f) => Some(f.to_string()),
        _ => None,
    }
}

fn int(v: &rusqlite::types::Value) -> Option<i64> {
    match v {
        rusqlite::types::Value::Integer(i) => Some(*i),
        rusqlite::types::Value::Text(s) => s.parse().ok(),
        _ => None,
    }
}

// Copies the live tables of the TS-era sqlite file into a FRESH state
// database, keeping ids (cancel_scheduled_task takes them; signal ids are in
// traces). Refuses when the target already holds signals, messages or
// accounts, so a second run cannot duplicate anything. The legacy tables no
// code reads any more (news_kv, dreaming_kv, news_digest_kv, channel_posts)
// stay behind in the file.
pub async fn import_sqlite(db: &Db, sqlite: &Path) -> anyhow::Result<ImportReport> {
    let (accounts, messages, tg_kv, gm_kv, settings, tasks, signals) = {
        let conn = rusqlite::Connection::open_with_flags(sqlite, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        (
            read_all(
                &conn,
                "integration_account",
                "provider, account_key, access_token, refresh_token, expires_at, metadata, created_at, updated_at",
            )?,
            read_all(&conn, "telegram_messages", "id, chat_id, tg_message_id, thread_id, role, text, created_at")?,
            read_all(&conn, "telegram_kv", "key, value")?,
            read_all(&conn, "gmail_kv", "key, value")?,
            read_all(&conn, "settings", "key, value, updated_at")?,
            read_all(&conn, "scheduled_tasks", "id, cron_expr, recurring, prompt, source, last_run_at, created_at")?,
            read_all(&conn, "signals", "id, source, content, created_at, consumed_at")?,
        )
    };

    let mut client = db.client().await?;
    let busy: i64 = client
        .query_one(
            "SELECT (SELECT count(*) FROM signals) + (SELECT count(*) FROM telegram_messages)
                  + (SELECT count(*) FROM integration_account)",
            &[],
        )
        .await?
        .get(0);
    anyhow::ensure!(busy == 0, "the state database already holds data — import only runs into a fresh one");

    let tx = client.transaction().await?;
    let mut report = ImportReport::default();
    for r in &accounts {
        tx.execute(
            "INSERT INTO integration_account (provider, account_key, access_token, refresh_token, expires_at, metadata, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), COALESCE($8, now()))",
            &[&text(&r[0]), &text(&r[1]), &text(&r[2]), &text(&r[3]), &text(&r[4]), &text(&r[5]), &sqlite_time(text(&r[6])), &sqlite_time(text(&r[7]))],
        )
        .await?;
        report.integration_accounts += 1;
    }
    for r in &messages {
        tx.execute(
            "INSERT INTO telegram_messages (id, chat_id, tg_message_id, thread_id, role, text, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()))",
            &[
                &int(&r[0]),
                &int(&r[1]),
                &int(&r[2]),
                &int(&r[3]),
                &text(&r[4]),
                &text(&r[5]).unwrap_or_default(),
                &sqlite_time(text(&r[6])),
            ],
        )
        .await?;
        report.telegram_messages += 1;
    }
    for (table, rows, count) in
        [("telegram_kv", &tg_kv, &mut report.telegram_kv), ("gmail_kv", &gm_kv, &mut report.gmail_kv)]
    {
        for r in rows {
            tx.execute(
                &format!("INSERT INTO {table} (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"),
                &[&text(&r[0]), &text(&r[1])],
            )
            .await?;
            *count += 1;
        }
    }
    for r in &settings {
        tx.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES ($1, $2, COALESCE($3, now()))
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
            &[&text(&r[0]), &text(&r[1]), &sqlite_time(text(&r[2]))],
        )
        .await?;
        report.settings += 1;
    }
    // Tasks an older release seeded here on connect give way to the file's
    // — including any the user cancelled or edited.
    if !tasks.is_empty() {
        tx.execute("DELETE FROM scheduled_tasks", &[]).await?;
    }
    for r in &tasks {
        tx.execute(
            "INSERT INTO scheduled_tasks (id, cron_expr, recurring, prompt, source, last_run_at, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()))",
            &[
                &int(&r[0]),
                &text(&r[1]),
                &(int(&r[2]) == Some(1)),
                &text(&r[3]),
                &text(&r[4]),
                &int(&r[5]),
                &sqlite_time(text(&r[6])),
            ],
        )
        .await?;
        report.scheduled_tasks += 1;
    }
    for r in &signals {
        tx.execute(
            "INSERT INTO signals (id, source, content, created_at, consumed_at) VALUES ($1, $2, $3, COALESCE($4, now()), $5)",
            &[&int(&r[0]), &text(&r[1]), &text(&r[2]), &sqlite_time(text(&r[3])), &sqlite_time(text(&r[4]))],
        )
        .await?;
        report.signals += 1;
    }
    // Explicit ids leave the sequences behind; the next insert must not
    // collide with an imported row.
    for table in ["telegram_messages", "scheduled_tasks", "signals"] {
        tx.execute(
            &format!(
                "SELECT setval(pg_get_serial_sequence('{table}', 'id'), GREATEST((SELECT max(id) FROM {table}), 1))"
            ),
            &[],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn imports_a_ts_era_sqlite_file_once() {
        let Some(db) = Db::test().await else { return };
        let path = std::env::temp_dir().join(format!("legacy-{}.db", rand::random::<u64>()));
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE integration_account (provider TEXT, account_key TEXT, access_token TEXT, refresh_token TEXT,
                   expires_at TEXT, metadata TEXT, created_at TEXT, updated_at TEXT);
                 INSERT INTO integration_account VALUES ('gmail', 'me@x', 'at', 'rt', '2026-10-02T10:00:00.000Z', NULL,
                   '2026-06-01 10:00:00', '2026-06-01 10:00:00');
                 CREATE TABLE telegram_messages (id INTEGER PRIMARY KEY, chat_id INTEGER, tg_message_id INTEGER,
                   thread_id INTEGER, role TEXT, text TEXT, created_at TEXT);
                 INSERT INTO telegram_messages VALUES (41, 7, 100, NULL, 'user', 'привет', '2026-10-01 09:00:00');
                 CREATE TABLE telegram_kv (key TEXT PRIMARY KEY, value TEXT);
                 INSERT INTO telegram_kv VALUES ('last_update_id', '555');
                 CREATE TABLE gmail_kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);
                 INSERT INTO settings VALUES ('timezone', 'Europe/Kiev', '2026-06-01 10:00:00'),
                                             ('system.seeded_default_tasks', '1', '2026-06-01 10:00:00');
                 CREATE TABLE scheduled_tasks (id INTEGER PRIMARY KEY, cron_expr TEXT, recurring INTEGER, prompt TEXT,
                   source TEXT, last_run_at INTEGER, created_at TEXT);
                 INSERT INTO scheduled_tasks VALUES (2, '0 8 * * *', 1, 'tech digest', NULL, 1780000000, '2026-06-01 10:00:00'),
                                                    (9, '30 14 2 10 *', 0, 'take pills', NULL, NULL, '2026-10-01 10:00:00');
                 CREATE TABLE signals (id INTEGER PRIMARY KEY, source TEXT, content TEXT, created_at TEXT, consumed_at TEXT);
                 INSERT INTO signals VALUES (300, 'telegram', 'old', '2026-10-01 09:00:00', '2026-10-01 09:00:05'),
                                            (301, 'gmail', 'pending', '2026-10-02 09:00:00', NULL);
                 CREATE TABLE news_kv (key TEXT PRIMARY KEY, value TEXT);",
            )
            .unwrap();
        }
        let report = import_sqlite(&db, &path).await.unwrap();
        assert_eq!(
            report,
            ImportReport {
                integration_accounts: 1,
                telegram_messages: 1,
                telegram_kv: 1,
                gmail_kv: 0,
                settings: 2,
                scheduled_tasks: 2,
                signals: 2
            }
        );
        // A second run is refused instead of duplicating.
        assert!(import_sqlite(&db, &path).await.is_err());
        std::fs::remove_file(&path).unwrap();

        let signals = crate::signals::Signals::new(db.clone());
        let popped = signals.pop_next().await.unwrap().unwrap();
        assert_eq!((popped.id, popped.created_at.as_str()), (301, "2026-10-02 09:00:00"));
        // Sequences moved past the imported ids.
        assert_eq!(signals.record("x", "y").await.unwrap(), 302);
        let tasks = crate::scheduler::Scheduler::new(db.clone()).list_active().await.unwrap();
        assert_eq!(tasks.iter().map(|t| t.id).collect::<Vec<_>>(), [2, 9]);
        assert_eq!(crate::settings::Settings::new(db.clone()).timezone().await, chrono_tz::Europe::Kiev);
    }

    #[test]
    fn derives_the_state_database_from_database_url() {
        let config = state_config("postgres://u:p@postgres:5432/mcp").unwrap();
        assert_eq!(config.get_dbname(), Some("mcp_state"));
        assert_eq!(config.get_user(), Some("u"));
    }

    #[test]
    fn formats_times_like_sqlite() {
        let t = DateTime::parse_from_rfc3339("2026-10-02T09:00:00.123Z").unwrap().with_timezone(&Utc);
        assert_eq!(sql_time(t), "2026-10-02 09:00:00");
    }
}
