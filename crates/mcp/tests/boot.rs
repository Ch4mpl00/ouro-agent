// Booting against Postgres when several MCP instances start at once (mcp and
// mcp-tunnel do, on every deploy): the state database and the news store are
// created, migrated and seeded exactly once, every instance comes up, and a
// later boot changes nothing. A port of packages/mcp/test/boot.test.ts.

mod support;

use std::collections::HashSet;
use std::sync::OnceLock;

use mcp_tools::db::Db;
use mcp_tools::pg;
use proptest::test_runner::{Config, TestRunner};
use support::{TestDatabase, runtime};

fn with_database(server_url: &str, name: &str) -> String {
    let mut url = url::Url::parse(server_url).unwrap();
    url.set_path(&format!("/{name}"));
    url.to_string()
}

fn env() -> &'static TestDatabase {
    static ENV: OnceLock<TestDatabase> = OnceLock::new();
    ENV.get_or_init(|| runtime().block_on(support::test_database()))
}

// Every case makes a database; a failed case leaves its own to the sweep of
// `t<secs>_` names a day later (support/container.rs).
async fn drop_database(admin: &str, name: &str) {
    let client = pg::connect(admin).unwrap().get().await.unwrap();
    let _ = client.batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)")).await;
}

async fn count(url: &str, sql: &str) -> i64 {
    pg::connect(url).unwrap().get().await.unwrap().query_one(sql, &[]).await.unwrap().get(0)
}

#[test]
fn the_state_database_is_created_and_migrated_once_and_every_instance_comes_up() {
    let db = env();
    TestRunner::new(Config::with_cases(12))
        .run(&(2usize..=6), |instances| {
            runtime().block_on(async {
                let state_name = format!("{}_state", support::container::database_name());
                let state_url = with_database(&db.url, &state_name);
                let booted =
                    futures::future::join_all((0..instances).map(|_| Db::connect_state(&db.url, &state_url))).await;
                let failures: Vec<String> =
                    booted.iter().filter_map(|b| b.as_ref().err().map(|e| format!("{e:#}"))).collect();
                assert_eq!(failures, Vec::<String>::new(), "{instances} instances booting at once");

                let versions: Vec<i64> = pg::connect(&state_url)
                    .unwrap()
                    .get()
                    .await
                    .unwrap()
                    .query("SELECT version::bigint FROM state_migrations ORDER BY version", &[])
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| r.get(0))
                    .collect();
                assert_eq!(
                    versions.len(),
                    versions.iter().collect::<HashSet<_>>().len(),
                    "each migration recorded once"
                );

                // A later boot finds everything in place and changes nothing.
                Db::connect_state(&db.url, &state_url).await.unwrap();
                assert_eq!(
                    count(&state_url, "SELECT count(*) FROM state_migrations").await,
                    versions.len() as i64,
                    "a reboot migrates nothing again"
                );
                // The agent owns the schedule now; the state database seeds no tasks.
                assert_eq!(count(&state_url, "SELECT count(*) FROM scheduled_tasks").await, 0);
                drop_database(&db.url, &state_name).await;
            });
            Ok(())
        })
        .unwrap();
}

#[test]
fn the_news_store_migrates_once_with_several_instances_racing() {
    let db = env();
    TestRunner::new(Config::with_cases(8))
        .run(&(2usize..=5), |instances| {
            runtime().block_on(async {
                let name = format!("{}_news", support::container::database_name());
                pg::connect(&db.url)
                    .unwrap()
                    .get()
                    .await
                    .unwrap()
                    .batch_execute(&format!("CREATE DATABASE {name}"))
                    .await
                    .unwrap();
                let url = with_database(&db.url, &name);
                let pools: Vec<pg::PgPool> = (0..instances).map(|_| pg::connect(&url).unwrap()).collect();
                let booted = futures::future::join_all(pools.iter().map(pg::migrate)).await;
                let failures: Vec<String> =
                    booted.iter().filter_map(|b| b.as_ref().err().map(|e| format!("{e:#}"))).collect();
                assert_eq!(failures, Vec::<String>::new(), "{instances} instances");
                let hashes: Vec<String> = pools[0]
                    .get()
                    .await
                    .unwrap()
                    .query("SELECT hash FROM drizzle.__drizzle_migrations", &[])
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| r.get(0))
                    .collect();
                assert_eq!(hashes.len(), hashes.iter().collect::<HashSet<_>>().len(), "each migration recorded once");
                drop(pools);
                drop_database(&db.url, &name).await;
            });
            Ok(())
        })
        .unwrap();
}
