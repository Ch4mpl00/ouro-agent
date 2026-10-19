// The agent validates every skill against the MCP registry at startup and
// refuses to start on a mismatch — on the droplet that is a crash loop. This
// checks the same thing before a deploy: the real server binary, booted the
// way prod's `mcp` instance is (every toolset), listed through the agent's own
// MCP client, and the agent's own `validate_all` over the skills that ship in
// skills.default. A port of test/shipped-skills.test.ts.
//
// Gateway tools (`prefix__tool`) can't be listed offline; a skill may declare
// one only under the prefix of an upstream the shipped gateway.config.json
// enables. Whether that upstream is reachable at boot is F15.

mod support;

use std::path::Path;
use std::process::{Command, Stdio};

use agent::mcp::{ConnectOptions, Mcp, McpClient, open_http};
use agent::skills::{FsSkillStore, SkillStore, SkillTools};
use serde_json::Value;

#[test]
fn all_shipped_skills_pass_the_agents_startup_validation_against_prods_mcp_surface() {
    support::runtime().block_on(async {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let db = support::test_database().await;
        let dir = tempfile::tempdir().unwrap();
        // Own tools only: the gateway's upstreams are checked by prefix below.
        std::fs::write(dir.path().join("gateway.config.json"), r#"{"upstreams":[]}"#).unwrap();
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let mut child = Command::new(env!("CARGO_BIN_EXE_mcp-tools"))
            .current_dir(dir.path())
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", std::env::var("HOME").unwrap_or_default())
            .env("MCP_TRANSPORT", "http")
            .env("MCP_PORT", port.to_string())
            .env("MCP_NO_POLLERS", "1")
            .env("DATABASE_URL", &db.url)
            .env("STATE_DATABASE_URL", &db.state_url)
            .env("GATEWAY_CONFIG", dir.path().join("gateway.config.json"))
            // Required at boot; nothing here embeds.
            .env("OPENAI_API_KEY", "sk-test-unused")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the binary starts");
        let url = format!("http://127.0.0.1:{port}/mcp");
        let mut up = false;
        for _ in 0..1200 {
            if reqwest::Client::new().post(&url).send().await.is_ok() {
                up = true;
                break;
            }
            if let Ok(Some(status)) = child.try_wait() {
                panic!("MCP exited ({status}) before listening");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(up, "MCP did not start");

        // Connected the way the agent container is: over HTTP.
        let mcp = McpClient::connect(open_http(url), ConnectOptions::default()).await.unwrap();
        let own: Vec<String> = mcp.tools().into_iter().map(|t| t.name).collect();
        assert!(own.iter().any(|t| t == "get_next_signal"), "the full instance serves the signal queue");
        // The read-only marks a saved workflow relies on: a call to one of
        // these never counts as a change. A new tool decides which it is here.
        let mut read_only: Vec<&str> = own.iter().map(String::as_str).filter(|t| mcp.read_only(t)).collect();
        read_only.sort();
        assert_eq!(
            read_only,
            [
                "doc_history",
                "download_gmail_attachment",
                "fetch_article",
                "fetch_url",
                "find_notes",
                "find_skills",
                "get_fact",
                "get_peer_thread",
                "get_telegram_chat_history",
                "get_timezone",
                "journal_tail",
                "list_memory",
                "list_monobank_transactions",
                "list_nashdom_mails",
                "list_news",
                "list_scheduled_tasks",
                "list_signals",
                "list_userbot_dialogs",
                "read_doc",
                "read_file",
                "read_pdf",
                "recall",
                "search_news",
                "wait_for_signal",
            ]
        );
        let gateway: Value =
            serde_json::from_str(&std::fs::read_to_string(repo.join("crates/mcp/gateway.config.json")).unwrap())
                .unwrap();
        let prefixes: Vec<String> = gateway["upstreams"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|u| u["enabled"] != false)
            .map(|u| u["prefix"].as_str().or(u["name"].as_str()).unwrap_or_default().to_owned())
            .collect();

        let store =
            FsSkillStore { live_dir: dir.path().join("no-live-skills"), defaults_dir: repo.join("skills.default") };
        let skills = store.list_skills().await.unwrap();
        assert!(skills.len() > 5, "skills.default is found");
        // What the gateway would add: every declared prefix__tool whose prefix
        // is an enabled upstream.
        let mut declared = Vec::new();
        for s in &skills {
            if let Some(file) = store.read_skill(&s.name).await.unwrap()
                && let SkillTools::Named(tools) = file.tools
            {
                declared.extend(tools);
            }
        }
        let via_gateway: Vec<String> =
            declared.into_iter().filter(|t| prefixes.iter().any(|p| t.starts_with(&format!("{p}__")))).collect();
        let known: Vec<String> = own.into_iter().chain(via_gateway).collect();
        let verdict = store.validate_all(&known).await;
        mcp.close().await;
        let _ = Command::new("kill").arg("-TERM").arg(child.id().to_string()).status();
        let _ = child.wait();
        db.drop_all().await;
        verdict.unwrap();
    });
}
