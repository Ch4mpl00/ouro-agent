// Named tool groups an instance can register (MCP_TOOLSETS). One MCP process
// serves one audience: the droplet supervisor gets everything, a third-party
// client (ChatGPT over the Secure MCP Tunnel) a hand-picked subset. Selection
// is by *registration*: an unselected tool never appears in tools/list, so it
// is invisible rather than merely forbidden
// (.claude/tasks/mcp-auth-and-tool-scoping.md, A2).
//
// Sections:
//   1. names     — the toolset vocabulary and the default surface
//   2. selection — parsing MCP_TOOLSETS
//   3. routers   — toolset → the domain file's tool router

use rmcp::handler::server::router::tool::ToolRouter;

use crate::server::McpTools;

// ── 1. names ─────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Toolset {
    Gmail,
    Telegram,
    // send_telegram_message only — no history, no edits, no chat actions.
    TelegramSend,
    Monobank,
    Pdf,
    Fs,
    // fetch_url — arbitrary page fetch behind an SSRF guard. Never handed to
    // third-party clients: it would turn this server into their proxy.
    Fetch,
    Signals,
    // search_news, list_news, fetch_article. Touches no personal account,
    // which is why it is safe to hand out.
    NewsRead,
    Knowledge,
    // Historical name: only list_signals.
    Dreaming,
    Userbot,
    Scheduler,
    Skills,
    // Unified memory — shared by every agent, safe to hand out.
    Memory,
}

impl Toolset {
    pub const ALL: [Toolset; 15] = [
        Toolset::Gmail,
        Toolset::Telegram,
        Toolset::TelegramSend,
        Toolset::Monobank,
        Toolset::Pdf,
        Toolset::Fs,
        Toolset::Fetch,
        Toolset::Signals,
        Toolset::NewsRead,
        Toolset::Knowledge,
        Toolset::Dreaming,
        Toolset::Userbot,
        Toolset::Scheduler,
        Toolset::Skills,
        Toolset::Memory,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Toolset::Gmail => "gmail",
            Toolset::Telegram => "telegram",
            Toolset::TelegramSend => "telegram-send",
            Toolset::Monobank => "monobank",
            Toolset::Pdf => "pdf",
            Toolset::Fs => "fs",
            Toolset::Fetch => "fetch",
            Toolset::Signals => "signals",
            Toolset::NewsRead => "news-read",
            Toolset::Knowledge => "knowledge",
            Toolset::Dreaming => "dreaming",
            Toolset::Userbot => "userbot",
            Toolset::Scheduler => "scheduler",
            Toolset::Skills => "skills",
            Toolset::Memory => "memory",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.name() == name)
    }
}

// The unrestricted surface, in the TS registration order. `telegram-send` is
// absent (a strict subset of `telegram`, it would double-register), and so is
// `skills`: the agent owns synthetic tools with those names, so the MCP export
// is scoped to the ChatGPT tunnel.
pub const DEFAULT_TOOLSETS: &[Toolset] = &[
    Toolset::Gmail,
    Toolset::Telegram,
    Toolset::Monobank,
    Toolset::Pdf,
    Toolset::Fs,
    Toolset::Fetch,
    Toolset::Signals,
    Toolset::NewsRead,
    Toolset::Knowledge,
    Toolset::Dreaming,
    Toolset::Userbot,
    Toolset::Scheduler,
    Toolset::Memory,
];

// ── 2. selection ─────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
pub struct ToolsetSelection {
    pub names: Vec<Toolset>,
    // True when MCP_TOOLSETS narrowed the surface. Decides things outside the
    // router: a restricted instance never attaches the gateway (namespaced
    // upstream tools can't be allow-listed) and allows several sessions.
    pub restricted: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("MCP_TOOLSETS: unknown toolset(s) {unknown}. Known: {known}")]
pub struct UnknownToolsets {
    unknown: String,
    known: String,
}

// Absent, empty or whitespace-only → the full default surface. An unknown
// name is a hard error: silently serving a different surface than intended is
// exactly what this feature exists to prevent.
pub fn parse_toolsets(raw: Option<&str>) -> Result<ToolsetSelection, UnknownToolsets> {
    let requested: Vec<&str> = raw.unwrap_or("").split(',').map(str::trim).filter(|n| !n.is_empty()).collect();
    if requested.is_empty() {
        return Ok(ToolsetSelection { names: DEFAULT_TOOLSETS.to_vec(), restricted: false });
    }

    let unknown: Vec<&str> = requested.iter().copied().filter(|n| Toolset::from_name(n).is_none()).collect();
    if !unknown.is_empty() {
        let mut known: Vec<&str> = Toolset::ALL.iter().map(|t| t.name()).collect();
        known.sort_unstable();
        return Err(UnknownToolsets { unknown: unknown.join(", "), known: known.join(", ") });
    }

    let mut names = Vec::new();
    for toolset in requested.into_iter().filter_map(Toolset::from_name) {
        if !names.contains(&toolset) {
            names.push(toolset);
        }
    }
    Ok(ToolsetSelection { names, restricted: true })
}

// ── 3. routers ───────────────────────────────────────────────────────────────

impl Toolset {
    fn router(self) -> ToolRouter<McpTools> {
        match self {
            Toolset::Gmail => McpTools::gmail_tools(),
            // The full telegram surface includes the send slice.
            Toolset::Telegram => McpTools::telegram_send_tools() + McpTools::telegram_tools(),
            Toolset::TelegramSend => McpTools::telegram_send_tools(),
            Toolset::Monobank => McpTools::monobank_tools(),
            Toolset::Pdf => McpTools::pdf_tools(),
            Toolset::Fs => McpTools::fs_tools(),
            Toolset::Fetch => McpTools::fetch_tools(),
            Toolset::Signals => McpTools::signals_tools(),
            Toolset::NewsRead => McpTools::news_tools(),
            Toolset::Knowledge => McpTools::knowledge_tools(),
            Toolset::Dreaming => McpTools::dreaming_tools(),
            Toolset::Userbot => McpTools::userbot_tools(),
            Toolset::Scheduler => McpTools::scheduler_tools(),
            Toolset::Skills => McpTools::skills_tools(),
            Toolset::Memory => McpTools::memory_tools(),
        }
    }
}

pub fn compose_router(names: &[Toolset]) -> ToolRouter<McpTools> {
    names.iter().fold(ToolRouter::new(), |acc, t| acc + t.router())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::Strategy;

    use super::*;

    fn tool_names(names: &[Toolset]) -> Vec<String> {
        let mut out: Vec<String> = compose_router(names).list_all().into_iter().map(|t| t.name.into_owned()).collect();
        out.sort();
        out
    }

    #[test]
    fn absent_or_blank_means_the_full_default_surface() {
        for raw in [None, Some(""), Some("   "), Some(",, ,")] {
            assert_eq!(
                parse_toolsets(raw).unwrap(),
                ToolsetSelection { names: DEFAULT_TOOLSETS.to_vec(), restricted: false }
            );
        }
    }

    #[test]
    fn parses_a_restricted_list_trimming_and_deduplicating() {
        assert_eq!(
            parse_toolsets(Some(" news-read , telegram-send ,news-read")).unwrap(),
            ToolsetSelection { names: vec![Toolset::NewsRead, Toolset::TelegramSend], restricted: true }
        );
    }

    #[test]
    fn rejects_an_unknown_toolset() {
        let err = parse_toolsets(Some("news-read,telegramm")).unwrap_err().to_string();
        assert!(err.contains("unknown toolset(s) telegramm"), "{err}");
    }

    // Pins docker-compose's MCP_TOOLSETS for mcp-tunnel exactly. If this list
    // and that env var drift apart, ChatGPT silently gets a surface nobody
    // decided on — in either direction.
    const TUNNEL: &[Toolset] = &[Toolset::NewsRead, Toolset::TelegramSend, Toolset::Skills, Toolset::Memory];

    #[test]
    fn exposes_exactly_the_tunnel_surface() {
        assert_eq!(
            tool_names(TUNNEL),
            [
                "append_doc",
                "create_project",
                "doc_history",
                "fetch_article",
                "get_fact",
                "journal_append",
                "journal_tail",
                "list_memory",
                "list_news",
                "list_skills",
                "patch_doc",
                "read_doc",
                "read_skill",
                "recall",
                "remember",
                "revert_patch",
                "search_news",
                "send_telegram_message",
                "update_fact",
                "write_doc",
            ]
        );
    }

    // Everything a third-party client can reach must be safe to hand out: no
    // personal account, no signal delivery, no arbitrary fetch.
    #[test]
    fn keeps_the_tunnel_clear_of_tools_that_must_never_leave_the_droplet() {
        let tunnel = tool_names(TUNNEL);
        for forbidden in [
            "get_next_signal",
            "wait_for_signal",
            "list_nashdom_mails",
            "list_monobank_transactions",
            "read_file",
            "fetch_url",
            "schedule_task",
            "get_telegram_chat_history",
        ] {
            assert!(!tunnel.iter().any(|t| t == forbidden), "{forbidden}");
        }
    }

    #[test]
    fn the_default_surface_matches_the_ts_server() {
        let all = tool_names(DEFAULT_TOOLSETS);
        assert_eq!(
            all,
            [
                "add_note",
                "append_doc",
                "create_project",
                "doc_history",
                "download_gmail_attachment",
                "edit_telegram_message",
                "fetch_article",
                "fetch_url",
                "find_notes",
                "get_fact",
                "get_next_signal",
                "get_telegram_chat_history",
                "get_timezone",
                "journal_append",
                "journal_tail",
                "list_memory",
                "list_monobank_transactions",
                "list_nashdom_mails",
                "list_news",
                "list_scheduled_tasks",
                "list_signals",
                "list_userbot_dialogs",
                "patch_doc",
                "read_doc",
                "read_file",
                "read_pdf",
                "recall",
                "remember",
                "revert_patch",
                "search_news",
                "send_telegram_chat_action",
                "send_telegram_message",
                "set_timezone",
                "start_typing",
                "telegram_send_status",
                "update_fact",
                "wait_for_signal",
                "write_doc",
            ]
        );
        // The agent owns synthetic tools with these names.
        assert!(!all.iter().any(|t| t == "list_skills" || t == "read_skill"));
    }

    #[test]
    fn keeps_shared_tunnel_capabilities_in_the_default_surface() {
        let all = tool_names(DEFAULT_TOOLSETS);
        let restricted = tool_names(&[Toolset::NewsRead, Toolset::TelegramSend, Toolset::Skills]);
        assert!(restricted.iter().filter(|n| *n != "list_skills" && *n != "read_skill").all(|n| all.contains(n)));
        assert!(!all.iter().any(|t| t == "list_skills" || t == "read_skill"));
        assert!(all.iter().any(|t| t == "get_next_signal"));
        assert!(all.iter().any(|t| t == "get_telegram_chat_history"));
    }

    // Every known group, read from the error that names them — so a group
    // added later is covered without touching this test.
    fn every_group() -> Vec<Toolset> {
        let err = parse_toolsets(Some("__none__")).unwrap_err().to_string();
        let known = err.split("Known: ").nth(1).expect("the error names the known groups");
        known.split(", ").map(|n| parse_toolsets(Some(n)).unwrap().names[0]).collect()
    }

    #[test]
    fn knows_every_group() {
        assert!(every_group().len() >= DEFAULT_TOOLSETS.len() + 2);
    }

    // Any MCP_TOOLSETS value an operator might write: a subset of the groups,
    // in any order, repeated, padded. The instance serves exactly the union of
    // what each chosen group serves on its own (toolsets.test.ts).
    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(200))]

        #[test]
        fn serves_exactly_the_union_of_the_chosen_groups(
            picked in proptest::sample::subsequence(every_group(), 1..=Toolset::ALL.len())
                .prop_flat_map(|names| {
                    let doubled: Vec<Toolset> = names.iter().chain(names.iter()).copied().collect();
                    let n = names.len();
                    proptest::sample::subsequence(doubled, n..=2 * n).prop_shuffle()
                }),
            pad in proptest::sample::select(vec!["", " ", "  "]),
        ) {
            use proptest::prelude::*;
            let raw: Vec<String> = picked.iter().map(|t| format!("{pad}{}{pad}", t.name())).collect();
            let raw = raw.join(",");
            let selection = parse_toolsets(Some(&raw)).unwrap();
            prop_assert!(selection.restricted);
            let mut first_mention = Vec::new();
            for t in &picked {
                if !first_mention.contains(t) {
                    first_mention.push(*t);
                }
            }
            prop_assert_eq!(&selection.names, &first_mention, "{}: each group once, first mention first", raw);
            let mut union: Vec<String> = selection.names.iter().flat_map(|t| tool_names(&[*t])).collect();
            union.sort();
            union.dedup();
            prop_assert_eq!(tool_names(&selection.names), union, "{}", raw);
        }
    }
}
