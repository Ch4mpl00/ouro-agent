// The skills contract between the two crates. The agent loads skills
// (crates/agent/src/skills.rs) and the MCP server exports them to readers
// (src/skills.rs) — two implementations of one layout, which the crates may
// not share code for. This test runs them side by side on the same generated
// skill directories and demands they agree: which skills exist and in which
// layer, what tools each declares, and the instructions in force (body +
// improver patch). A port of test/skills-contract.test.ts.

use std::collections::BTreeMap;

use agent::skills::{FsSkillStore, SkillSource as AgentSource, SkillStore, SkillTools, append_patch};
use mcp_tools::skills::{SkillCatalog, SkillSource as McpSource, Tools};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

mod support;

const NAMES: [&str; 5] = ["telegram", "news-digest", "a_b", "planner", "x1"];
const FRONTMATTER: [&str; 9] = [
    "---\ntools: *\n---\n",
    "---\r\ntools: [send_telegram_message, recall]\r\n---\r\n",
    "---\ntools: []\n---\n",
    "---\ntools:*\n---\n",
    "---\ntools:   *  \n---\n",
    "---\ntools: [ a ,b ]\n---\n",
    "---\ntitle: X\ntools: [a, b]\n---\n",
    "",                              // no frontmatter: the agent refuses the skill
    "---\ntools: [Bad-Name]\n---\n", // the agent refuses an invalid tool name
];
const BODY: [&str; 5] = [
    "# Title\n\nDo the thing.\n",
    "# Title\r\n\r\nWindows lines.\r\n",
    "body with --- inside\n---\nmore\n",
    "trailing space   \n\n\n",
    "",
];
const PATCH: [Option<&str>; 5] = [None, Some(""), Some("   \n"), Some("Also do X.\n"), Some("\n\nCRLF fix\r\n")];

#[derive(Clone, Debug)]
struct SkillFiles {
    live: Option<(usize, usize)>,
    def: Option<(usize, usize)>,
    patch: Option<&'static str>,
}

fn file() -> impl Strategy<Value = (usize, usize)> {
    (0..FRONTMATTER.len(), 0..BODY.len())
}

fn layout() -> impl Strategy<Value = BTreeMap<&'static str, SkillFiles>> {
    let files = (proptest::option::of(file()), proptest::option::of(file()), proptest::sample::select(PATCH.to_vec()))
        .prop_map(|(live, def, patch)| SkillFiles { live, def, patch });
    proptest::collection::btree_map(proptest::sample::select(NAMES.to_vec()), files, 1..=NAMES.len())
}

fn text((frontmatter, body): (usize, usize)) -> String {
    format!("{}{}", FRONTMATTER[frontmatter], BODY[body])
}

fn tool_ok(t: &str) -> bool {
    regex::Regex::new(r"^[a-z_][a-z0-9_]*$").unwrap().is_match(t)
}

async fn check(skills: &BTreeMap<&'static str, SkillFiles>) -> Result<(), String> {
    let root = tempfile::tempdir().unwrap();
    let live_dir = root.path().join("live");
    let defaults_dir = root.path().join("defaults");
    std::fs::create_dir(&live_dir).unwrap();
    std::fs::create_dir(&defaults_dir).unwrap();
    for (name, s) in skills {
        if let Some(f) = s.live {
            std::fs::write(live_dir.join(format!("{name}.md")), text(f)).unwrap();
        }
        if let Some(f) = s.def {
            std::fs::write(defaults_dir.join(format!("{name}.md")), text(f)).unwrap();
        }
        if let Some(p) = s.patch {
            std::fs::write(live_dir.join(format!("{name}.patch.md")), p).unwrap();
        }
    }
    let agent = FsSkillStore { live_dir: live_dir.clone(), defaults_dir: defaults_dir.clone() };
    let mcp = SkillCatalog::new(live_dir, defaults_dir);
    let at = format!("{skills:?}");

    let agent_list: Vec<(String, &str)> =
        agent.list_skills().await.unwrap().into_iter().map(|e| (e.name, e.source.as_str())).collect();
    let mcp_list: Vec<(String, &str)> = mcp
        .list_skills()
        .await
        .unwrap()
        .into_iter()
        .map(|e| (e.name, if e.source == McpSource::Live { "live" } else { "default" }))
        .collect();
    if agent_list != mcp_list {
        return Err(format!("{at}: the same skills, from the same layer: {agent_list:?} vs {mcp_list:?}"));
    }
    let exists: Vec<&str> =
        skills.iter().filter(|(_, s)| s.live.is_some() || s.def.is_some()).map(|(n, _)| *n).collect();
    let listed: Vec<&str> = agent_list.iter().map(|(n, _)| n.as_str()).collect();
    if listed != exists {
        return Err(format!("{at}: exactly the skill files, never a patch: {listed:?}"));
    }

    for name in exists {
        let exported = mcp.read_skill(&format!("{name}.md")).await.unwrap();
        let Some(exported) = exported else { return Err(format!("{name}: listed but not readable through MCP")) };
        // The agent refuses broken frontmatter; nothing runs.
        let loaded = agent.read_skill(name).await.ok().flatten();
        // A skill the export reads as well-formed (tools parsed, names valid)
        // is one the agent must load: otherwise the export shows a skill that
        // never runs.
        let well_formed = match &exported.summary.tools {
            Some(Tools::All(_)) => true,
            Some(Tools::List(tools)) => tools.iter().all(|t| tool_ok(t)),
            None => false,
        };
        if loaded.is_some() != well_formed {
            return Err(format!("{at}: {name} loads iff the export reads it as well-formed"));
        }
        let Some(loaded) = loaded else { continue };
        let same_layer = matches!(
            (exported.summary.source, loaded.source),
            (McpSource::Live, AgentSource::Live) | (McpSource::Default, AgentSource::Default)
        );
        if !same_layer {
            return Err(format!("{at}: {name} layer"));
        }
        let same_tools = match (&exported.summary.tools, &loaded.tools) {
            (Some(Tools::All(_)), SkillTools::All) => true,
            (Some(Tools::List(a)), SkillTools::Named(b)) => a == b,
            _ => false,
        };
        if !same_tools {
            return Err(format!("{at}: {name} tools"));
        }
        let in_force = append_patch(&loaded.body, agent.read_patch(name).await.unwrap().as_deref().unwrap_or(""));
        if exported.effective_instructions != in_force {
            return Err(format!("{at}: {name} instructions in force"));
        }
        let hash = agent::skills::hash_value(
            &serde_json::json!({"content":in_force,"tools":loaded.tools.to_json(),"routable":loaded.routable}),
        );
        if exported.content_hash().unwrap() != hash {
            return Err(format!("{at}: {name} snapshot hash"));
        }
        if exported.summary.patched != (in_force != loaded.body) {
            return Err(format!("{at}: {name} patched flag"));
        }
    }
    Ok(())
}

#[test]
fn the_agents_loader_and_the_mcp_export_agree_on_skills_layers_tools_and_instructions() {
    let mut runner =
        TestRunner::new(Config { cases: support::property_runs(150), failure_persistence: None, ..Config::default() });
    runner.run(&layout(), |skills| support::runtime().block_on(check(&skills)).map_err(TestCaseError::fail)).unwrap();
}
