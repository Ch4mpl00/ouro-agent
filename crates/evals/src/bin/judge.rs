// Judge an agent run per node from the terminal.
//
//   judge [--provider openai] [--dump] <traceId>
//   judge [--provider openai] [--dump] --recent [N]
//
// --dump writes the per-node judge materials (no LLM call) for the in-session
// judge-trace skill to /tmp/judge-dump-<traceId>.md.
use agent::db::AgentDb;
use agent::skills::FsSkillStore;
use evals::backend::{JudgeBackend, JudgeProvider, judge_backend};
use evals::env::{db_path, load_env, root};
use evals::langfuse_api::Langfuse;
use evals::materials::{NodeMaterial, assemble_node_materials};
use evals::node_judge::judge_node;
use evals::print::{NodeHeader, node_summary_line, print_node_judgement, print_trace_header};
use evals::schema::rubric_for;
use evals::source::{LocalSource, TraceSource};
use regex::Regex;
use std::sync::Arc;

struct Cli {
    dump: bool,
    provider: JudgeProvider,
    args: Vec<String>,
}

fn parse_args(argv: &[String]) -> anyhow::Result<Cli> {
    let mut cli = Cli { dump: false, provider: JudgeProvider::OpenAi, args: Vec::new() };
    let mut i = 0;
    while i < argv.len() {
        let arg = &argv[i];
        if arg == "--dump" {
            cli.dump = true;
        } else if arg == "--provider" {
            i += 1;
            cli.provider = JudgeProvider::parse(argv.get(i).map(String::as_str).unwrap_or(""))?;
        } else if let Some(v) = arg.strip_prefix("--provider=") {
            cli.provider = JudgeProvider::parse(v)?;
        } else {
            cli.args.push(arg.clone());
        }
        i += 1;
    }
    Ok(cli)
}

async fn judge_one(
    source: &dyn TraceSource,
    skills: &FsSkillStore,
    backend: &dyn JudgeBackend,
    provider: JudgeProvider,
    trace_id: &str,
) -> anyhow::Result<()> {
    let (_, nodes) = assemble_node_materials(source, skills, trace_id).await?;
    eprintln!("[judge] trace {trace_id} · provider={} · {} judgeable nodes", provider.as_str(), nodes.len());
    print_trace_header(trace_id);
    let mut summary = Vec::new();
    for node in &nodes {
        let header = NodeHeader { label: &node.label, kind: node.kind.as_str(), skill: &node.skill };
        let verdict = judge_node(backend, &node.judge_input(), false).await?;
        print_node_judgement(&header, &verdict);
        summary.push(node_summary_line(&header, &verdict));
    }
    if nodes.len() > 1 {
        println!("\n── summary ──");
        for line in summary {
            println!("{line}");
        }
    }
    Ok(())
}

// One block per node, each the exact user prompt that node's rubric would
// receive (contract + node IO), minus the trailing scoring instruction.
fn render_node_dump(node: &NodeMaterial, index: usize) -> String {
    let prompt = rubric_for(node.kind).build_user_prompt(
        &node.skill,
        node.contract.as_deref(),
        &node.input_text,
        &node.output_text,
    );
    let tail = Regex::new(r"\n\n(?:Score this|Extract F's)[\s\S]*$").unwrap();
    let materials = tail.replace(&prompt, "");
    format!("## NODE {} · {} · {} · skill {}\n\n{materials}", index + 1, node.label, node.kind.as_str(), node.skill)
}

async fn dump_one(source: &dyn TraceSource, skills: &FsSkillStore, trace_id: &str) -> anyhow::Result<()> {
    let (_, nodes) = assemble_node_materials(source, skills, trace_id).await?;
    let mut parts = vec![format!("# JUDGE MATERIALS · trace {trace_id} · {} nodes\n", nodes.len())];
    parts.extend(nodes.iter().enumerate().map(|(i, n)| render_node_dump(n, i)));
    parts.push(String::new());
    let body = parts.join("\n\n");
    let path = format!("/tmp/judge-dump-{trace_id}.md");
    std::fs::write(&path, &body)?;
    println!("[judge] dumped {trace_id} · {} nodes · {} chars → {path}", nodes.len(), body.encode_utf16().count());
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cli = parse_args(&argv)?;
    let root = root()?;
    let skills = FsSkillStore::new(&root);
    // Local mirror first (fast, and present when run on the droplet), Langfuse
    // as fallback for ids not mirrored locally. --recent lists from Langfuse
    // (recent prod traces).
    let langfuse = Langfuse::from_env()?;
    let source = LocalSource { db: AgentDb::open(db_path(&root))?, fallback: Some(Arc::new(langfuse.clone())) };
    let backend: Option<Arc<dyn JudgeBackend>> = if cli.dump { None } else { Some(judge_backend(cli.provider)?) };
    let run_one = async |trace_id: &str| -> anyhow::Result<()> {
        match &backend {
            None => dump_one(&source, &skills, trace_id).await,
            Some(b) => judge_one(&source, &skills, &**b, cli.provider, trace_id).await,
        }
    };

    if cli.args.first().map(String::as_str) == Some("--recent") {
        let n = cli.args.get(1).and_then(|v| v.parse::<u32>().ok()).filter(|n| *n > 0).unwrap_or(5);
        let recent = langfuse.recent_traces(n).await?;
        eprintln!("[judge] fetched {} recent traces", recent.len());
        for t in recent {
            println!("\n{}", "—".repeat(72));
            println!("trace {} · {} · [{}] · {}", t.id, t.name, t.tags.join(","), t.timestamp);
            if let Err(err) = run_one(&t.id).await {
                eprintln!("[judge] {} failed: {err:#}", t.id);
            }
        }
        return Ok(());
    }
    let Some(trace_id) = cli.args.first().filter(|a| !a.starts_with("--")) else {
        eprintln!(
            "usage: judge [--provider openai] [--dump] <traceId>  |  judge [--provider openai] [--dump] --recent [N]"
        );
        std::process::exit(1);
    };
    run_one(trace_id).await
}
