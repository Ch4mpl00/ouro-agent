// optimize <trace-id>... [--definition FILE] [--model MODEL] [--out FILE] [--dump]
// One proposal; no supervisor, MCP connection, workflow writes or activation.
use std::{path::PathBuf, sync::Arc};

use agent::models::{ModelPreset, ReasoningEffort, default_presets};
use agent::providers::{
    DEEPSEEK_BASE_URL, GEMINI_BASE_URL, HttpApi, OPENAI_BASE_URL, ProviderKind, RetryOpts, SharedProvider,
    is_openrouter_model, openrouter_api, provider, with_retry,
};
use agent::skills::{FsSkillStore, SkillStore};
use agent::tracing::null_scope;
use evals::env::{load_env, root};
use evals::langfuse_api::Langfuse;
use evals::optimizer::{compact_trace, prompt, proposal_yaml, propose};
use tokio_util::sync::CancellationToken;

const USAGE: &str = "optimize <trace-id>... [--definition FILE] [--model MODEL] [--out FILE] [--dump]\n\
    --dump  Print the assembled prompt without calling a model.\n\
    --out   Write the report (or dumped prompt) to this file as well as stdout.\n\
    Validates YAML syntax and OWF schema only; no dry run or activation.";

fn chat_provider(model: &str) -> anyhow::Result<SharedProvider> {
    let (kind, key_name, url) = if is_openrouter_model(model) {
        (ProviderKind::Openrouter, "OPEN_ROUTER_API_KEY", "")
    } else if model.starts_with("deepseek") {
        (ProviderKind::Deepseek, "DEEPSEEK_API_KEY", DEEPSEEK_BASE_URL)
    } else if model.starts_with("gemini") {
        (ProviderKind::Gemini, "GEMINI_API_KEY", GEMINI_BASE_URL)
    } else {
        (ProviderKind::Openai, "OPENAI_API_KEY", OPENAI_BASE_URL)
    };
    let key = std::env::var(key_name).ok().filter(|k| !k.is_empty());
    let key = key.ok_or_else(|| anyhow::anyhow!("{key_name} missing in env"))?;
    let api = if kind == ProviderKind::Openrouter {
        openrouter_api(&key)
    } else {
        let url = if kind == ProviderKind::Openai {
            std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| url.into())
        } else {
            url.into()
        };
        HttpApi::new(&url, &key)
    };
    Ok(Arc::new(with_retry(Arc::new(provider(kind, Arc::new(api))), RetryOpts::default())))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    let mut args = std::env::args().skip(1);
    let mut ids = Vec::new();
    let mut definition = None;
    let mut output = None;
    let mut dump = false;
    let mut model = std::env::var("AGENT_SMARTEST_MODEL").unwrap_or_else(|_| default_presets().smartest.model);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--definition" => {
                definition =
                    Some(PathBuf::from(args.next().ok_or_else(|| anyhow::anyhow!("--definition needs a file"))?));
            }
            "--out" => output = Some(PathBuf::from(args.next().ok_or_else(|| anyhow::anyhow!("--out needs a file"))?)),
            "--model" => model = args.next().ok_or_else(|| anyhow::anyhow!("--model needs a model"))?,
            "--dump" => dump = true,
            "--help" | "-h" => {
                println!("{USAGE}");
                return Ok(());
            }
            flag if flag.starts_with('-') => anyhow::bail!("unknown option: {flag}\n{USAGE}"),
            id => ids.push(id.to_owned()),
        }
    }
    anyhow::ensure!(!ids.is_empty(), "{USAGE}");
    let root = root()?;
    let definition_path = definition.unwrap_or_else(|| root.join("crates/agent/workflows/agent-loop.yaml"));
    let definition = std::fs::read_to_string(definition_path)?;
    owf::model::parse_yaml(&definition)?;
    let skills = FsSkillStore::new(&root);
    let author =
        skills.read_skill("workflow-author").await?.ok_or_else(|| anyhow::anyhow!("workflow-author missing"))?;
    let optimizer =
        skills.read_skill("workflow-optimizer").await?.ok_or_else(|| anyhow::anyhow!("workflow-optimizer missing"))?;
    let source = Langfuse::from_env()?;
    let cancel = CancellationToken::new();
    let operation = async {
        let mut traces = Vec::new();
        for id in ids {
            let (trace, observations) = source.trace_by_id(&id).await?;
            let compact = compact_trace(&trace, &observations);
            eprintln!("[optimize] {id}: {} observations; models={}", observations.len(), compact["models"]);
            traces.push(compact);
        }
        let prompt = prompt(&author.body, &optimizer.body, &definition, &traces)?;
        let report = if dump {
            format!("# System\n\n{}\n\n# User\n\n{}\n", prompt.system, prompt.user)
        } else {
            let preset = ModelPreset { model: model.clone(), reasoning_effort: ReasoningEffort::Max };
            eprintln!("[optimize] model={model}, effort=max; proposal only");
            let proposal = propose(&chat_provider(&model)?, &preset, &prompt, &null_scope(), cancel.clone()).await?;
            if let Some(usage) = &proposal.usage {
                eprintln!("[optimize] tokens: {} input, {} output, {} total", usage.input, usage.output, usage.total);
            }
            // Print/save even an invalid proposal so its failure is reviewable.
            proposal.report
        };
        println!("{report}");
        if let Some(path) = output {
            std::fs::write(path, &report)?;
        }
        if !dump {
            proposal_yaml(&report)?;
            eprintln!(
                "[optimize] YAML parser + OWF schema passed. Function contracts, guard structure and behaviour have not been verified."
            );
        }
        Ok(())
    };
    tokio::select! {
        result = operation => result,
        result = tokio::signal::ctrl_c() => {
            result?;
            cancel.cancel();
            anyhow::bail!("optimization cancelled");
        }
    }
}
