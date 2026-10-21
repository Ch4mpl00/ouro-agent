// Named model presets: a model id with its reasoning effort. Sessions pick a
// preset by name — "base reply", "smart digest" — rather than configuring the
// two separately. Provider routing (DeepSeek / Gemini / OpenRouter / OpenAI)
// follows the model name, see Engine::resolve_provider.

use std::fmt;

// "low" exists for the Gemini compiler (its default dynamic budget is ~12 s
// per plan, "low" ~2.8 s); "medium" is the DeepSeek latency knob. Only the
// DeepSeek, Gemini and OpenRouter providers send it; the OpenAI provider
// never does, so for an OpenAI-routed preset it is documentation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReasoningEffort {
    Disabled,
    Low,
    Medium,
    High,
    Max,
}

impl ReasoningEffort {
    pub fn as_str(self) -> &'static str {
        match self {
            ReasoningEffort::Disabled => "disabled",
            ReasoningEffort::Low => "low",
            ReasoningEffort::Medium => "medium",
            ReasoningEffort::High => "high",
            ReasoningEffort::Max => "max",
        }
    }
}

impl fmt::Display for ReasoningEffort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelPreset {
    pub model: String,
    pub reasoning_effort: ReasoningEffort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PresetName {
    Base,
    Smart,
    Smartest,
    Compiler,
    Compose,
}

impl PresetName {
    pub fn as_str(self) -> &'static str {
        match self {
            PresetName::Base => "base",
            PresetName::Smart => "smart",
            PresetName::Smartest => "smartest",
            PresetName::Compiler => "compiler",
            PresetName::Compose => "compose",
        }
    }

    // A preset a workflow step or a sub-agent may pick; never `compiler`.
    pub fn selectable(name: &str) -> Option<PresetName> {
        match name {
            "base" => Some(PresetName::Base),
            "smart" => Some(PresetName::Smart),
            "smartest" => Some(PresetName::Smartest),
            _ => None,
        }
    }

    // An `llm_compose` step: the selectable presets, plus `compose` (the
    // default) for a model that answers without tools.
    pub fn for_compose(name: &str) -> Option<PresetName> {
        if name == "compose" { Some(PresetName::Compose) } else { PresetName::selectable(name) }
    }
}

impl fmt::Display for PresetName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Selectable inside a workflow (llm_compose / llm_agent `preset`) or by a
// sub-agent. `compiler` is the compiler's own model, never chosen there.
pub const PRESET_NAMES: [&str; 3] = ["base", "smart", "smartest"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Presets {
    pub base: ModelPreset,
    pub smart: ModelPreset,
    pub smartest: ModelPreset,
    pub compiler: ModelPreset,
    pub compose: ModelPreset,
}

impl Presets {
    pub fn get(&self, name: PresetName) -> &ModelPreset {
        match name {
            PresetName::Base => &self.base,
            PresetName::Smart => &self.smart,
            PresetName::Smartest => &self.smartest,
            PresetName::Compiler => &self.compiler,
            PresetName::Compose => &self.compose,
        }
    }
}

fn preset(model: &str, reasoning_effort: ReasoningEffort) -> ModelPreset {
    ModelPreset { model: model.to_owned(), reasoning_effort }
}

// Applied at startup when the AGENT_*_MODEL env overrides are absent.
//   base     — non-thinking chat: recovery, simple workers, the
//              workflow-path primary loop, the activity journal.
//   smart    — the primary Telegram loop and editorial work.
//   smartest — a reserve high-end preset, selectable by sub-agents.
//   compiler — the workflow compiler: one structured plan per signal.
//   compose  — `llm_compose` steps by default: one generation, no tools.
// DeepSeek V4.1 Flash (via OpenRouter) is the workhorse with tools. gpt-6.1-sol writes
// text only: its Chat Completions endpoint takes no tool calls, so it never
// backs a loop.
const WORKHORSE: &str = "openrouter/deepseek/deepseek-v4.1-flash";
pub fn default_presets() -> Presets {
    Presets {
        base: preset(WORKHORSE, ReasoningEffort::Disabled),
        smart: preset(WORKHORSE, ReasoningEffort::High),
        smartest: preset(WORKHORSE, ReasoningEffort::Max),
        compiler: preset(WORKHORSE, ReasoningEffort::High),
        compose: preset("gpt-6.1-sol", ReasoningEffort::Medium),
    }
}

// The typed-decision model (an OpenRouter id, without the "openrouter/"
// prefix — the decisions API is OpenRouter-only). AGENT_DECISION_MODEL
// overrides it.
pub const DEFAULT_DECISION_MODEL: &str = "typesafe/jev-1.13";
