// Live-trend monitor + patch bookkeeping (Phase 3, part 3), all pure. The gate can
// be fooled (it optimizes the judge's score on a frozen cluster); PROD is the
// ground truth. After a patch ships, the cron worker gathers the target axis's
// scores on traces that ran AFTER the ship and asks `decide_revert` whether the
// live trend held above the pre-ship baseline. If it confidently fell, the
// worker removes the lesson. The IO (reading post-ship judgements, deleting the
// file) lives in the worker; this module just does the stats + string surgery.

use std::sync::LazyLock;

use regex::Regex;

pub fn mean(xs: &[f64]) -> Option<f64> {
    if xs.is_empty() { None } else { Some(xs.iter().sum::<f64>() / xs.len() as f64) }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Baseline {
    pub mean: f64,
    pub n: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevertDecision {
    Insufficient,
    Keep,
    Revert,
}

impl RevertDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            RevertDecision::Insufficient => "insufficient",
            RevertDecision::Keep => "keep",
            RevertDecision::Revert => "revert",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RevertVerdict {
    pub decision: RevertDecision,
    pub post_mean: Option<f64>,
    pub post_n: usize,
    pub baseline_mean: f64,
    // The two-sample noise band the drop had to clear to count as a real regression.
    pub margin: Option<f64>,
}

// Decide whether a shipped patch's live trend regressed below its pre-ship
// baseline. Conservative by construction: we only revert on a CONFIDENT drop
// (baseline − post beyond k·σ·√(1/postN + 1/baselineN), the two-sample noise
// band), never on a flat trend — a patch that passed the gate and merely fails
// to help in prod is harmless and stays. `min_n` guards against reverting on a
// handful of post-ship traces; below it we keep watching (Insufficient).
pub fn decide_revert(
    baseline: Baseline,
    post_scores: &[f64],
    sigma: Option<f64>,
    k: f64,
    min_n: usize,
) -> RevertVerdict {
    let post_n = post_scores.len();
    let mut verdict = RevertVerdict {
        decision: RevertDecision::Insufficient,
        post_mean: mean(post_scores),
        post_n,
        baseline_mean: baseline.mean,
        margin: None,
    };
    if post_n < min_n {
        return verdict;
    }
    let Some(post_mean) = mean(post_scores) else { return verdict };
    let margin = match sigma {
        None => 0.0,
        Some(sigma) => k * sigma * (1.0 / post_n as f64 + 1.0 / baseline.n.max(1.0)).sqrt(),
    };
    let drop = baseline.mean - post_mean;
    verdict.margin = Some(margin);
    verdict.decision = if drop > margin { RevertDecision::Revert } else { RevertDecision::Keep };
    verdict
}

// ─── patch lessons (append-only blocks separated by a blank line) ────

static BLANK_LINE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\n\s*\n").unwrap());

// Split a .patch.md into its appended lesson blocks (the unit the improver
// joins with a blank line). Empty/whitespace input → no lessons.
pub fn split_lessons(patch: &str) -> Vec<String> {
    BLANK_LINE.split(patch).map(str::trim).filter(|b| !b.is_empty()).map(str::to_owned).collect()
}

pub fn count_lessons(patch: &str) -> usize {
    split_lessons(patch).len()
}

// True when adding one more lesson would exceed the per-skill budget — the guard
// against the giant-prompt regression as lessons accumulate.
pub fn budget_exceeded(patch: &str, max_lessons: usize) -> bool {
    count_lessons(patch) >= max_lessons
}

// Surgically remove ONE lesson (the auto-revert of a single bad ship), leaving
// the other lessons intact. Returns the rebuilt patch ("" if nothing remains —
// the caller deletes the file then). Matching is on trimmed text.
pub fn remove_lesson(patch: &str, lesson: &str) -> String {
    let want = lesson.trim();
    let kept: Vec<String> = split_lessons(patch).into_iter().filter(|b| b != want).collect();
    if kept.is_empty() { String::new() } else { format!("{}\n", kept.join("\n\n")) }
}
