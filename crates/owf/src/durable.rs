//! Storage contract for resumable execution. The host owns transactions,
//! leases and fencing; the interpreter owns the records' meaning.
//!
//! # Invariants
//!
//! The interpreter and Journal host jointly enforce the run/call contract.
//! Event waits use the same invocation identities and checkpoints.
//! `Effect` alone never determines
//! retry safety.
//!
//! ## Run identity and replay
//!
//! 1. A run has a stable ID and immutable definition, original input and
//!    runtime bindings. Persist them before executing its first step. State
//!    format and interpreter semantics are versioned; incompatible recovery
//!    fails explicitly instead of running the saved state under new semantics.
//! 2. Recovery replays the DSL from the beginning. A completed step restores
//!    its saved output, context and flow/error outcome without invoking its
//!    body again. No serialized Rust stack or arbitrary-code continuation is
//!    required. Unfinished parents replay their completed children.
//! 3. Every logical invocation has a stable identity: run ID, task reference
//!    and full invocation path, including nested loop iterations, `then` visits,
//!    reusable-function callers and fork branches. Recovery keeps that identity;
//!    a genuinely new invocation gets a different one. A DSL retry attempt is
//!    not a new invocation for a protected call (a `Write`, or any at-most-once
//!    call): its identity is the first attempt's (`Record::Call::logical`),
//!    taken from the attempts the interpreter is running, never parsed out of
//!    a path that holds author-chosen task names. A protected call an earlier
//!    attempt completed returns that saved result instead of being sent again;
//!    different function or arguments under the same identity are a conflict
//!    that fails the run without sending. A failed one is sent again under its
//!    delivery policy. Reads and model steps run afresh on every attempt.
//! 4. Function, arguments and delivery policy are bound to the invocation ID.
//!    A mismatch on replay is an error, never a cache miss or a new side effect.
//!    Decisions influenced by time, external data or concurrency must be saved
//!    or reproducible from saved inputs, including the winner of `compete`.
//!
//! ## Checkpoints and ownership
//!
//! 5. A checkpoint becomes visible to dependent steps only after durable commit.
//!    Its output, context and control outcome are committed atomically. A saved
//!    call result survives a crash before the enclosing task checkpoint, so
//!    finishing that task does not call the function again. Completed invocation
//!    results cannot be overwritten by pending state or a different result.
//! 6. Save/load failure, corrupt state or lost executor ownership stops progress;
//!    there is no fallback to an empty journal or non-durable execution. A save
//!    error may mean the commit succeeded: recover from storage before proceeding.
//! 7. Only the current executor may advance a run. Ownership checks and commits
//!    are atomic; a replaced executor cannot overwrite its successor's state.
//!    Storage fencing does not undo or fence an external request already sent.
//! 8. Store the current complete run state, not necessarily every historical
//!    snapshot. It may span SQLite rows, but retains every result, decision,
//!    pending operation and wait still needed for replay and deduplication.
//!    Deleting old snapshots must not delete that evidence.
//!
//! ## External calls
//!
//! 9. Delivery policy belongs to the agent's durable layer and applies to
//!    third-party MCP tools without requiring server changes. The DSL defaults
//!    to `at least once`; `at most once` is explicit and independent of the
//!    function's read/write classification. This is no external exactly-once
//!    guarantee and does not require a host-side idempotency guard.
//! 10. `At least once`: durably save the received result before using it. Without
//!     a saved result, recovery may resend the call, so effects can be duplicated.
//!     A tool error response is also a result: save it and apply DSL error/retry
//!     policy rather than treating it as success or as an absent response.
//! 11. `At most once`: durably save intent before any possible dispatch, then
//!     save the response. Intent without a saved response means outcome unknown
//!     on recovery; never resend it. A crash between intent and dispatch may
//!     therefore lose the execution. HTTP 200 is not a universal durable ACK.
//! 12. An at-most-once invocation may be retried only with proof of no execution:
//!     it was not sent, or the server definitively rejected it before execution.
//!     The adapter must distinguish `not_executed` from `outcome_unknown`;
//!     timeout, disconnect after sending or a tool error alone are not such proof.
//!     Evidence allowing a retry must itself survive recovery.
//! 13. Unknown at-most-once outcomes prohibit resending at EVERY layer: restart
//!     recovery, DSL retries, host wrappers and transport retries. Allocating a
//!     new attempt ID cannot bypass this prohibition. Resuming an existing
//!     response stream is distinct from issuing a new call.
//! 14. Unknown is not successful completion and has no fabricated output.
//!     Surface it explicitly for error handling/reconciliation; dependent steps
//!     requiring the missing result cannot proceed. A host may instead name an
//!     explicit marker for it (`Functions::unknown_outcome`), saved as the
//!     call's result for a reader that reconciles it (a model told the call
//!     may have run); the call is still never sent again. Cancellation, a timeout or
//!     dropping a losing fork branch does not prove an in-flight effect stopped.
//!
//! ## Waiting and completion
//!
//! 15. Persist absolute deadlines, retry progress and wait correlation before
//!     releasing the worker. Restart does not reset timers, attempts or limits;
//!     waiting needs no live Rust future or model session. Persist a `compete`
//!     winner before exposing its result; replay cannot select a different one.
//! 16. An event is durably accepted by the agent before its upstream ACK. Event,
//!     timeout and cancellation race through one atomic wait resolution;
//!     duplicate events cannot resolve it twice. Early events remain available
//!     for matching. Terminal run states are durable and cannot be reopened by
//!     recovery or late events; cancellation does not undo external effects.
//! 17. Taking an event and saving its `Record::Event` are one fenced atomic
//!     commit in the event adapter. A crash before the task checkpoint restores
//!     the same event, even after its deadline; it never consumes another one.
//!     Each loop iteration has a distinct event key. Registration is idempotent;
//!     terminal runs cannot receive events. A host must preserve a delivery's
//!     wake-up when the executor concurrently releases its worker as waiting.
use crate::{
    model::Flow,
    run::{Effect, WorkflowError},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    #[default]
    AtLeastOnce,
    AtMostOnce,
}

/// The host must classify failures conservatively: an ordinary transport
/// error is Unknown, not proof that dispatch never happened.
#[derive(Debug)]
pub enum CallFailure {
    NotExecuted(WorkflowError),
    Response(WorkflowError),
    Unknown(WorkflowError),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CallState {
    Pending,
    NotExecuted(WorkflowError),
    Done(Result<Value, WorkflowError>),
}

/// All methods must reject an expired/replaced executor. A successful save
/// must be durable before it returns, including before an external call.
/// Load returns one consistent snapshot of this run; save replaces one record
/// atomically and must enforce `check_transition` against persisted state.
pub trait Journal: Send + Sync {
    fn load(&self) -> anyhow::Result<BTreeMap<String, Record>>;
    fn save(&self, key: &str, record: &Record) -> anyhow::Result<()>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Execution {
        version: u32,
        run_id: String,
        definition: Value,
        input: Value,
        runtime: Value,
        started_ms: i64,
        max_tasks: usize,
        max_attempts: u32,
    },
    Task {
        started_ms: i64,
        result: Option<TaskResult>,
    },
    Call {
        /// The invocation's identity across DSL retry attempts: its reference
        /// with every enclosing attempt replaced by that `try`'s first one.
        /// Loop iterations, `then` visits, callers and branches stay distinct.
        logical: String,
        function: String,
        arguments: Value,
        effect: Effect,
        delivery: Delivery,
        state: CallState,
    },
    Event {
        event: Value,
    },
    Timer {
        deadline_ms: i64,
    },
    Retry {
        delay_ms: Option<u64>,
    },
    Winner {
        output: Value,
    },
    Finished {
        status: crate::run::Status,
    },
}

/// Only pending tasks/calls may advance. Everything else is immutable.
pub fn check_transition(old: &Record, new: &Record) -> anyhow::Result<()> {
    let allowed = old == new
        || match (old, new) {
            (Record::Task { started_ms: a, result: None }, Record::Task { started_ms: b, .. }) => a == b,
            (
                Record::Call { logical: l, function: a, arguments: b, effect: c, delivery: d, state },
                Record::Call { logical: m, function: e, arguments: f, effect: g, delivery: h, .. },
            ) => !matches!(state, CallState::Done(_)) && l == m && a == e && b == f && c == g && d == h,
            _ => false,
        };
    anyhow::ensure!(allowed, "invalid durable record transition");
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskResult {
    pub context: Value,
    pub outcome: StepOutcome,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StepOutcome {
    Completed { output: Value, flow: Flow },
    Faulted { error: WorkflowError, fatal: bool },
}
