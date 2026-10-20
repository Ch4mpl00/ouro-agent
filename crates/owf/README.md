# Durable execution

`run::resume` replays the DSL against an injected `durable::Journal`.
Completed tasks restore output, context and control flow. External call
responses are checkpointed separately, so a crash during output processing
does not resend the call. Iterations, repeated `then` visits, function
callers, branches and retry attempts have distinct, reproducible identities.

The agent uses `agent::durable::RunStore` on agent.db. The supervisor admits
saved workflows with pinned definitions and inputs, executes them with
fenced leases and renews ownership while working; a new run is admitted
already claimed. Its recovery worker picks up due waits and expired leases,
up to four runs at once and alongside the supervisor's current signal.
Shutdown releases unfinished work for recovery; `RunStore::cancel` is
explicit, terminal cancellation. Any other interruption (a storage error,
state the interpreter rejects) is retried with backoff from 2 s and fails
the run after `MAX_INTERRUPTIONS` (10) in a row, so a cause that never
clears cannot retry forever. An interrupted run sends no failure report:
it is still recoverable.

## Delivery

```yaml
- send:
    call: send_telegram_message
    metadata: { delivery: at_most_once }
    with: { chatId: 123, text: '${ .message }' }
```

The default is `at_least_once`, independently of read/write hints. Without a
saved response, recovery can resend a call. A tool error response is saved
as an error and handled by the DSL.

`at_most_once` commits intent before dispatch. An interrupted invocation
without a saved response becomes `outcome_unknown`, a fatal workflow error;
neither catch/retry nor recovery can resend it. The crash window between
intent and dispatch can therefore lose execution. Retry is safe only when
the host proves non-execution and that evidence is durably saved.

Host adapters implement `Functions::invoke`, classifying failures as
`NotExecuted`, `Response` or `Unknown`. They must not internally retry
ambiguous dispatches. The agent's MCP adapter uses `call_tool_once` for
durable calls. Third-party tools require no changes.

A host may classify one invocation by its arguments
(`Functions::effect_of`, `Functions::at_most_once`): the agent's `tool`
function is a write, sent at most once, when the tool it names is one. It
may also name a marker for an unknown outcome (`Functions::unknown_outcome`):
the marker is saved as the call's result instead of faulting the run, and the
call is still never sent again. The agent loop uses it to tell the model a
write may have run before a restart.

## Concurrent `for`

`metadata: { concurrency: n }` on a `for` (an extension; `metadata` is free
in the specification) runs up to `n` iterations at once, each on the `for`'s
input and a copy of the context, whose exports stay inside it. Every
iteration runs to its end; then the first fault in item order wins, else the
earliest wait, else the output is the iterations' outputs in item order. A
concurrent `for` takes no `while`.

## Retries and idempotency

A `try` retry attempt does not make a protected call (a `Write`, or any
at-most-once call) a new one. Its identity is the run ID plus its path with
every enclosing attempt replaced by that `try`'s first one; loop iterations,
`then` visits, function callers and fork branches stay distinct. The
interpreter records the prefixes of the attempts it runs and stores the
result in the call record (`logical`), so the identity is never parsed out
of a path made of author-chosen task names (a loop named `try` has paths
like `/do/0/try[1]/do/0/send`), and recovery finds an earlier attempt's call
even when a completed inner `try` is restored without replaying its attempts.

Under that identity: an earlier success is returned again instead of sent
(the retry goes on); a tool error or a lost response is sent again under the
call's delivery policy; other function or arguments are a `conflict`, which
fails the run and sends nothing. Arguments that change between attempts
usually come from a model regenerating text inside the `try`, so keep a
`try` narrow around the call it retries. An at-most-once call whose outcome
is unknown or a tool error blocks the retry of its attempt. Reads and model
steps run afresh on every attempt. The key lives only in agent.db; tools
never see it. Ephemeral runs keep no results and never retry an attempt
that started a write.

## Storage and compatibility

Journal load returns one consistent snapshot of a run; each save is atomic
and durable before returning. Storage errors stop execution, including errors
reported after a commit actually succeeded; they count as interruptions. Completed records are immutable.
The host verifies ownership in the same transaction as every checkpoint.
SQLite uses WAL and `synchronous=FULL`; no remote coordinator is required.

Keep the records needed for replay, not every prior snapshot. Bump
`durable::VERSION` when persisted state or interpreter semantics change
incompatibly. Existing runs reject incompatible versions or changed limits
instead of silently executing under different rules.

## Current boundary

The supported OWF subset includes sequences, loops, reusable functions,
switches, fork/join/compete, retries, deadlines and terminal cancellation.
Waiting releases the worker; deadlines and retry decisions survive restarts.
State corruption or lost ownership stops progress without an ephemeral fallback.

External-event/user-input waits and a durable signal inbox with upstream
lease/ack are still tracked in `.claude/tasks/durable-workflows.md`.
Durability starts at run admission; the current destructive MCP dequeue has
an earlier delivery gap. The agent rejects `at_most_once` on `llm_compose`
and `filter_relevant` until their provider adapters expose a single-attempt path;
those calls currently use `at_least_once`. Delivery on a reusable function
belongs on its external call tasks.

Tests inject failure before and after checkpoint commits, exercise replay
through loops/retries/forks, and verify at-most-once dispatch, SQLite reopen,
lease fencing, pinned definitions, shutdown and ambiguous MCP responses.

`cargo test --offline -p agent --test durable` also runs a Unix subprocess
test with real SIGKILL at eight boundaries for each delivery policy: before
intent, inside an uncommitted intent transaction, after intent, after the
external effect, after its response checkpoint, after the task checkpoint,
after the terminal checkpoint, and during a durable wait. The effect has its
own fsynced ledger, separate from agent.db. Fresh processes recover against
the same SQLite file; the test checks rollback, integrity, output/context,
effect counts and terminal-state stability. Lease expiry is advanced via
the test DB to avoid waiting 30 seconds per case. The ignored worker test is
a subprocess entry point invoked by the parent test, not skipped coverage.

The durable property tests reuse `tests/support/program.rs`, generating
workflows, mixed delivery policies and sequences of up to eight before/after
commit failures. Sequential deterministic programs compare output, errors
and saved contexts against a crash-free durable run. Fork/compete programs
check protected-call dispatch and terminal-state safety without assuming a
fixed race winner. State is serialized and a new interpreter/host is created
on every resumption. `PROPERTY_RUNS` controls cases per property:

```sh
PROPERTY_RUNS=256 cargo test --offline -p owf --test durable
```
