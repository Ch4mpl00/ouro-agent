# 07. Step 6: remove `crates/owf`

Status: executor's design (step 6, no goldfish round: a removal with no new
contract) · Overview: [01](01-engine-migration.md) step 6, decision 11 ·
Epic: #62 · Engine: ouro 0.3.0, no engine change · Decisions log: [LOG.md](LOG.md)

## Problem

Since step 5 every run of the agent is an engine run, yet the forked
interpreter `crates/owf` is still a workspace member with three users:

- `crates/agent/src/flows.rs`: `check` (the `owf` reading of a definition,
  for the evals workbench), §6 `Standalone`/`standalone_host` (the workbench's
  adapter of the agent's steps and MCP calls, an `owf::run::Functions`), and
  `owf::durable::Delivery` in both;
- `crates/evals`: the optimizer's workbench runs candidates on
  `owf::run::run` with `owf::run::{Functions, Effect, Observer, …}`, and
  `parse_yaml` checks the proposal (`optimizer.rs`, `bin/optimize.rs`,
  `tests/optimizer_workbench.rs`);
- `crates/agent/tests/engine_dialect.rs`, which compares `owf`'s and `ows`'s
  reading of the flat dialect fixture `tests/fixtures/telegram-loop.flat.yaml`
  (also `crates/evals/tests/optimizer.rs`'s `DEFINITION`).

So the workbench still runs candidates in the dialect the agent no longer
runs: a candidate's `metadata: { concurrency: 3 }` runs its passes three at a
time on the bench and one at a time (the key ignored) on the engine, and a
`metadata.ouro` extension is the other way round. The workflow-optimizer skill teaches the flat keys.

`crates/evals` has no `workflow_runs` dependency (no SQL on agent.db's old run
tables, no `agent::durable`): checked by grep, nothing to change there.

## Decisions

1. **The workbench runs on `ows::run::run`** (overview decision 11), the
   engine's interpreter in memory with no journal, not the engine's dry run.
   `ows::run` is the same seam shape as `owf::run` (`Functions`, `Host`,
   `Observer`, `Limits`, `Status`, `CallFailure`), so the workbench's rules
   stay as they are: an exact fixture answers first; else a call the host
   allowed real runs only if it reads (cached per task, function and
   arguments); else the trial stops `needs_mock` with a host interruption
   (`CallFailure::Interrupted`) that YAML cannot catch. The dry run
   (`Runtime::dry_run_opts`) is rejected for now: it needs a registry of
   schema'd `Function`s and a second runtime, dispatches every unmarked call
   for real (the workbench needs "missing" as a stop), and its observer and
   journal would have to be mapped back onto the same report. It would buy
   nothing the candidates use: they are standalone (no host data, children or
   events, `WORKBENCH_LIMITS`).
2. **The effect classification moves to the workbench's own seam.**
   `ows::run::Functions` has no `effect`: the engine does not know whether a
   function reads or writes. `evals::optimizer::Adapter: Functions` adds
   `effect(name)` and `effect_of(name, args)` (the per-invocation one, default
   `effect`). `ScenarioFunctions` answers `has` from it and declares
   `at_most_once` for a write invocation, as the agent's registry binds an MCP
   write (overview decision 8). The effect type is the agent's
   `agent::agent::Effect`, which gains `Serialize`/`Deserialize` with the
   variant names `owf::run::Effect` had (`Read`, `Write`), so `--catalog`
   files and reports keep their shape.
3. **The agent's adapter is `agent::flows::Standalone`** (§6, made public,
   built by `flows::standalone(engine, root)` in place of `standalone_host`):
   an `ows::run::Functions` (`has`, `invoke`, `call`, `at_most_once` for a
   write) with an inherent `effect`; `evals` implements `Adapter` for it.
   Its rules do not change: only `workflow`-context host functions and MCP
   tools; a model step refuses at-most-once delivery (`set_memory` excepted);
   a `[tool error]` reply is an error response; transport failures keep their
   not-executed/unknown split.
4. **`flows::check` reads with `ows`**: `ows::model::parse_yaml` + `validate`
   against what the agent can call + the same argument-name check as `gate`.
   It stays a registry-free check for the workbench (the gate needs the
   engine's registry and is the save/activate path); its doc stops calling
   itself the `owf` reading. `proposal_yaml` and the optimize binary's
   `final_definition` parse with `ows::model::parse_yaml`.
5. **The workbench's observer** maps the `ows::run::Event`s it reported
   before (`TaskStarted`, `TaskCompleted`, `TaskFaulted`, `TaskSkipped`,
   `SwitchCase`, `TaskRetrying`, `TaskSuspended`) to the same `path` entries,
   and ignores the engine's newer ones (calls are already in `calls`; the
   enum is `non_exhaustive`).
6. **The workflow-optimizer skill teaches the engine's dialect**:
   `metadata: { ouro: { concurrency: N } }` on a `for`, as
   `workflow-author` does.
7. **`engine_dialect.rs` keeps what still guards something real, without
   `owf`.** It goes: `converted_workflows_mean_the_same_task_by_task` (the
   `owf`-vs-`ows` equivalence of step 2's conversion; that conversion is done
   and no flat definition is left to convert). It stays: the built-ins are in
   the engine's dialect (no flat key left: converting them is a no-op; they
   opt out of the breaker), the engine silently drops flat keys (shown on the
   synthetic definition: this is why the first check matters), the converter
   keeps other metadata, and document extensions reach the engine. The fixture
   `tests/fixtures/telegram-loop.flat.yaml` is deleted; `optimizer.rs`'s
   `DEFINITION` becomes the reference loop, `agent::agent::AGENT_LOOP`.
8. **Removal:** `crates/owf` (sources, tests, `schema/`, README) and its
   workspace entry; the `owf` dependency of `agent` and `evals` (`evals`
   gains `ows` at the agent's tag). Docs: the `crates/owf` bullet and layout
   lines of `CLAUDE.md` (the `evals` bullet gains the optimizer), the OWF
   wording that means the old interpreter in `docs/architecture.md`, the
   deploy note in `docs/operations.md`, the crate table and highlight in
   `README.md`. "OWF/OWS" stays where it names the DSL. The `.claude/tasks`
   records that describe `crates/owf` as live code
   (`durable-workflows.md`, `agent-loop-on-owf.md`) get a superseded status
   line pointing here; their history stays. The `area:owf` issue label is
   GitHub's and stays.

## Non-goals

- No change to the engine, to the agent's running path (registry, runs,
  sandbox, schedules) or to the gate.
- No dry-run workbench (decision 1), no new workbench capability (children,
  events, host data), no change to report fields.
- The `.claude/tasks` records are not rewritten; open issues #43/#44 they
  link are the maintainer's to re-triage against the engine.

## Tests

Existing, which prove the workbench still works after the switch (all in
`crates/evals/tests/optimizer_workbench.rs` unless named):

- `exact_values_canonical_keys_and_fixture_precedence`: fixture precedence and
  exact-value matching;
- `missing_mock_escapes_catch_but_injected_error_is_catchable_and_no_live_fallback`:
  the `needs_mock` stop escapes `try/catch`, an injected error does not;
- `real_cache_survives_fixture_changes_but_never_execution_state_or_candidate_changes`;
- `parallel_calls_are_independent_bounded_and_match_values_not_arrival_order`
  (rewritten to `metadata: { ouro: { concurrency: 3 } }`; its peak of 3 fails
  before the switch, since `owf` ignores the nested key: the test-first case
  of decision 6);
- `rejected_context_unknown_function_and_duplicate_fixtures_do_not_execute`;
- `full_recorded_refs_preserve_json_wire_strings_and_errors_without_previews`;
- `twenty_recorded_responses_partial_all_failed_empty_parse_and_read_failure`
  (its pipeline's `for` rewritten to the nested key);
- `existing_agent_loop_inspects_missing_mock_adds_fixture_and_reruns`;
- `real_compose_reuses_existing_function_adapter_and_fake_provider`,
  `provider_meter_tracks_large_cached_usage_without_stopping_execution`,
  `candidate_compose_uses_actual_preset_family`: the agent's `Standalone`
  adapter on `ows`;
- `dynamic_write_effect_is_never_permitted_real_and_cancelled_calls_stay_reported`;
- `competing_branch_missing_mock_cannot_leave_a_success_output`;
- `crates/evals/tests/optimizer.rs`: proposals parse with `ows`, on the
  reference loop;
- `crates/agent/tests/flows.rs`: `check`'s refusals (unknown function,
  supervisor-only tool, misspelt and missing arguments, model arguments).

New: `a_flat_concurrency_key_is_not_the_engines` (workbench): the same
`for` with the flat `metadata: { concurrency: 3 }` runs as the engine runs
it: one pass at a time (peak 1), its output the last pass's, not the array,
so a candidate tested on the bench means what it means in production.

Hand mutants (at most two, once each on a scratch copy, no cargo-mutants),
on the workbench's key rule (fixture, else allowed read, else uncatchable
stop):

1. A missing fixture answered as a catchable error response
   (`CallFailure::Response` in place of `Interrupted`): caught by
   `missing_mock_escapes_catch…` and `competing_branch…`.
2. An allowed name run real whatever its effect (the `effect == Read`
   condition dropped from the mode): caught by
   `dynamic_write_effect_is_never_permitted_real…`.

Then `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings` and one
full `scripts/test.py`.

## Rollout

Nothing deploys: `crates/owf` was never a binary, and the agent binary's
running path does not change. The maintainer merges `feat/engine` and
deploys by the overview's rollout.

## Open questions

None.

## Deviations

1. **Hand mutant 1 is caught by one test, not two.** A missing fixture
   answered as a catchable error is caught by
   `missing_mock_escapes_catch…` only; `competing_branch…` still sees
   `needs_mock` (the losing branch's call is recorded `missing` whatever the
   error kind). One killing test is what the mutant needs; the design's
   "and `competing_branch…`" was wrong.
