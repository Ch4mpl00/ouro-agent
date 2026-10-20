# Remove the dynamic workflow planner

**Status:** done
**Priority:** P1
**Area:** agent workflows / signal routing
**Created:** 2026-10-20

## Context

The JSON DSL compiler/executor and OWF saved workflows duplicated workflow
execution. Only two production monthly reminders still used the planner.
Scheduled prompts can be handled by the primary agent loop; recurring
deterministic work belongs in an explicitly authored saved workflow.

## Acceptance

- Remove the dynamic DSL, variables, compiler, executor, planner skill and
  planner-generation replay mode, with their dedicated tests and fixtures.
- Keep saved workflows' composition, signal input and result parsing in
  `flows.rs`, including JSON mode and compose trace metadata.
- Route scheduled prompts without a saved workflow straight to the primary
  loop with the scheduler skill, base preset and `agent-loop` trace tag.
- Preserve saved-workflow handover before writes and recovery after writes.
- Update composition roots, integration tests, skills and architecture docs.
- Run workspace Clippy and the full test suite; record results in the task
  report. Keep the judge's Planner node/rubric for historical traces.

## Notes

No migration, production task rewrite, deployment or push is required.
After deployment, the production reminders use the primary loop on their
next fire. Historical trace fixtures and judge support remain readable. The compiler preset stays
as a configuration compatibility field; the removed planner no longer uses it.

Validation: workspace Clippy with all targets and warnings denied passed;
`scripts/test.py` passed all 96 binaries with local TCP/Docker access. The
initial sandboxed run failed on unavailable local sockets/Docker; the task
report records that output.
