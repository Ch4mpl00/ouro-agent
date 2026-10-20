# tasks/

Tech-debt and planned work that we agreed on but haven't picked up yet.
One markdown file per task, written in English (Russian only inside quoted
user data or examples). Files stay here while pending or in-progress.

When a task is finished, delete the file: git history keeps the record. The
exception is a task other files point to (CLAUDE.md, `docs/`, code comments,
skills, other tasks): it is a design record people follow, so move it to
`tasks/done/` and fix the references. Before deleting or moving, grep for the
file name. A task about code that no longer exists is obsolete: delete it, don't
leave it to rot.

Keep a task honest as the code moves: when part of it ships, say in **Status**
what is done and what is left, and drop what no longer applies.

## File format

```markdown
# <short title>

**Status:** pending | in-progress | paused | done (+ a clause on what is done
and what is left, when partly done)
**Priority:** P0 | P1 | P2 | P3
**Area:** <module / concern>
**Created:** YYYY-MM-DD

## Context
Why this matters. What problem it solves, what it unblocks, what
incident or design discussion it came from.

## Acceptance
What "done" looks like — concrete, checkable.

## Notes
Optional: alternatives considered, links, open questions.
```

Keep priorities honest:

- **P0** — blocking or actively burning ($-leak, prod outage path).
- **P1** — important architectural debt that gates further growth.
- **P2** — quality-of-life / nice to have.
- **P3** — write it down so we don't forget; no commitment to do it.

The tasks are in the public repository: no server addresses, credentials,
chat ids, account numbers or amounts, and nothing personal about the user.
