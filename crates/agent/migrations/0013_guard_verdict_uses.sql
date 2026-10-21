CREATE TABLE guard_verdict_uses (
    child_run TEXT NOT NULL REFERENCES workflow_runs(id),
    call_key TEXT NOT NULL,
    invocation TEXT NOT NULL,
    PRIMARY KEY (child_run, call_key)
);
