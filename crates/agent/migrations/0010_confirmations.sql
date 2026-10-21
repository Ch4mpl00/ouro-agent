CREATE TABLE confirmations (
    run_id TEXT NOT NULL REFERENCES workflow_runs(id),
    call_key TEXT NOT NULL,
    yes_type TEXT NOT NULL UNIQUE,
    no_type TEXT NOT NULL UNIQUE,
    chat_id TEXT NOT NULL,
    thread_id INTEGER,
    message_id INTEGER,
    card TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    outcome TEXT,
    PRIMARY KEY (run_id, call_key)
);
