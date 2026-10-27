-- A loop's `run_reflex` call, resolved once (design 10, R1): the reflex's
-- version, declaration, input and what it does, pinned for the call's
-- review, start and join. Keyed by the loop run and the call's place in
-- its round; the first stored resolution wins. Swept with the run's
-- evidence.
CREATE TABLE reflex_calls (
    run_id TEXT NOT NULL,
    position TEXT NOT NULL,
    call_key TEXT NOT NULL,
    body TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    PRIMARY KEY (run_id, position)
);
