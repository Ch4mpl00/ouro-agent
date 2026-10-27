-- A loop a workflow starts itself (`start_child` with `host: inherit`, design
-- 09) inherits its parent's host data and carries its parameters in its
-- input (`request`). The first host function that serves it prepares it
-- once; the first stored preparation is the one every later call, recovery
-- and guard reads. Swept with the run's evidence.
CREATE TABLE prepared_loops (
    run_id TEXT PRIMARY KEY NOT NULL,
    body TEXT NOT NULL,
    created_ms INTEGER NOT NULL
);
