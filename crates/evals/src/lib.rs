// The evaluation tooling around the agent: the per-node judge (CLI + online
// worker), judge-noise calibration, the improver gate and the closed-loop
// improver, plus the replay and filter evals. It reads the agent's trace
// mirror (agent.db) and Langfuse; nothing here runs in the signal path. The
// binaries in src/bin are the composition roots.

pub mod backend;
pub mod env;
pub mod gate;
pub mod gate_runtime;
pub mod improve_cycle;
pub mod improve_worker;
pub mod improver;
pub mod improver_store;
pub mod langfuse_api;
pub mod materials;
pub mod monitor;
pub mod node_judge;
pub mod noise;
pub mod optimizer;
pub mod print;
pub mod schema;
pub mod scores;
pub mod sigma;
pub mod source;
pub mod worker;
