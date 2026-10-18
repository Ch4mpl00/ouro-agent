// Shared by the property targets (invariants, metamorphic, model): the
// random program generator and the host that runs a program and records it.
//
//   program — the program tree, its generator, rendering and rewrites
//   host    — fake functions, the event/call journal, running on a paused clock

#![allow(dead_code)]

pub mod host;
pub mod program;
