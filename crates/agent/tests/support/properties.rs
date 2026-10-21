//! Shared case budget and non-vacuity checks; no store reference model.
use std::{collections::BTreeMap, sync::Mutex};

pub fn runs() -> u32 {
    std::env::var("PROPERTY_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(24)
}

pub struct Witnesses(Mutex<BTreeMap<&'static str, usize>>);

impl Witnesses {
    pub fn new(required: &[&'static str]) -> Self {
        Self(Mutex::new(required.iter().map(|s| (*s, 0)).collect()))
    }

    pub fn hit(&self, name: &'static str) {
        *self.0.lock().unwrap().get_mut(name).expect("declared witness") += 1;
    }

    pub fn finish(self) {
        let counts = self.0.into_inner().unwrap();
        eprintln!("witnesses: {counts:?}");
        assert!(counts.values().all(|n| *n > 0), "unreached witnesses: {counts:?}");
    }
}
