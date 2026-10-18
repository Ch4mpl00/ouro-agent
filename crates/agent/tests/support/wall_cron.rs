// What a 5-field cron means on the user's wall clock, restated for tests: a
// wall-clock minute either matches the expression or it doesn't. A port of
// packages/mcp/test/support/wall-cron.ts. Deliberately not the scheduler's
// cron library (no "next" arithmetic) — tests scan instants minute by minute
// and ask the wall clock, so a bug in croner can't hide behind an oracle
// built on it.
//
// Supported syntax: `*`, `n`, `a-b`, `*/s`, `a-b/s`, comma lists; day of week
// 0-7 with 7 = Sunday. Day-of-month and day-of-week combine the classic way:
// both restricted → either may match; otherwise both must.

#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use chrono::{Datelike, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

#[derive(Debug, Clone)]
pub struct WallCron {
    minute: BTreeSet<u32>,
    hour: BTreeSet<u32>,
    dom: BTreeSet<u32>,
    month: BTreeSet<u32>,
    dow: BTreeSet<u32>,
    pub dom_restricted: bool,
    pub dow_restricted: bool,
}

fn field(text: &str, min: u32, max: u32) -> Result<BTreeSet<u32>, String> {
    let mut out = BTreeSet::new();
    for part in text.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (r, Some(s)),
            None => (part, None),
        };
        let step: u32 = step.map_or(Ok(1), |s| s.parse().map_err(|_| format!("bad step {s}")))?;
        let (lo, hi) = if range == "*" {
            (min, max)
        } else {
            let (a, b) = match range.split_once('-') {
                Some((a, b)) => (a, Some(b)),
                None => (range, None),
            };
            let lo: u32 = a.parse().map_err(|_| format!("bad field {text:?}"))?;
            let hi = match b {
                Some(b) => b.parse().map_err(|_| format!("bad field {text:?}"))?,
                None if part.contains('/') => max,
                None => lo,
            };
            (lo, hi)
        };
        if step < 1 || lo < min || hi > max || lo > hi {
            return Err(format!("wall-cron: bad field {text:?}"));
        }
        out.extend((lo..=hi).step_by(step as usize));
    }
    Ok(out)
}

pub fn parse_wall_cron(expr: &str) -> Result<WallCron, String> {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    if parts.len() != 5 {
        return Err(format!("wall-cron: not 5 fields: {expr:?}"));
    }
    let mut dow = field(parts[4], 0, 7)?;
    if dow.contains(&7) {
        dow.insert(0);
    }
    Ok(WallCron {
        minute: field(parts[0], 0, 59)?,
        hour: field(parts[1], 0, 23)?,
        dom: field(parts[2], 1, 31)?,
        month: field(parts[3], 1, 12)?,
        dow,
        dom_restricted: !parts[2].starts_with('*'),
        dow_restricted: !parts[4].starts_with('*'),
    })
}

// A wall-clock minute, as minutes since 1970-01-01T00:00 *on that wall
// clock* (not an instant): comparable, and its calendar fields read back as
// if it were UTC.
pub type WallMinute = i64;

fn naive(wall: WallMinute) -> NaiveDateTime {
    Utc.timestamp_opt(wall * 60, 0).unwrap().naive_utc()
}

pub fn matches(cron: &WallCron, wall: WallMinute) -> bool {
    let d = naive(wall);
    if !cron.minute.contains(&d.minute()) || !cron.hour.contains(&d.hour()) || !cron.month.contains(&d.month()) {
        return false;
    }
    let dom_ok = cron.dom.contains(&d.day());
    let dow_ok = cron.dow.contains(&d.weekday().num_days_from_sunday());
    if cron.dom_restricted && cron.dow_restricted { dom_ok || dow_ok } else { dom_ok && dow_ok }
}

// The wall-clock minute in `tz` at the instant `minute` (minutes since epoch).
pub fn wall_at(tz: Tz, minute: i64) -> WallMinute {
    static CACHE: Mutex<Option<HashMap<(Tz, i64), WallMinute>>> = Mutex::new(None);
    let mut cache = CACHE.lock().unwrap();
    let cache = cache.get_or_insert_with(HashMap::new);
    if let Some(w) = cache.get(&(tz, minute)) {
        return *w;
    }
    let local = Utc.timestamp_opt(minute * 60, 0).unwrap().with_timezone(&tz).naive_local();
    let wall = local.and_utc().timestamp() / 60;
    if cache.len() > 2_000_000 {
        cache.clear();
    }
    cache.insert((tz, minute), wall);
    wall
}

// The instant `minute` shows a wall time the clock already showed earlier
// (the repeated hour — or half hour — after clocks go back). Such a repeat is
// a second chance at the same wall time, never an obligation.
pub fn is_repeat(tz: Tz, minute: i64) -> bool {
    let wall = wall_at(tz, minute);
    wall_at(tz, minute - 30) == wall || wall_at(tz, minute - 60) == wall
}

// The first instant (whole minute, strictly after `after_ms`, at most
// `until_ms`) at which a scheduler owes a fire: the wall clock shows a
// matching time it hasn't shown before. None if none.
pub fn first_owed(cron: &WallCron, tz: Tz, after_ms: i64, until_ms: i64) -> Option<i64> {
    let mut m = after_ms.div_euclid(60_000) + 1;
    while m * 60_000 <= until_ms {
        if matches(cron, wall_at(tz, m)) && !is_repeat(tz, m) {
            return Some(m * 60_000);
        }
        m += 1;
    }
    None
}

// `slot_ms` is a legitimate fire time: its wall time matches, or it stands in
// for a matching wall time the clock skipped (clocks going forward) — the
// skipped time shifted by the size of the jump, within two hours of it.
pub fn legitimate_slot(cron: &WallCron, tz: Tz, slot_ms: i64) -> bool {
    if slot_ms % 60_000 != 0 {
        return false;
    }
    let slot = slot_ms / 60_000;
    let wall = wall_at(tz, slot);
    if matches(cron, wall) {
        return true;
    }
    for g in slot - 120..=slot {
        let jump = wall_at(tz, g) - wall_at(tz, g - 1) - 1;
        if jump <= 0 {
            continue;
        }
        let skipped = wall - jump;
        if skipped > wall_at(tz, g - 1) && skipped < wall_at(tz, g) && matches(cron, skipped) {
            return true;
        }
    }
    false
}

const LONGEST_MONTH: [u32; 12] = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

// Some selected month has one of the selected days (Nov 31 doesn't; Feb 29
// does).
pub fn days_exist(cron: &WallCron) -> bool {
    cron.month.iter().any(|m| cron.dom.iter().any(|d| *d <= LONGEST_MONTH[(*m - 1) as usize]))
}
