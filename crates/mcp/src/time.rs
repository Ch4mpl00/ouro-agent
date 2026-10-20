// Timestamps as the agent sees them. Every date this server hands out has
// always been a JS `Date#toISOString()` ("2026-10-02T09:00:00.000Z"), and
// every date it accepts went through `new Date(input)`; skills and saved
// workflows depend on both shapes, so they are reproduced here once.

use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Utc};

pub fn iso(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn iso_from_unix(secs: i64) -> Option<String> {
    Utc.timestamp_opt(secs, 0).single().map(iso)
}

pub fn iso_from_unix_ms(ms: i64) -> Option<String> {
    Utc.timestamp_millis_opt(ms).single().map(iso)
}

// The inputs `new Date(...)` accepted in practice: full ISO with an offset,
// ISO without one (the process runs in UTC, so local == UTC), a bare date,
// and RFC 2822 (RSS pubDate, Gmail Date headers).
pub fn parse_js_date(input: &str) -> Option<DateTime<Utc>> {
    let s = input.trim();
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Some(t.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(t) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(t.and_utc());
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d.and_hms_opt(0, 0, 0).map(|t| t.and_utc());
    }
    DateTime::parse_from_rfc2822(s).ok().map(|t| t.with_timezone(&Utc))
}

// For tool filters: an unparseable date is the caller's mistake and must say
// so, the way `new Date("garbage")` blew up in the query builder.
pub fn require_js_date(field: &str, input: &str) -> anyhow::Result<DateTime<Utc>> {
    parse_js_date(input).ok_or_else(|| anyhow::anyhow!("{field}: invalid date \"{input}\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_like_to_iso_string() {
        let t = Utc.with_ymd_and_hms(2026, 10, 2, 9, 0, 0).unwrap();
        assert_eq!(iso(t), "2026-10-02T09:00:00.000Z");
    }

    #[test]
    fn parses_what_new_date_accepted() {
        let want = Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap();
        for input in ["2026-10-02", "2026-10-02T00:00:00Z", "2026-10-02T03:00:00+03:00", "2026-10-02T00:00:00.000"] {
            assert_eq!(parse_js_date(input), Some(want), "{input}");
        }
        assert_eq!(parse_js_date("Fri, 02 Oct 2026 00:00:00 +0000"), Some(want));
        assert_eq!(parse_js_date("yesterday"), None);
    }
}
