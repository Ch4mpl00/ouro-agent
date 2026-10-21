// Thin Langfuse public-API client for the terminal tools (the trace inspector,
// the judge, the replay). Auth comes from LANGFUSE_PUBLIC_KEY /
// LANGFUSE_SECRET_KEY / LANGFUSE_BASE_URL — the same vars the agent uses to
// WRITE traces; here we READ them back. Langfuse stores full payloads (no
// server-side truncation), so a reader gets the agent's complete retrieval
// results, not the UI-clipped view.

use std::time::Duration;

use agent::db::TraceSummary;
use agent::trace_model::{Observation, Trace};
use base64::Engine;
use serde_json::{Map, Value};

// The observations endpoint can arrive in any order. Trace identity comes
// from the earliest root; final output may be on a nested resumed segment.
pub fn trace_from_observations(id: &str, observations: Vec<Observation>) -> (Trace, Vec<Observation>) {
    let root = observations.iter().filter(|o| o.parent_observation_id.is_none()).min_by_key(|o| &o.start_time);
    let output = observations
        .iter()
        .filter(|o| {
            o.meta(agent::trace_model::TRACE_OUTPUT_META)
                .is_some_and(|v| v == &Value::Bool(true) || v.as_str() == Some("true"))
        })
        // An initial signal can end after a fast continuation; its temporary
        // output must not replace the completed continuation's answer.
        .max_by_key(|o| (o.parent_observation_id.is_some(), &o.end_time))
        .or(root);
    let trace = Trace {
        id: id.to_owned(),
        name: root.map_or_else(|| id.to_owned(), |r| r.name.clone()),
        session_id: None,
        timestamp: root.map(|r| r.start_time.clone()).unwrap_or_default(),
        input: root.map(|r| r.input.clone()).unwrap_or(Value::Null),
        output: output.map(|r| r.output.clone()).unwrap_or(Value::Null),
        metadata: root.and_then(|r| r.metadata.clone()),
        observations: observations.clone(),
        latency: 0.0,
        total_cost: 0.0,
        tags: Vec::new(),
    };
    (trace, observations)
}

#[derive(Clone)]
pub struct Langfuse {
    client: reqwest::Client,
    base_url: String,
    auth: String,
}

impl Langfuse {
    pub fn enabled() -> bool {
        ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"].iter().all(|k| std::env::var(k).is_ok_and(|v| !v.is_empty()))
    }

    pub fn from_env() -> anyhow::Result<Langfuse> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let (Some(public), Some(secret)) = (var("LANGFUSE_PUBLIC_KEY"), var("LANGFUSE_SECRET_KEY")) else {
            anyhow::bail!("LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY missing in env");
        };
        let base_url = var("LANGFUSE_BASE_URL").unwrap_or_else(|| "https://cloud.langfuse.com".into());
        let auth = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("{public}:{secret}")));
        Ok(Langfuse { client: reqwest::Client::new(), base_url: base_url.trim_end_matches('/').into(), auth })
    }

    // Retry transient failures (5xx, network errors) with linear backoff; 4xx
    // are permanent and returned at once. Langfuse Cloud occasionally 502s,
    // and a large trace can time out the gateway — both are worth a couple of
    // retries, especially for the online judge worker that polls continuously.
    async fn request(&self, method: reqwest::Method, path: &str, body: Option<&Value>) -> anyhow::Result<Value> {
        let what = if method == reqwest::Method::POST { format!("POST {path}") } else { path.to_owned() };
        let mut last: Option<anyhow::Error> = None;
        for attempt in 0..3u64 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(600 * attempt)).await;
            }
            let mut req = self
                .client
                .request(method.clone(), format!("{}/api/public{path}", self.base_url))
                .header("Authorization", &self.auth);
            if let Some(body) = body {
                req = req.json(body);
            }
            let res = match req.send().await {
                Ok(res) => res,
                Err(err) => {
                    last = Some(err.into());
                    continue;
                }
            };
            let status = res.status();
            if status.is_success() {
                let text = res.text().await?;
                return Ok(if text.is_empty() { Value::Null } else { serde_json::from_str(&text)? });
            }
            let reason = status.canonical_reason().unwrap_or("");
            if status.is_server_error() {
                last = Some(anyhow::anyhow!("langfuse {} {reason} on {what}", status.as_u16()));
                continue;
            }
            let detail =
                if body.is_some() { format!(": {}", res.text().await.unwrap_or_default()) } else { String::new() };
            anyhow::bail!("langfuse {} {reason} on {what}{detail}", status.as_u16());
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("langfuse request failed on {what}")))
    }

    pub async fn get(&self, path: &str) -> anyhow::Result<Value> {
        self.request(reqwest::Method::GET, path, None).await
    }

    pub async fn post(&self, path: &str, body: &Value) -> anyhow::Result<Value> {
        self.request(reqwest::Method::POST, path, Some(body)).await
    }

    // One trace by id with every observation resolved to a full object.
    // `/traces/<id>` inlines Observation objects; the string-id form is
    // resolved with a per-id follow-up, fanned out.
    pub async fn trace_by_id(&self, id: &str) -> anyhow::Result<(Trace, Vec<Observation>)> {
        match self.inlined_trace(id).await {
            Ok(found) => Ok(found),
            // `/traces/<id>` inlines every observation and gateway-times-out
            // (502) on large traces (big digests with 50+ snippets). Fall back
            // to the observations on their own lighter endpoint and synthesize
            // a trace stub from the root observation — judge/replay need
            // observations + the root's input/output/metadata, not the
            // trace-list fields (tags, cost).
            Err(_) => {
                let observations = self.observations_by_trace(id).await?;
                Ok(trace_from_observations(id, observations))
            }
        }
    }

    async fn inlined_trace(&self, id: &str) -> anyhow::Result<(Trace, Vec<Observation>)> {
        let raw = self.get(&format!("/traces/{}", encode(id))).await?;
        self.resolve_trace(raw).await
    }

    // A trace object from the API, its observations (ids or objects) resolved.
    pub async fn resolve_trace(&self, raw: Value) -> anyhow::Result<(Trace, Vec<Observation>)> {
        let entries = raw["observations"].as_array().cloned().unwrap_or_default();
        let observations = futures::future::try_join_all(entries.into_iter().map(|entry| async move {
            match entry {
                Value::String(oid) => observation(self.get(&format!("/observations/{oid}")).await?),
                other => observation(other),
            }
        }))
        .await?;
        Ok((trace_from(&raw, observations.clone()), observations))
    }

    // All observations for a trace via the dedicated endpoint, paged.
    async fn observations_by_trace(&self, trace_id: &str) -> anyhow::Result<Vec<Observation>> {
        let mut all = Vec::new();
        for page in 1.. {
            let res = self.get(&format!("/observations?traceId={}&limit=100&page={page}", encode(trace_id))).await?;
            let data = res["data"].as_array().cloned().unwrap_or_default();
            let empty = data.is_empty();
            for o in data {
                all.push(observation(o)?);
            }
            let total = res["meta"]["totalPages"].as_u64().unwrap_or(1);
            if empty || page >= total {
                break;
            }
        }
        Ok(all)
    }

    // Most recent traces first (Langfuse lists newest-first by default).
    // Summary only — observations are NOT inlined.
    pub async fn recent_traces(&self, limit: u32) -> anyhow::Result<Vec<TraceSummary>> {
        let res = self.get(&format!("/traces?limit={limit}")).await?;
        Ok(res["data"].as_array().into_iter().flatten().map(summary).collect())
    }
}

pub fn encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>().replace('+', "%20")
}

fn text(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_owned()
}

fn object(v: &Value) -> Option<Map<String, Value>> {
    v.as_object().cloned()
}

pub fn summary(v: &Value) -> TraceSummary {
    TraceSummary {
        id: text(&v["id"]),
        name: text(&v["name"]),
        timestamp: text(&v["timestamp"]),
        tags: v["tags"].as_array().into_iter().flatten().filter_map(|t| t.as_str().map(str::to_owned)).collect(),
    }
}

pub fn trace_from(raw: &Value, observations: Vec<Observation>) -> Trace {
    let s = summary(raw);
    Trace {
        id: s.id,
        name: s.name,
        session_id: raw["sessionId"].as_str().map(str::to_owned),
        timestamp: s.timestamp,
        input: raw["input"].clone(),
        output: raw["output"].clone(),
        metadata: object(&raw["metadata"]),
        observations,
        latency: raw["latency"].as_f64().unwrap_or(0.0),
        total_cost: raw["totalCost"].as_f64().unwrap_or(0.0),
        tags: s.tags,
    }
}

// An observation as the API returns it, with the fields the API may leave
// null (an open span's endTime, latency) filled the way the local mirror has
// them.
pub fn observation(mut v: Value) -> anyhow::Result<Observation> {
    if let Some(o) = v.as_object_mut() {
        for key in ["endTime", "startTime", "level"] {
            if !o.get(key).is_some_and(Value::is_string) {
                o.insert(key.into(), if key == "level" { "DEFAULT".into() } else { "".into() });
            }
        }
        if !o.get("latency").is_some_and(Value::is_number) {
            o.insert("latency".into(), 0.into());
        }
        for key in ["metadata", "modelParameters", "usageDetails"] {
            if !o.get(key).is_some_and(Value::is_object) {
                o.insert(key.into(), Value::Null);
            }
        }
        for key in ["input", "output"] {
            o.entry(key).or_insert(Value::Null);
        }
        let usage_ok = o.get("usage").is_some_and(|u| ["input", "output", "total"].iter().all(|k| u[*k].is_u64()));
        if !usage_ok {
            o.insert("usage".into(), Value::Null);
        }
        if let Some(Value::Object(details)) = o.get_mut("usageDetails") {
            details.retain(|_, v| v.is_number());
        }
    }
    Ok(serde_json::from_value(v)?)
}
