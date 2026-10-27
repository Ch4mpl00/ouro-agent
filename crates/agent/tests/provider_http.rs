// The actual reqwest adapter against loopback HTTP: one retry layer, exact
// request headers/body and cancellation. No credentials or external services.
use agent::{models::ReasoningEffort, providers::*};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
    seen: Arc<Mutex<Vec<Value>>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server() -> Server {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let log = seen.clone();
    let app=axum::Router::new().route("/chat/completions",axum::routing::post(move|headers:axum::http::HeaderMap,axum::Json(body):axum::Json<Value>|{let log=log.clone();let count=count.clone();async move{
        assert_eq!(headers["authorization"],"Bearer test-key");log.lock().unwrap().push(body.clone());
        if body["model"]=="retry" && count.fetch_add(1,Ordering::SeqCst)==0{return (axum::http::StatusCode::SERVICE_UNAVAILABLE,axum::Json(json!({"error":{"message":"offline"}})));}
        if body["model"]=="wait"{std::future::pending::<()>().await;}
        (axum::http::StatusCode::OK,axum::Json(json!({"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}})))
    }}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server { url: format!("http://{addr}"), task, seen }
}
#[tokio::test]
async fn actual_http_requests_retry_exactly_once_and_preserve_body() {
    let server = server().await;
    let inner = Arc::new(provider(ProviderKind::Openai, Arc::new(HttpApi::new(&server.url, "test-key"))));
    let p = with_retry(inner, RetryOpts { max_retries: 1, base_delay_ms: 0, ..Default::default() });
    let mut messages = vec![user("hello")];
    let result = p
        .complete(&mut CompletionParams {
            model: "retry".into(),
            messages: &mut messages,
            reasoning_effort: ReasoningEffort::Max,
            tools: Vec::new(),
            response_format: None,
            trace: None,
            cancel: CancellationToken::new(),
        })
        .await
        .unwrap();
    assert_eq!(result.message["content"], "ok");
    assert_eq!(result.usage.unwrap().total, 12);
    let seen = server.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], json!({"model":"retry","messages":[{"role":"user","content":"hello"}]}));
    assert_eq!(seen[1], seen[0]);
}
#[tokio::test]
async fn actual_http_wait_is_cancelled() {
    let server = server().await;
    let api = Arc::new(HttpApi::new(&server.url, "test-key"));
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let task = tokio::spawn(async move { api.post("chat/completions", json!({"model":"wait"}), &signal).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while server.seen.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    assert!(tokio::time::timeout(std::time::Duration::from_secs(2), task).await.unwrap().unwrap().is_err());
    assert_eq!(server.seen.lock().unwrap().len(), 1);
}
