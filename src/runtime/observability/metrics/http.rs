//! Optional HTTP `/metrics` scrape endpoint (Prometheus text).
//!
//! Bind with `VOLGA_METRICS_BIND_ADDR` (kube sets `0.0.0.0:9090`). Unset = no
//! listener, so local multi-worker processes do not collide on 9090.

use std::net::SocketAddr;

use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

use super::registry::{init_metrics, prometheus_handle};

pub const METRICS_BIND_ENV: &str = "VOLGA_METRICS_BIND_ADDR";
pub const DEFAULT_METRICS_PORT: u16 = 9090;

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Install the global recorder and, if `VOLGA_METRICS_BIND_ADDR` is set, serve `/metrics`.
pub fn spawn_metrics_http_from_env() {
    init_metrics();
    let Ok(raw) = std::env::var(METRICS_BIND_ENV) else {
        return;
    };
    if raw.is_empty() {
        return;
    }
    let addr: SocketAddr = match raw.parse() {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("{METRICS_BIND_ENV}={raw} is not a socket address: {err}");
            return;
        }
    };
    tokio::spawn(async move {
        if let Err(err) = serve_metrics(addr).await {
            eprintln!("metrics HTTP server on {addr} failed: {err}");
        }
    });
}

async fn serve_metrics(addr: SocketAddr) -> std::io::Result<()> {
    let app = Router::new().route("/metrics", get(metrics_handler));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("[metrics] HTTP /metrics listening on {addr}");
    axum::serve(listener, app).await
}

async fn metrics_handler() -> impl IntoResponse {
    let body = prometheus_handle()
        .map(|handle| handle.render())
        .unwrap_or_default();
    (
        StatusCode::OK,
        [(CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
        body,
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::serve_metrics;
    use crate::common::ports::gen_unique_grpc_port;
    use crate::runtime::metrics::init_metrics;
    use metrics::counter;

    #[tokio::test]
    async fn metrics_http_serves_prometheus_text() {
        init_metrics();
        counter!("volga_metrics_http_test").increment(1);
        let addr = format!("127.0.0.1:{}", gen_unique_grpc_port())
            .parse()
            .unwrap();
        tokio::spawn(async move {
            serve_metrics(addr).await.expect("metrics server");
        });

        let url = format!("http://{addr}/metrics");
        let started = tokio::time::Instant::now();
        let body = loop {
            match reqwest::get(&url).await {
                Ok(resp) => {
                    assert_eq!(resp.status(), reqwest::StatusCode::OK);
                    break resp.text().await.expect("metrics body");
                }
                Err(_) if started.elapsed() < Duration::from_secs(2) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => panic!("GET {url}: {err}"),
            }
        };
        assert!(
            body.contains("volga_metrics_http_test"),
            "scrape missing test series: {body:?}"
        );
    }
}
