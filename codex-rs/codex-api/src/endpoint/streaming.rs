use crate::auth::AuthProvider;
use crate::auth::add_auth_headers;
use crate::common::ResponseStream;
use crate::error::ApiError;
use crate::provider::Provider;
use crate::telemetry::SseTelemetry;
use crate::telemetry::run_with_request_telemetry;
use codex_client::HttpTransport;
use codex_client::Request;
use codex_client::RequestTelemetry;
use codex_client::StreamResponse;
use http::HeaderMap;
use http::Method;
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::Write as IoWrite;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

pub(crate) struct StreamingClient<T: HttpTransport, A: AuthProvider> {
    transport: T,
    provider: Provider,
    auth: A,
    request_telemetry: Option<Arc<dyn RequestTelemetry>>,
    sse_telemetry: Option<Arc<dyn SseTelemetry>>,
}

impl<T: HttpTransport, A: AuthProvider> StreamingClient<T, A> {
    pub(crate) fn new(transport: T, provider: Provider, auth: A) -> Self {
        Self {
            transport,
            provider,
            auth,
            request_telemetry: None,
            sse_telemetry: None,
        }
    }

    pub(crate) fn with_telemetry(
        mut self,
        request: Option<Arc<dyn RequestTelemetry>>,
        sse: Option<Arc<dyn SseTelemetry>>,
    ) -> Self {
        self.request_telemetry = request;
        self.sse_telemetry = sse;
        self
    }

    pub(crate) fn provider(&self) -> &Provider {
        &self.provider
    }

    pub(crate) async fn stream(
        &self,
        path: &str,
        body: Value,
        extra_headers: HeaderMap,
        spawner: fn(StreamResponse, Duration, Option<Arc<dyn SseTelemetry>>) -> ResponseStream,
    ) -> Result<ResponseStream, ApiError> {
        let builder = || {
            let mut req = self.provider.build_request(Method::POST, path);
            req.headers.extend(extra_headers.clone());
            req.headers.insert(
                http::header::ACCEPT,
                http::HeaderValue::from_static("text/event-stream"),
            );
            req.body = Some(body.clone());
            let req = add_auth_headers(&self.auth, req);
            log_request(&req);
            req
        };

        let stream_response = run_with_request_telemetry(
            self.provider.retry.to_policy(),
            self.request_telemetry.clone(),
            builder,
            |req| self.transport.stream(req),
        )
        .await?;

        Ok(spawner(
            stream_response,
            self.provider.stream_idle_timeout,
            self.sse_telemetry.clone(),
        ))
    }
}

fn log_request(req: &Request) {
    if let Err(err) = append_request(req) {
        debug!("failed to log HTTP request: {err}");
    }
}

fn append_request(req: &Request) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("codex_prompt.log")?;

    writeln!(file, "==== HTTP Request ====")?;
    writeln!(file, "Method: {}", req.method)?;
    writeln!(file, "URL: {}", req.url)?;
    writeln!(file, "Headers:")?;
    for (name, value) in &req.headers {
        writeln!(file, "{}: {}", name, value.to_str().unwrap_or("<non-utf8>"))?;
    }
    let body = req.body.as_ref().map_or_else(
        || "None".to_string(),
        |body| serde_json::to_string(body).unwrap_or_else(|err| format!("invalid JSON: {err}")),
    );
    writeln!(file, "Body: {body}")?;
    writeln!(file)?;
    file.flush()?;
    Ok(())
}
