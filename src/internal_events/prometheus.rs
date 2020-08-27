use super::InternalEvent;
use crate::sources::prometheus::parser::ParserError;
use http::Uri;
use metrics::{counter, timing};
use std::borrow::Cow;
use std::time::Instant;

#[derive(Debug)]
pub struct PrometheusEventReceived {
    pub byte_size: usize,
    pub count: usize,
}

impl InternalEvent for PrometheusEventReceived {
    fn emit_logs(&self) {
        debug!(message = "Scraped events.", ?self.count);
    }

    fn emit_metrics(&self) {
        counter!(
            "events_processed", self.count as u64,
            "component_kind" => "source",
            "component_type" => "prometheus",
        );
        counter!(
            "bytes_processed", self.byte_size as u64,
            "component_kind" => "source",
            "component_type" => "prometheus",
        );
    }
}

#[derive(Debug)]
pub struct PrometheusRequestCompleted {
    pub start: Instant,
    pub end: Instant,
}

impl InternalEvent for PrometheusRequestCompleted {
    fn emit_logs(&self) {
        debug!(message = "Request completed.");
    }

    fn emit_metrics(&self) {
        counter!("requests_completed", 1,
            "component_kind" => "source",
            "component_type" => "prometheus",
        );
        timing!("request_duration_nanoseconds", self.start, self.end,
            "component_kind" => "source",
            "component_type" => "prometheus",
        );
    }
}

#[derive(Debug)]
pub struct PrometheusParseError<'a> {
    pub error: ParserError,
    pub url: &'a Uri,
    pub body: Cow<'a, str>,
}

impl<'a> InternalEvent for PrometheusParseError<'a> {
    fn emit_logs(&self) {
        error!(message = "parsing error.", url = %self.url, error = %self.error);
        debug!(
            message = %format!("failed to parse response:\n\n{}\n\n", self.body),
            url = %self.url,
            rate_limit_secs = 10
        );
    }

    fn emit_metrics(&self) {
        counter!("parse_errors", 1,
            "component_kind" => "source",
            "component_type" => "prometheus",
        );
    }
}

#[derive(Debug)]
pub struct PrometheusErrorResponse<'a> {
    pub code: hyper::StatusCode,
    pub url: &'a Uri,
}

impl InternalEvent for PrometheusErrorResponse<'_> {
    fn emit_logs(&self) {
        error!(message = "HTTP error response.", url = %self.url, code = %self.code);
    }

    fn emit_metrics(&self) {
        counter!("http_error_response", 1,
            "component_kind" => "source",
            "component_type" => "prometheus",
        );
    }
}

#[derive(Debug)]
pub struct PrometheusHttpError<'a> {
    pub error: hyper::Error,
    pub url: &'a Uri,
}

impl InternalEvent for PrometheusHttpError<'_> {
    fn emit_logs(&self) {
        error!(message = "HTTP request processing error.", url = %self.url, error = %self.error);
    }

    fn emit_metrics(&self) {
        counter!("http_request_errors", 1,
            "component_kind" => "source",
            "component_type" => "prometheus",
        );
    }
}
