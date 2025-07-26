use exn::Result;
use exn::ResultExt;
use exn::bail;
use fastrace::prelude::*;
use jiff::SignedDuration;
use nu_ansi_term::Color;
use scopedb_client::ResultSet;
use scopedb_client::StatementCancelResponse;
use scopedb_client::StatementEstimatedProgress;
use uuid::Uuid;

use crate::error::Error;

#[derive(Debug)]
pub struct ScopeQLClient {
    client: scopedb_client::Client,
}

fn format_result_set(
    result_set: ResultSet,
    duration: SignedDuration,
    progress: StatementEstimatedProgress,
) -> Result<String, Error> {
    let num_rows = match result_set.num_rows() {
        n @ 0..=1 => format!("({n} row)"),
        n => format!("({n} rows)"),
    };

    let result = result_set
        .into_values()
        .or_raise(|| Error("failed to format result set".to_string()))?;

    let queue_secs =
        SignedDuration::from_nanos(progress.nanos_from_submitted - progress.nanos_from_started);
    let run_secs = SignedDuration::from_nanos(progress.nanos_from_started);
    let total_secs = duration;

    let queue_secs = Color::LightCyan.paint(format!("{:.3}s", queue_secs.as_secs_f64()));
    let run_secs = Color::LightCyan.paint(format!("{:.3}s", run_secs.as_secs_f64()));
    let total_secs = Color::LightCyan.paint(format!("{:.3}s", total_secs.as_secs_f64()));

    let queue = Color::LightGreen.paint("queue");
    let run = Color::LightGreen.paint("run");
    let total = Color::LightGreen.paint("total");

    Ok(format!(
        "{result:?}\n{num_rows}\ntime: {queue_secs} {queue} {run_secs} {run} {total_secs} {total}",
    ))
}

impl ScopeQLClient {
    pub fn new(endpoint: String) -> Self {
        let client = reqwest::ClientBuilder::new()
            .no_proxy()
            .build()
            .expect("failed to create HTTP client");

        ScopeQLClient {
            client: scopedb_client::Client::new(endpoint, client).unwrap(),
        }
    }

    pub async fn execute_statement(
        &self,
        statement_id: Uuid,
        statement: String,
        display_progress: impl Fn(&'static str, StatementEstimatedProgress),
    ) -> Result<String, Error> {
        let trace_id = statement_id.to_u128_le();
        let root = Span::root(
            func_path!(),
            SpanContext::new(TraceId(trace_id), SpanId::default()),
        );
        self.do_execute_statement(statement_id, statement, display_progress)
            .in_span(root)
            .await
    }

    async fn do_execute_statement(
        &self,
        statement_id: Uuid,
        statement: String,
        display_progress: impl Fn(&'static str, StatementEstimatedProgress),
    ) -> Result<String, Error> {
        let make_error = || {
            Error(format!(
                "failed to execute statement ({statement_id}): {statement}"
            ))
        };

        let start_time = jiff::Timestamp::now();
        display_progress("Submitting", StatementEstimatedProgress::default());

        let mut handle = self
            .client
            .statement(statement.clone())
            .with_statement_id(statement_id)
            .submit()
            .await
            .or_raise(make_error)?;

        loop {
            handle.fetch_once().await.or_raise(make_error)?;
        }
    }

    pub async fn cancel_statement(
        &self,
        statement_id: Uuid,
    ) -> Result<StatementCancelResponse, Error> {
        let trace_id = statement_id.to_u128_le();
        let root = Span::root(
            func_path!(),
            SpanContext::new(TraceId(trace_id), SpanId(std::random::random(..))),
        );

        let mut handle = self.client.statement_handle(statement_id);
        handle
            .cancel()
            .in_span(root)
            .await
            .or_raise(|| Error("failed to cancel statement".to_string()))
    }
}
