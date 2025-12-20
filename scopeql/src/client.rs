// Copyright 2024 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use exn::Result;
use exn::ResultExt;
use jiff::SignedDuration;
use nu_ansi_term::Color;
use scopedb_client::IngestData;
use scopedb_client::IngestResult;
use scopedb_client::ResultSet;
use scopedb_client::StatementCancelResult;
use scopedb_client::StatementEstimatedProgress;
use scopedb_client::StatementStatus;
use scopedb_client::Value;
use uuid::Uuid;

use crate::error::Error;
use crate::pretty::pretty_print;

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

    let header = result_set
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect::<Vec<_>>();

    let rows = result_set
        .into_values()
        .or_raise(|| Error("failed to convert result rows".to_string()))?;

    // @see https://docs.rs/comfy-table/7.1.3/comfy_table/presets/index.html
    const TABLE_STYLE_PRESET: &str = "||--+-++|    ++++++";
    let mut table = comfy_table::Table::new();
    table.load_preset(TABLE_STYLE_PRESET);
    table.set_header(header);
    for row in rows {
        let row = row
            .into_iter()
            .map(|v| match v {
                Value::Null
                | Value::Int(_)
                | Value::UInt(_)
                | Value::Float(_)
                | Value::Timestamp(_)
                | Value::Interval(_)
                | Value::Boolean(_)
                | Value::Binary(_) => v.to_string(),
                Value::String(s) => s,
                Value::Array(s) | Value::Object(s) | Value::Any(s) => {
                    const MAX_COMPACT_LEN: usize = 64;
                    if s.len() > MAX_COMPACT_LEN {
                        pretty_print(&s)
                    } else {
                        s
                    }
                }
            })
            .collect::<Vec<_>>();
        table.add_row(row);
    }

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
        "{table}\n{num_rows}\ntime: {queue_secs} {queue} {run_secs} {run} {total_secs} {total}",
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

    pub async fn load_data(
        &self,
        data: IngestData,
        transform: String,
    ) -> Result<IngestResult, Error> {
        let format = data.format();
        let make_error = || Error(format!("failed to load {format} data: {transform}"));

        self.client
            .insert(data, transform.clone())
            .await
            .or_raise(make_error)
    }

    pub async fn execute_statement(
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

        let statement = statement.clone();
        let mut handle = self
            .client
            .statement(statement)
            .with_statement_id(statement_id)
            .submit()
            .await
            .or_raise(make_error)?;

        loop {
            handle.fetch_once().await.or_raise(make_error)?;

            const DEFAULT_FETCH_INTERVAL: Duration = Duration::from_millis(42);

            // SAFETY: after successfully fetch once, the status field is guaranteed to be set
            match handle.status().unwrap() {
                StatementStatus::Pending(s) => {
                    display_progress("Pending", s.progress.clone());
                }
                StatementStatus::Running(s) => {
                    display_progress("Running", s.progress.clone());
                }
                StatementStatus::Finished(s) => {
                    let elapsed = start_time.duration_until(jiff::Timestamp::now());
                    return format_result_set(s.result_set(), elapsed, s.progress.clone());
                }
                StatementStatus::Failed(s) => {
                    return Ok(s.message.clone());
                }
                StatementStatus::Cancelled(s) => {
                    return Ok(s.message.clone());
                }
            }

            tokio::time::sleep(DEFAULT_FETCH_INTERVAL).await;
        }
    }

    pub async fn cancel_statement(
        &self,
        statement_id: Uuid,
    ) -> Result<StatementCancelResult, Error> {
        let make_error = || Error(format!("failed to cancel statement: {statement_id}"));
        let mut handle = self.client.statement_handle(statement_id);
        handle.cancel().await.or_raise(make_error)
    }
}
