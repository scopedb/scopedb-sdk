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

mod append_stream;
mod client;
mod error;
mod ingest_stream;
mod protocol;
mod result;
mod statement;
mod table;

pub use append_stream::AppendAdmissionResult;
pub use append_stream::AppendBatchFailureAction;
pub use append_stream::AppendBatchFailureEvent;
pub use append_stream::AppendCircuitBreakerOptions;
pub use append_stream::AppendCircuitState;
pub use append_stream::AppendDeliveryOutcome;
pub use append_stream::AppendDeliveryReport;
pub use append_stream::AppendDroppedRows;
pub use append_stream::AppendFailurePolicy;
pub use append_stream::AppendLastFailure;
pub use append_stream::AppendStream;
pub use append_stream::AppendStreamBuilder;
pub use append_stream::AppendStreamState;
pub use append_stream::AppendStreamStats;
pub use append_stream::AppendTrySendError;
pub use client::Client;
pub use error::Error;
pub use error::ErrorKind;
pub use ingest_stream::IngestStream;
pub use ingest_stream::IngestStreamBuilder;
pub use protocol::AppendErrorDetails;
pub use protocol::AppendRowError;
pub use protocol::AppendRowsResult;
pub use protocol::AppendState;
pub use protocol::CatalogListOptions;
pub use protocol::CatalogPage;
pub use protocol::DataType;
pub use protocol::DatabaseResource;
pub use protocol::IngestResult;
pub use protocol::SchemaResource;
pub use protocol::StatementCancelResult;
pub use protocol::StatementEstimatedProgress;
pub use protocol::StatementProgress;
pub use protocol::StatementStatus;
pub use protocol::StatementStatusCancelled;
pub use protocol::StatementStatusFailed;
pub use protocol::StatementStatusFinished;
pub use protocol::StatementStatusPending;
pub use protocol::StatementStatusRunning;
pub use protocol::TableColumnSpec;
pub use protocol::TableDistinctSpec;
pub use protocol::TableResource;
pub use protocol::TableResourceSummary;
pub use protocol::TableSpec;
/// The HTTP client version used by this SDK.
pub use reqwest;
pub use result::FieldSchema;
pub use result::ResultSet;
pub use result::Schema;
pub use result::Value;
pub use statement::Statement;
pub use statement::StatementHandle;
pub use table::Table;
