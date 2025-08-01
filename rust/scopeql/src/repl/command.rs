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

use clap::Parser;
use clap::Subcommand;

use crate::client::ScopeQLClient;
use crate::error::format_result;
use crate::global::rt;

#[derive(Debug, Parser)]
#[command(multicall = true)]
pub struct ReplCommand {
    #[command(subcommand)]
    pub cmd: ReplSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum ReplSubCommand {
    /// Cancel the statement with the given ID.
    #[command(name = "cancel")]
    Cancel(CommandCancel),
    /// Connect to another ScopeDB server.
    #[command(name = "connect")]
    Connect(CommandConnect),
}

#[derive(Debug, Parser)]
pub struct CommandConnect {
    /// The endpoint of the server to connect to.
    #[arg(value_name = "ENDPOINT")]
    pub endpoint: String,
}

#[derive(Debug, Parser)]
pub struct CommandCancel {
    /// The ID of the statement to cancel.
    #[arg(value_name = "STATEMENT_ID")]
    pub statement_id: String,
}

impl CommandCancel {
    pub fn run(self, client: Option<&ScopeQLClient>) {
        let statement_id = &self.statement_id;
        let statement_id = match uuid::Uuid::try_parse(statement_id) {
            Ok(statement_id) => statement_id,
            Err(err) => {
                println!("error: invalid statement ID {statement_id:?}\n{err}");
                return;
            }
        };

        let Some(client) = client.as_ref() else {
            println!("error: cancel statement without endpoint");
            return;
        };

        let output = rt().block_on(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => None,
                output = client.cancel_statement(statement_id) => Some(output),
            }
        });

        match output {
            Some(output) => {
                let output = format_result(&output);
                println!("{output}");
            }
            None => {
                println!("interrupted");
            }
        }
    }
}
