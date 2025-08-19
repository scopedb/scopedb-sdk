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

use crate::client::ScopeQLClient;
use crate::command::Config;
use crate::error::Error;
use crate::error::format_error;
use crate::global;
use crate::tokenizer::TokenKind;

pub fn execute(config: Config, stmts: String) {
    let client = ScopeQLClient::new(config.endpoint);

    let tokens = match crate::tokenizer::run_tokenizer(&stmts) {
        Ok(tokens) => tokens,
        Err(err) => {
            let err = err.raise(Error("failed to parse statements".to_string()));
            global::display(format!("{err:?}"));
            std::process::exit(1);
        }
    };

    let mut stmts_range = vec![];
    let mut start = 0;
    let mut in_transaction = false;
    let mut in_statement = true;

    for token in &tokens {
        // transactions
        match token.kind {
            TokenKind::BEGIN => in_transaction = true,
            TokenKind::END => in_transaction = false,
            _ => {}
        }

        // semicolons
        match token.kind {
            TokenKind::SemiColon => {
                if in_statement && !in_transaction {
                    let end = token.span.start;
                    stmts_range.push(start..end);
                    start = token.span.end;
                    in_statement = false;
                }
            }
            _ => {
                if !in_statement {
                    start = token.span.start;
                    in_statement = true;
                }
            }
        }
    }

    if start < stmts.len() {
        stmts_range.push(start..stmts.len());
    }

    if stmts_range.is_empty() {
        global::display("No statements provided.");
        return;
    }

    global::display("Starting execute statements ...");

    for range in stmts_range {
        let stmt = stmts[range].to_string();
        let id = uuid::Uuid::now_v7();
        global::display(format!("statement_id: {id}"));

        let result = global::rt().block_on(client.execute_statement(id, stmt, |_, _| ()));

        match result {
            Ok(output) => global::display(output),
            Err(err) => {
                global::display(format_error(err));
                std::process::exit(1);
            }
        }
    }
}
