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
use crate::config::Config;
use crate::global;
use crate::tokenizer::TokenKind;
use crate::tokenizer::run_tokenizer;

pub fn execute(config: &Config, stmts: String) {
    let endpoint = config
        .get_default_connection()
        .expect("no default connection in config");
    let endpoint = endpoint.endpoint().to_owned();
    let client = ScopeQLClient::new(endpoint);

    let tokens = match run_tokenizer(&stmts) {
        Ok(tokens) => tokens,
        Err(err) => {
            log::error!("failed to parse statements: {err:?}");
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
        log::info!("no statements provided");
        return;
    }

    for range in stmts_range {
        let stmt = stmts[range].to_string();
        let id = uuid::Uuid::now_v7();
        log::info!("executing statement {id}: {stmt}");

        match global::rt().block_on(client.execute_statement(id, stmt, |_, _| ())) {
            Ok(output) => log::info!("statement {id} results in:\n{output}"),
            Err(err) => {
                log::error!("failed to execute statement: {err:?}");
                std::process::exit(1);
            }
        }
    }
}
