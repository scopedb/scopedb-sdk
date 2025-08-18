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

use reedline::ValidationResult;
use reedline::Validator;

use crate::repl::tokenizer::TokenKind;
use crate::repl::tokenizer::run_tokenizer;

pub struct ScopeQLValidator;

impl Validator for ScopeQLValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        if line.trim().starts_with("\\") {
            return ValidationResult::Complete;
        }

        let Ok(tokens) = run_tokenizer(line) else {
            // throw out the line if it's not valid; handle error in the repl
            return ValidationResult::Complete;
        };

        let mut in_transaction = false;

        for token in &tokens {
            match token.kind {
                TokenKind::BEGIN => in_transaction = true,
                TokenKind::END => in_transaction = false,
                TokenKind::SemiColon if !in_transaction => return ValidationResult::Complete,
                _ => {}
            }
        }

        ValidationResult::Incomplete
    }
}
