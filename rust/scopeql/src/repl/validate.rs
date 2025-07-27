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
