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

use nu_ansi_term::Color;
use nu_ansi_term::Style;
use reedline::Highlighter;
use reedline::StyledText;

use crate::repl::tokenizer::run_tokenizer;

pub struct ScopeQLHighlighter;

impl Highlighter for ScopeQLHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled_text = StyledText::new();
        styled_text.push((Style::default(), line.to_owned()));
        if let Ok(tokens) = run_tokenizer(line) {
            for token in tokens {
                if token.kind.is_literal() {
                    styled_text.style_range(
                        token.span.start,
                        token.span.end,
                        Style::new().fg(Color::LightCyan),
                    );
                } else if token.kind.is_symbol() {
                    styled_text.style_range(
                        token.span.start,
                        token.span.end,
                        Style::new().fg(Color::Yellow),
                    );
                } else if token.kind.is_keyword() {
                    styled_text.style_range(
                        token.span.start,
                        token.span.end,
                        Style::new().fg(Color::LightGreen),
                    );
                } else {
                    styled_text.style_range(
                        token.span.start,
                        token.span.end,
                        Style::new().fg(Color::LightCyan),
                    );
                }
            }
        }
        styled_text
    }
}
