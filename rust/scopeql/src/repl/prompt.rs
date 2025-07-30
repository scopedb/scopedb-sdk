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

use std::borrow::Cow;

use reedline::Prompt;
use reedline::PromptEditMode;
use reedline::PromptHistorySearch;
use reedline::PromptHistorySearchStatus;

#[derive(Default, Debug)]
pub struct CommandLinePrompt {
    endpoint: Option<String>,
}

impl CommandLinePrompt {
    pub fn set_endpoint(&mut self, endpoint: Option<String>) {
        self.endpoint = endpoint;
    }

    fn prompt_len(&self) -> usize {
        "scopeql[]".len()
            + match self.endpoint {
                None => "no-connect".len(),
                Some(ref endpoint) => endpoint.len(),
            }
    }
}

impl Prompt for CommandLinePrompt {
    fn render_prompt_left(&'_ self) -> Cow<'_, str> {
        match self.endpoint {
            Some(ref endpoint) => format!("scopeql[{endpoint}]> ").into(),
            None => "scopeql[no-connect]> ".into(),
        }
    }

    fn render_prompt_right(&'_ self) -> Cow<'_, str> {
        "".into()
    }

    fn render_prompt_indicator(&'_ self, _: PromptEditMode) -> Cow<'_, str> {
        "".into()
    }

    fn render_prompt_multiline_indicator(&'_ self) -> Cow<'_, str> {
        format!("{:width$}> ", " ", width = self.prompt_len()).into()
    }

    fn render_prompt_history_search_indicator(
        &'_ self,
        history_search: PromptHistorySearch,
    ) -> Cow<'_, str> {
        // NOTE: This is copied from the DefaultPrompt implementation.
        let PromptHistorySearch { term, status } = history_search;
        let prefix = match status {
            PromptHistorySearchStatus::Passing => "",
            PromptHistorySearchStatus::Failing => "failing ",
        };
        Cow::Owned(format!("({prefix}reverse-search: {term}) "))
    }

    fn get_prompt_color(&self) -> reedline::Color {
        reedline::Color::DarkGrey
    }

    fn get_prompt_multiline_color(&self) -> nu_ansi_term::Color {
        nu_ansi_term::Color::DarkGray
    }
}
