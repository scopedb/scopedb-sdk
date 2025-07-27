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
    fn render_prompt_left(&self) -> Cow<str> {
        match self.endpoint {
            Some(ref endpoint) => format!("scopeql[{endpoint}]> ").into(),
            None => "scopeql[no-connect]> ".into(),
        }
    }

    fn render_prompt_right(&self) -> Cow<str> {
        "".into()
    }

    fn render_prompt_indicator(&self, _: PromptEditMode) -> Cow<str> {
        "".into()
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<str> {
        format!("{:width$}> ", " ", width = self.prompt_len()).into()
    }

    fn render_prompt_history_search_indicator(
        &self,
        history_search: PromptHistorySearch,
    ) -> Cow<str> {
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
