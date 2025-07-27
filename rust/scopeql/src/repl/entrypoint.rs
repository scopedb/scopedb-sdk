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
use reedline::DefaultHinter;
use reedline::Emacs;
use reedline::FileBackedHistory;
use reedline::KeyCode;
use reedline::KeyModifiers;
use reedline::Reedline;
use reedline::ReedlineEvent;
use reedline::default_emacs_keybindings;

use crate::command::Config;

fn make_file_history() -> Option<FileBackedHistory> {
    let Some(home_dir) = dirs::home_dir() else {
        eprintln!("cannot get home directory; history disabled");
        return None;
    };

    let history_file = home_dir.join(".scopeql_history");
    let history = FileBackedHistory::with_file(1000, history_file).unwrap();
    Some(history)
}

pub fn entrypoint(_config: Config) {
    let mut keybindings = default_emacs_keybindings();
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::HistoryHintComplete,
    );

    let hinter = DefaultHinter::default().with_style(Style::new().fg(Color::DarkGray));

    let mut state = Reedline::create()
        .with_hinter(Box::new(hinter))
        .with_edit_mode(Box::new(Emacs::new(keybindings)));

    if let Some(history) = make_file_history() {
        state = state.with_history(Box::new(history));
    }

    loop {}
}
