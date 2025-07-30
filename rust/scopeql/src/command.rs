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

#[derive(Debug, clap::Parser)]
#[command(name = "scopeql", version, styles=styled())]
pub struct Command {
    #[clap(flatten)]
    config: Config,

    #[command(subcommand)]
    subcommand: Option<Subcommand>,
}

impl Command {
    pub fn config(&self) -> Config {
        self.config.clone()
    }

    pub fn subcommand(&self) -> Subcommand {
        self.subcommand.clone().unwrap_or(Subcommand::Repl)
    }
}

#[derive(Default, Debug, Clone, clap::Args)]
pub struct Config {
    /// The endpoint of ScopeDB service to connect to.
    #[clap(short, long, default_value = "http://localhost:6543")]
    pub endpoint: String,

    /// Suppress normal output.
    #[clap(short, long, alias = "silent", default_value = "false")]
    pub quiet: bool,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum Subcommand {
    #[clap(about = "Start an interactive REPL [default]")]
    Repl,
}

pub fn styled() -> clap::builder::Styles {
    use anstyle::AnsiColor;
    use anstyle::Color;
    use anstyle::Style;

    let default = Style::new();
    let bold = default.bold();
    let bold_underline = bold.underline();

    clap::builder::Styles::styled()
        .usage(bold_underline.fg_color(Some(Color::Ansi(AnsiColor::BrightGreen))))
        .header(bold_underline.fg_color(Some(Color::Ansi(AnsiColor::BrightGreen))))
        .valid(bold_underline.fg_color(Some(Color::Ansi(AnsiColor::Green))))
        .literal(bold.fg_color(Some(Color::Ansi(AnsiColor::BrightCyan))))
        .invalid(bold.fg_color(Some(Color::Ansi(AnsiColor::Red))))
        .error(bold.fg_color(Some(Color::Ansi(AnsiColor::Red))))
        .placeholder(default.fg_color(Some(Color::Ansi(AnsiColor::Cyan))))
}
