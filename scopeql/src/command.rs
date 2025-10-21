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

use std::path::PathBuf;

use clap::ValueHint;
use clap_stdin::MaybeStdin;

use crate::version::version;

#[derive(Debug, clap::Parser)]
#[command(name = "scopeql", version, long_version = version(), styles=styled())]
pub struct Command {
    #[clap(flatten)]
    config: Args,

    #[command(subcommand)]
    subcommand: Option<Subcommand>,
}

impl Command {
    pub fn args(&self) -> Args {
        self.config.clone()
    }

    pub fn subcommand(&self) -> Subcommand {
        self.subcommand.clone().unwrap_or(Subcommand::Repl)
    }
}

#[derive(Default, Debug, Clone, clap::Args)]
pub struct Args {
    /// Run `scopeql` with the given config file; if not specified, the default lookup logic is
    /// applied.
    #[clap(long, value_hint = ValueHint::FilePath)]
    pub config_file: Option<PathBuf>,

    /// Suppress normal output.
    #[clap(short, long, alias = "silent", default_value = "false")]
    pub quiet: bool,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum Subcommand {
    #[clap(about = "Start an interactive REPL [default]")]
    Repl,
    #[clap(visible_alias = "-c", about = "Execute only single statement and exit")]
    Command {
        /// The statements to execute ("-" to read from stdin).
        statements: MaybeStdin<String>,
    },
    #[clap(name = "gen", about = "Generate command-line interface utilities")]
    Generate {
        /// Output file path (if not specified, output to stdout).
        #[clap(short, long, value_hint = ValueHint::FilePath)]
        output: Option<PathBuf>,

        /// The target to generate.
        #[clap(value_enum)]
        target: GenerateTarget,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum GenerateTarget {
    /// Generate the default config file.
    Config,
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
