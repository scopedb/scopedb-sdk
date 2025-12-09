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

use clap::ArgAction;
use clap::ValueHint;

use crate::load::DataFormat;
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

    pub fn subcommand(&self) -> Option<Subcommand> {
        self.subcommand.clone()
    }
}

#[derive(Default, Debug, Clone, clap::Args)]
pub struct Args {
    /// Run `scopeql` with the given config file.
    #[clap(name = "FILE", long, value_hint = ValueHint::FilePath)]
    pub config_file: Option<PathBuf>,

    /// Suppress normal output.
    #[clap(short, long, alias = "silent", default_value = "false")]
    pub quiet: bool,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum Subcommand {
    #[clap(about = "Execute statements")]
    Run {
        /// The scopeql script file to run.
        #[clap(group = "input", short, long, value_hint = ValueHint::FilePath, action = ArgAction::Append)]
        files: Vec<PathBuf>,
        /// The statements to run.
        #[clap(group = "input", action = ArgAction::Append)]
        statements: Vec<String>,
    },
    #[clap(about = "Perform a load operation of source with transformations")]
    Load {
        /// The file path to load the source from.
        #[clap(short, long, value_hint = ValueHint::FilePath)]
        file: PathBuf,
        /// The transformation to apply during the load.
        #[clap(short, long)]
        transform: String,
        /// The source data format.
        #[clap(long, value_enum)]
        format: Option<DataFormat>,
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

fn styled() -> clap::builder::Styles {
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
