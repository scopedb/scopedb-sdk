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

use clap::Arg;
use clap::ArgAction;
use clap::ArgGroup;
use clap::Command;

use crate::load::DataFormat;
use crate::version::version;

pub fn command() -> Command {
    Command::new("scopeql")
        .version(clap::crate_version!())
        .long_version(version())
        .styles(styled())
        .arg(
            Arg::new("quiet")
                .short('q')
                .long("quiet")
                .alias("silent")
                .action(ArgAction::SetTrue)
                .help("Suppress normal output"),
        )
        .arg(
            Arg::new("config-file")
                .long("config-file")
                .value_name("FILENAME")
                .value_hint(clap::ValueHint::FilePath)
                .value_parser(clap::value_parser!(PathBuf))
                .help("Run `scopeql` with the given config file"),
        )
        .subcommand_required(false)
        .subcommand(
            Command::new("run")
                .about("Run scopeql statements")
                .arg(
                    Arg::new("file")
                        .short('f')
                        .long("file")
                        .value_name("FILENAME")
                        .value_hint(clap::ValueHint::FilePath)
                        .value_parser(clap::value_parser!(PathBuf))
                        .action(ArgAction::Append)
                        .conflicts_with("statement")
                        .help("The scopeql script file to run"),
                )
                .arg(
                    Arg::new("statement")
                        .value_name("STATEMENT")
                        .value_hint(clap::ValueHint::Other)
                        .value_parser(clap::value_parser!(String))
                        .action(ArgAction::Append)
                        .help("The scopeql statement to run"),
                )
                .group(
                    ArgGroup::new("input")
                        .args(&["file", "statement"])
                        .required(true),
                ),
        )
        .subcommand(
            Command::new("gen")
                .about("Generate command-line interface utilities")
                .arg(
                    Arg::new("output")
                        .short('o')
                        .long("output")
                        .value_name("OUTPUT")
                        .value_hint(clap::ValueHint::FilePath)
                        .value_parser(clap::value_parser!(PathBuf))
                        .help("Output file path (if not specified, output to stdout)"),
                )
                .arg(
                    Arg::new("target")
                        .value_name("TARGET")
                        .value_parser(clap::value_parser!(GenerateTarget))
                        .help("The target to generate")
                        .required(true),
                ),
        )
        .subcommand(
            Command::new("load")
                .about("Perform a load operation of source with transformations")
                .arg(
                    Arg::new("file")
                        .short('f')
                        .long("file")
                        .value_name("FILENAME")
                        .value_hint(clap::ValueHint::FilePath)
                        .value_parser(clap::value_parser!(PathBuf))
                        .help("The file path to load the source from")
                        .required(true),
                )
                .arg(
                    Arg::new("transform")
                        .short('t')
                        .long("transform")
                        .value_name("TRANSFORM")
                        .value_parser(clap::value_parser!(String))
                        .help("The transformation to apply during the load")
                        .required(true),
                )
                .arg(
                    Arg::new("format")
                        .long("format")
                        .value_name("FORMAT")
                        .value_parser(clap::value_parser!(DataFormat))
                        .help("The source data format"),
                ),
        )
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
