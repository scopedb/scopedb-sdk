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

#![feature(random)]
#![feature(string_from_utf8_lossy_owned)]

mod client;
mod command;
mod config;
mod error;
mod execute;
mod global;
mod load;
mod pretty;
mod repl;
mod tokenizer;
mod version;

use clap::Parser;

use crate::command::Args;
use crate::command::Command;
use crate::command::GenerateTarget;
use crate::command::Subcommand;
use crate::config::Config;
use crate::config::load_config;

fn main() {
    let cmd = Command::parse();

    let Args { config_file, quiet } = cmd.args();
    global::set_printer(quiet);

    match cmd.subcommand() {
        None => {
            let config = load_config(config_file);
            repl::entrypoint(&config);
        }
        Some(Subcommand::Run { files, statements }) => {
            // command definition ensures exactly one of statement or file is provided
            debug_assert!(
                files.is_empty() ^ statements.is_empty(),
                "files: {files:?}, statements: {statements:?}"
            );

            let config = load_config(config_file);
            for stmt in statements {
                execute::execute(&config, stmt);
            }
            for file in files {
                match std::fs::read_to_string(&file) {
                    Ok(content) => execute::execute(&config, content),
                    Err(err) => {
                        let file = file.display();
                        global::display(format!("failed to read script file {file}: {err}"));
                    }
                }
            }
        }
        Some(Subcommand::Generate { target, output }) => {
            let content = match target {
                GenerateTarget::Config => {
                    let config = Config::default();
                    toml::to_string(&config).expect("default config must be always valid")
                }
            };

            if let Some(output) = output {
                std::fs::write(&output, content).unwrap_or_else(|err| {
                    let output = output.display();
                    let target = match target {
                        GenerateTarget::Config => "configurations",
                    };
                    panic!("failed to write {target} to {output}: {err}")
                });
            } else {
                println!("{content}");
            }
        }
        Some(Subcommand::Load {
            file,
            transform,
            format,
        }) => {
            let config = load_config(config_file);
            load::load(&config, file, transform, format);
        }
    }
}
