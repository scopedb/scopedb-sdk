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

use clap::Parser;
use repl::entrypoint;

use crate::command::Command;
use crate::command::GenerateTarget;
use crate::command::Subcommand;
use crate::config::Config;
use crate::config::load_config;
use crate::execute::execute;

mod client;
mod command;
mod config;
mod error;
mod execute;
mod global;
mod pretty;
mod repl;
mod tokenizer;
mod version;

fn main() {
    let cmd = Command::parse();

    let args = cmd.args();
    global::set_printer(args.quiet);

    match cmd.subcommand() {
        Subcommand::Generate { output, target } => {
            let content = match target {
                GenerateTarget::Config => {
                    let config = Config::default();
                    toml::to_string(&config).expect("default config must be always valid")
                }
            };

            if let Some(output) = output {
                std::fs::write(&output, content).unwrap_or_else(|err| {
                    let target = match target {
                        GenerateTarget::Config => "configurations",
                    };
                    panic!("failed to write {target} to {}: {err}", output.display())
                });
            } else {
                println!("{content}");
            }
        }
        Subcommand::Repl => {
            let config = load_config(args.config_file);
            entrypoint(config)
        }
        Subcommand::Command { statements } => {
            let config = load_config(args.config_file);
            execute(config, statements.into_inner())
        }
    }
}
