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

use std::path::PathBuf;

use crate::command::GenerateTarget;
use crate::config::Config;
use crate::config::load_config;

fn main() {
    let cmd = command::command().get_matches();

    let quiet = cmd.get_flag("quiet");
    global::set_printer(quiet);

    let config_file = cmd.get_one::<PathBuf>("config-file").map(|p| p.as_path());
    match cmd.subcommand() {
        None => {
            let config = load_config(config_file);
            repl::entrypoint(&config);
        }
        Some(("run", args)) => {
            // command definition ensures exactly one of statement or file is provided

            let config = load_config(config_file);
            for cmd in args.get_many::<String>("statement").unwrap_or_default() {
                execute::execute(&config, cmd.to_string());
            }
            for file in args.get_many::<PathBuf>("file").unwrap_or_default() {
                match std::fs::read_to_string(&file) {
                    Ok(content) => execute::execute(&config, content),
                    Err(err) => global::display(format!(
                        "failed to read script file {}: {err}",
                        file.display()
                    )),
                }
            }
        }
        Some(("gen", args)) => {
            let target = args.get_one::<GenerateTarget>("target").unwrap();
            let output = args.get_one::<PathBuf>("output");

            let content = match target {
                GenerateTarget::Config => {
                    let config = Config::default();
                    toml::to_string(&config).expect("default config must be always valid")
                }
            };

            if let Some(output) = output {
                std::fs::write(output, content).unwrap_or_else(|err| {
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
        Some(("load", args)) => {
            let file = args.get_one::<PathBuf>("file").unwrap().to_owned();
            let transform = args.get_one::<String>("transform").unwrap().to_owned();
            let format = args.get_one::<load::DataFormat>("format").copied();

            let config = load_config(config_file);
            load::load(&config, file, transform, format);
        }
        Some((name, _)) => unreachable!("unknown subcommand: {}", name),
    }
}
