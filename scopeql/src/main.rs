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

use clap::Parser;
use repl::entrypoint;

use crate::command::Command;
use crate::command::Subcommand;
use crate::config::load_config;
use crate::execute::execute;

mod client;
mod command;
mod config;
mod error;
mod execute;
mod global;
mod repl;
mod tokenizer;
mod version;

fn main() {
    let cmd = Command::parse();

    let args = cmd.args();
    global::set_printer(args.quiet);

    let config = load_config(args.config_file);
    match cmd.subcommand() {
        Subcommand::Repl => entrypoint(config),
        Subcommand::Command { statements } => execute(config, statements.into_inner()),
    }
}
