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

mod client;
mod command;
mod error;
#[allow(dead_code)]
mod global;
mod repl;

fn main() {
    let cmd = Command::parse();

    let config = cmd.config();
    global::set_printer(config.quiet);

    match cmd.subcommand() {
        Subcommand::Repl => entrypoint(config),
    }
}
