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

use std::fmt;
use std::sync::LazyLock;
use std::sync::OnceLock;

use tokio::runtime::Builder;
use tokio::runtime::Runtime;

pub fn rt() -> &'static Runtime {
    static RT: LazyLock<Runtime> = LazyLock::new(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("scopeql_thread")
            .worker_threads(1)
            .build()
            .expect("failed to create runtime")
    });

    &RT
}

static PRINTER: OnceLock<Printer> = OnceLock::new();

pub fn set_printer(quiet: bool) {
    if PRINTER.set(Printer::new(quiet)).is_err() {
        eprintln!("printer already set");
    }
}

pub fn display<M: fmt::Display>(message: M) {
    let p = PRINTER.get_or_init(|| Printer::new(false));
    p.display(message);
}

#[derive(Debug)]
struct Printer {
    quiet: bool,
}

impl Printer {
    fn new(quiet: bool) -> Self {
        Self { quiet }
    }

    fn display<M: fmt::Display>(&self, message: M) {
        if !self.quiet {
            println!("{message}");
        }
    }
}
