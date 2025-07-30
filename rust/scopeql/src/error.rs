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

use std::borrow::Borrow;

use exn::Exn;
use exn::Result;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct Error(pub String);

pub fn format_result<T: Serialize>(result: &Result<T, Error>) -> String {
    match result {
        Ok(result) => serde_json::to_string_pretty(result).unwrap(),
        Err(err) => format_error(err),
    }
}

pub fn format_error<E: Borrow<Exn<Error>>>(err: E) -> String {
    format!("{:?}", err.borrow())
}
