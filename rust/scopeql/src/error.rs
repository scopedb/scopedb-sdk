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
