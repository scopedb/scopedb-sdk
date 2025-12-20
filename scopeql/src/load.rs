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

use std::collections::BTreeMap;
use std::fmt::Write;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use csv::ReaderBuilder;
use exn::Result;
use exn::ResultExt;
use scopedb_client::IngestData;

use crate::client::ScopeQLClient;
use crate::config::Config;
use crate::error::Error;
use crate::global;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum DataFormat {
    Csv,
    Json,
}

pub fn load(config: &Config, file: PathBuf, transform: String, format: Option<DataFormat>) {
    let endpoint = config
        .get_default_connection()
        .expect("no default connection in config");
    let endpoint = endpoint.endpoint().to_owned();
    let client = ScopeQLClient::new(endpoint);

    let format = match format {
        Some(format) => format,
        None => match file.extension().and_then(|s| s.to_str()) {
            Some("json") => DataFormat::Json,
            Some("csv") => DataFormat::Csv,
            _ => {
                log::error!("unknown data file format: {}", file.display());
                log::error!("Please specify the format using the --format option.");
                std::process::exit(1);
            }
        },
    };

    let content = match format {
        DataFormat::Csv => load_csv_data(file),
        DataFormat::Json => load_json_data(file),
    };

    let data = match content {
        Ok(rows) => IngestData::Json { rows },
        Err(err) => {
            log::error!("failed to load data: {err:?}");
            std::process::exit(1);
        }
    };

    let result = global::rt().block_on(client.load_data(data, transform));
    match result {
        Ok(result) => match result.num_rows_inserted {
            0 => log::info!("no rows were inserted"),
            1 => log::info!("successfully inserted 1 row"),
            n => log::info!("successfully inserted {n} rows"),
        },
        Err(err) => {
            log::error!("failed to load data: {err:?}");
            std::process::exit(1);
        }
    }
}

fn load_csv_data(file: PathBuf) -> Result<String, Error> {
    let make_error = || {
        Error(format!(
            "failed to load csv data from file: {}",
            file.display()
        ))
    };

    let file = File::open(&file).or_raise(make_error)?;
    let reader = BufReader::new(file);
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    let mut data = String::new();
    for result in reader.records() {
        let mut row = BTreeMap::new();
        let record = result.or_raise(make_error)?;
        for (i, field) in record.iter().enumerate() {
            row.insert(format!("col_{i}"), field.to_string());
        }
        write!(&mut data, "{}", serde_json::to_string(&row).unwrap()).unwrap();
    }
    Ok(data)
}

fn load_json_data(file: PathBuf) -> Result<String, Error> {
    let make_error = || {
        Error(format!(
            "failed to load json data from file: {}",
            file.display()
        ))
    };

    let file = File::open(&file).or_raise(make_error)?;
    let reader = BufReader::new(file);
    let reader = serde_json::Deserializer::from_reader(reader);

    let mut data = String::new();
    for row in reader.into_iter::<serde_json::Value>() {
        let row = row.or_raise(make_error)?;
        write!(&mut data, "{}", serde_json::to_string(&row).unwrap()).unwrap();
    }
    Ok(data)
}
