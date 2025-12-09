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
use std::path::Path;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;
use serde::de::IntoDeserializer;
use toml_edit::DocumentMut;

use crate::global;

pub fn load_config(config_file: Option<&Path>) -> Config {
    // Layer 0: the config file
    let content = if let Some(file) = config_file {
        std::fs::read_to_string(file)
            .unwrap_or_else(|err| panic!("failed to read config file {}: {err}", file.display()))
    } else {
        let mut candidates = vec![];
        if let Some(home_dir) = dirs::home_dir() {
            candidates.push(home_dir.join(".scopeql").join("config.toml"));
            candidates.push(home_dir.join(".config").join("scopeql").join("config.toml"));
        }
        if let Some(config_dir) = dirs::config_dir() {
            candidates.push(config_dir.join("scopeql").join("config.toml"));
        }
        candidates.sort();
        candidates.dedup();

        candidates
            .into_iter()
            .find_map(|candidate| std::fs::read_to_string(candidate).ok())
            .unwrap_or_else(|| {
                toml::to_string(&Config::default()).expect("failed to serialize default config")
            })
    };

    let mut config = DocumentMut::from_str(&content)
        .unwrap_or_else(|err| panic!("failed to parse config content: {err}"));

    // Layer 1: environment variables
    let env = std::env::vars()
        .filter_map(|(k, v)| {
            let normalized_key = k.trim().to_lowercase();
            if normalized_key.starts_with("scopeql_config_") {
                let prefix_len = "scopeql_config_".len();
                let normalized_key = &normalized_key[prefix_len..];
                Some((normalized_key.to_owned(), v))
            } else {
                None
            }
        })
        .collect::<std::collections::HashMap<_, _>>();

    fn set_toml_path(doc: &mut DocumentMut, parts: &[&str], value: toml_edit::Item) {
        let mut current = doc.as_item_mut();

        let len = parts.len();
        assert!(len > 0, "path must not be empty");

        for part in parts.iter().take(len - 1) {
            current = &mut current[part];
        }

        current[parts[len - 1]] = value;
    }

    for (k, v) in env {
        if k == "default_connection" {
            let value = toml_edit::value(v);
            set_toml_path(&mut config, &["default_connection"], value);
            continue;
        }

        if k.starts_with("connections_") && k.ends_with("_endpoint") {
            let prefix_len = "connections_".len();
            let suffix_len = "_endpoint".len();
            let name = &k[prefix_len..k.len() - suffix_len];
            let value = toml_edit::value(v);
            set_toml_path(&mut config, &["connections", name, "endpoint"], value);
            continue;
        }

        global::display(format!(
            "ignore unknown environment variable {k} with value {v}"
        ));
    }

    Config::deserialize(config.into_deserializer()).expect("failed to deserialize config")
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    default_connection: String,

    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    connections: BTreeMap<String, ConnectionSpec>,
}

impl Config {
    pub fn get_connection(&self, name: &str) -> Option<&ConnectionSpec> {
        self.connections.get(name)
    }

    pub fn get_default_connection(&self) -> Option<&ConnectionSpec> {
        self.get_connection(&self.default_connection)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            default_connection: "default".to_string(),
            connections: BTreeMap::from([(
                "default".to_string(),
                ConnectionSpec {
                    endpoint: "http://127.0.0.1:6543".to_string(),
                },
            )]),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectionSpec {
    endpoint: String,
}

impl ConnectionSpec {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}
