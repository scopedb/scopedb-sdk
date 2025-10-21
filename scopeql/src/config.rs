use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;
use serde::de::IntoDeserializer;
use toml_edit::DocumentMut;

use crate::global;

pub fn load_config(config_file: Option<PathBuf>) -> Config {
    // Layer 0: the config file
    let content = if let Some(file) = config_file {
        std::fs::read_to_string(&file)
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
        .filter(|(k, _)| k.starts_with("SCOPEQL_CONFIG_"))
        .collect::<std::collections::HashMap<_, _>>();

    fn set_toml_path(
        doc: &mut DocumentMut,
        key: &str,
        path: &str,
        value: toml_edit::Item,
    ) -> Vec<String> {
        let mut current = doc.as_item_mut();
        let mut warnings = vec![];

        let parts = path.split('.').collect::<Vec<_>>();
        let len = parts.len();
        assert!(len > 0, "path must not be empty");

        for part in parts.iter().take(len - 1) {
            if current.get(part).is_none() {
                warnings.push(format!(
                    "[key={key}] config path '{path}' has missing parent '{part}'; created",
                ));
            }
            current = &mut current[part];
        }

        current[parts[len - 1]] = value;
        warnings
    }

    let mut warnings = vec![];
    for (k, v) in env {
        let normalized_key = k.trim().to_lowercase();

        if normalized_key == "scopeql_config_default_connection" {
            let path = "default_connection";
            let value = toml_edit::value(v);
            warnings.extend(set_toml_path(&mut config, &k, path, value));
            continue;
        }

        if normalized_key.starts_with("scopeql_config_connections_")
            && normalized_key.ends_with("_endpoint")
        {
            let prefix_len = "scopeql_config_connections_".len();
            let suffix_len = "_endpoint".len();
            let name = &normalized_key[prefix_len..normalized_key.len() - suffix_len];
            let path = format!("connections.{name}.endpoint");
            let value = toml_edit::value(v);
            warnings.extend(set_toml_path(&mut config, &k, &path, value));
            continue;
        }

        warnings.push(format!(
            "ignore unknown environment variable {k} with value {v}"
        ));
    }
    for warning in warnings {
        global::display(format!("warning: {warning}"));
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
