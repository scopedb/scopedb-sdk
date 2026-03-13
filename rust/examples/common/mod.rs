use scopedb_client::Client;

pub fn endpoint() -> String {
    std::env::var("SCOPEDB_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6543".to_string())
}

pub fn client() -> Result<Client, scopedb_client::Error> {
    Client::new(endpoint(), reqwest::Client::new())
}
