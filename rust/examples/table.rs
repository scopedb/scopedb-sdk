mod common;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;
    let table = client.table("events").with_schema("public");

    println!("identifier: {}", table.identifier());

    let schema = table.table_schema().await?;
    for field in schema.fields() {
        println!("{}: {:?}", field.name(), field.data_type());
    }

    Ok(())
}
