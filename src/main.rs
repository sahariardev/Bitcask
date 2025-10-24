use crate::entry::key::Serializable;
use crate::kv_store::KVStore;
use std::io::Error;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

mod entry;
mod key_directory;
mod kv_store;
mod segment;
mod segments;
mod store;
mod time_based_id_generator;
mod util;

impl Serializable for String {
    fn serialize(&self) -> Result<Vec<u8>, Error> {
        Ok(self.as_bytes().to_vec())
    }
    fn deserialize(bytes: Vec<u8>) -> Result<String, Error> {
        Ok(String::from_utf8(bytes).unwrap())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Listening on port 6379");

    let dir = ".";
    let store = Arc::new(Mutex::new(KVStore::<String>::new(
        dir.to_string(),
        1024 * 1024,
    )?));

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("SIGINT received, shutting down");
        std::process::exit(0);
    });

    loop {
        let (socket, addr) = listener.accept().await?;

        let store_clone = store.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, store_clone).await {
                eprintln!("Error handling client {}: {:?}", addr, e);
            }
        });
    }
}

async fn handle_client(
    socket: tokio::net::TcpStream,
    store: Arc<Mutex<KVStore<String>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            break;
        }

        println!("Received: {}", line.trim());

        let command = Command::parse(&line);

        let response_message = match command {
            Command::Get(key) => {
                let store = store.lock().await;
                if let Some(value) = store.get(key) {
                    String::from_utf8(value).unwrap()
                } else {
                    "Error Key not found".to_string()
                }
            }
            Command::Set(key, value) => {
                let mut store = store.lock().await;
                store
                    .put(key.clone(), value.clone().into_bytes())
                    .expect("Error Key store");
                "OK".to_string()
            }
            Command::Delete(key) => {
                let mut store = store.lock().await;
                store.delete(key.clone()).expect("Error Key store");
                "OK".to_string()
            }
            Command::Unknown => "Unknown Command".to_string(),
        };

        let response = response_message + "\n";
        writer.write_all(response.as_bytes()).await?;
    }

    Ok(())
}

#[derive(Debug)]
pub enum Command {
    Get(String),
    Set(String, String),
    Delete(String),
    Unknown,
}

impl Command {
    pub fn parse(input: &str) -> Command {
        let parts: Vec<&str> = input.trim().splitn(3, ' ').collect();

        match parts.as_slice() {
            ["GET", key] => Command::Get(key.to_string()),
            ["SET", key, value] => Command::Set(key.to_string(), value.to_string()),
            ["DELETE", key] => Command::Delete(key.to_string()),
            _ => Command::Unknown,
        }
    }
}
