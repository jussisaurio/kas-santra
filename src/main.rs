use std::sync::Arc;

use kassantra::Database;
use kassantra::ql::parser::Operation;
use tokio::{net::TcpListener, io::{AsyncReadExt, AsyncWriteExt}, sync::Mutex};

#[tokio::main]
async fn main() {
    let database = Arc::new(Mutex::new(Database::new("data")));
    let port_from_env = std::env::var("PORT").unwrap_or("8080".to_string());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port_from_env)).await.unwrap();
    println!("Listening on port {}", port_from_env);

    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let database_clone = database.clone(); // this clones the Arc, not the Mutex or the Database
        tokio::spawn(async move {
            // read everything that is sent to the socket, but no extra bytes
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            let buf_to_string = String::from_utf8(buf[0..n].to_vec()).unwrap();

            // ql::parser::Operation implements the from_str trait
            let response = match Operation::from_str(buf_to_string.as_ref()) {
                Ok(operation) => {
                    match operation {
                        Operation::Insert(key, value) => {
                            let mut database = database_clone.lock().await;
                            database.set(key, value);
                            Ok("OK".to_string())
                        }
                        Operation::Select (key) => {
                            let mut database = database_clone.lock().await;
                            match database.get(&key) {
                                Some(value) => Ok(value),
                                None => Ok("Key not found".to_string()),
                            }
                        }
                        Operation::Delete (key) => {
                            let mut database = database_clone.lock().await;
                            database.delete(&key);
                            Ok("OK".to_string())
                        }
                    }
                }
                Err(e) => {
                    Err(format!("Error: {}", e))
                }
            };
            match response {
                Ok(response) => {
                    socket.write(response.as_bytes()).await.unwrap();
                }
                Err(e) => {
                    socket.write(e.as_bytes()).await.unwrap();
                }
            }
        });
    }
}
