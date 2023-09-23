use std::{sync::Arc, time::Duration};

use kassantra::Database;
use kassantra::ql::parser::Operation;
use tokio::{net::{TcpListener, TcpStream}, io::{AsyncReadExt, AsyncWriteExt}, sync::Mutex};
use rand::Rng;

#[tokio::main]
async fn main() {
    // get the first commandline argument, if it's 'client' execute run_client, otherwise execute run_server
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "client" {
        run_client().await;
    } else {
        run_server().await;
    }
}

fn ascii_letter_from_index(index: u8) -> char {
    let ascii_a = 97;
    let ascii_z = 122;
    let ascii_offset = ascii_z - ascii_a;
    let ascii_index = ascii_a + (index % ascii_offset);
    ascii_index as char
}

async fn run_client() {
    let port_from_env = std::env::var("PORT").unwrap_or("8080".to_string());
    // loop and bombard the tcp server with requests
    loop {
        // wait 10 milliseconds
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port_from_env)).await.unwrap();
        let mut random_number_generator = rand::thread_rng();
        let random_three_letter_key = format!("{}{}{}", 
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
        );
        let random_three_letter_value = format!("{}{}{}", 
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
        );

        // INSERT INTO the_table (key) VALUES ("value");
        let insert_command = format!("INSERT INTO the_table ({}) VALUES (\"{}\");\n", random_three_letter_key, random_three_letter_value);
        stream.write(insert_command.as_bytes()).await.unwrap();
        let mut buf = [0; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let buf_to_string = String::from_utf8(buf[0..n].to_vec()).unwrap();
        println!("Insert response: {}", buf_to_string);
    }
}

async fn run_server() {
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
