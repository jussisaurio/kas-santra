use std::{sync::Arc, time::Duration};

use kassantra::ql::parser::Operation;
use kassantra::Database;
use rand::Rng;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

#[tokio::main]
async fn main() {
    // console_subscriber::init();
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
    let requests = Mutex::new(0);
    let start_time = std::time::Instant::now();
    // loop and bombard the tcp server with requests
    loop {
        // sleep for some ms
        tokio::time::sleep(Duration::from_millis(1)).await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port_from_env))
            .await
            .unwrap();
        let mut random_number_generator = rand::thread_rng();
        let random_three_letter_key = format!(
            "{}{}{}",
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
        );
        let random_three_letter_value = format!(
            "{}{}{}",
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
            ascii_letter_from_index(random_number_generator.gen_range(0..26)),
        );

        // INSERT INTO the_table (key) VALUES ("value");
        let insert_command = format!(
            "INSERT INTO the_table ({}) VALUES (\"{}\");\n",
            random_three_letter_key, random_three_letter_value
        );
        stream.write(insert_command.as_bytes()).await.unwrap();
        let mut buf = [0; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let buf_to_string = String::from_utf8(buf[0..n].to_vec()).unwrap();
        println!("Insert response: {}", buf_to_string);
        let mut reqs = requests.lock().await;
        *reqs += 1;
        let cur_time = std::time::Instant::now();
        let rps = *reqs as f64 / cur_time.duration_since(start_time).as_secs_f64();
        println!("{} requests, {} rps", reqs, rps);
    }
}

async fn run_server() {
    let database = Arc::new(Database::new("data"));
    let port_from_env = std::env::var("PORT").unwrap_or("8080".to_string());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port_from_env))
        .await
        .unwrap();
    println!("Listening on port {}", port_from_env);

    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let database_clone = database.clone(); // this clones the Arc, not the Database
                                               // println!("Got a connection from {:?}", socket.peer_addr());
        tokio::spawn(async move {
            // read everything that is sent to the socket, but no extra bytes
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            let buf_to_string = String::from_utf8(buf[0..n].to_vec());

            if buf_to_string.is_err() {
                println!("Error: {}", buf_to_string.err().unwrap());
                return;
            }

            let buf_to_string = buf_to_string.unwrap();

            // ql::parser::Operation implements the from_str trait
            let response = match Operation::from_str(buf_to_string.as_ref()) {
                Ok(operation) => match operation {
                    Operation::Insert(key, value) => {
                        // println!("Inserting key: {}, value: {}", key, value);
                        database_clone.set(key, value).await;
                        // println!("Response: OK");
                        Ok("OK".to_string())
                    }
                    Operation::Select(key) => match database_clone.get(&key).await {
                        Some(value) => Ok(value),
                        None => Ok("Key not found".to_string()),
                    },
                    Operation::Delete(key) => {
                        database_clone.delete(&key).await;
                        Ok("OK".to_string())
                    }
                },
                Err(e) => Err(format!("Error: {}", e)),
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
