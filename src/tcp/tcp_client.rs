use std::{
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

use crate::{mqtt::Client, ENABLE_REGISTER};
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::Semaphore,
    time::Instant,
};

#[derive(Debug, Deserialize)]
pub struct TcpClient {
    pub send_data: Vec<u8>,
    pub len_index: usize,
    pub len_size: usize,
}

impl TcpClient {
    pub fn new(send_data: Vec<u8>, len_index: usize, len_size: usize) -> Self {
        Self {
            send_data,
            len_index,
            len_size,
        }
    }

    async fn process_read(mut reader: OwnedReadHalf) {
        let mut buf = [0; 1024];

        loop {
            match reader.read(&mut buf).await {
                Ok(0) => {
                    break;
                }

                Ok(n) => {
                    println!("收到数据: {}", String::from_utf8_lossy(&buf[..n]));
                }
                Err(e) => {
                    eprintln!("读取数据错误: {:?}", e);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct TcpClientData {
    pub mac: String,
    pub enable_register: bool,
    pub writer: OwnedWriteHalf,
}

impl TcpClientData {
    pub fn new(mac: String, enable_register: bool, writer: OwnedWriteHalf) -> Self {
        Self {
            mac,
            enable_register,
            writer,
        }
    }

    pub fn set_mac(&mut self, mac: String) {
        self.mac = mac;
    }

    pub fn get_mac(&self) -> String {
        self.mac.clone()
    }

    pub fn get_writer(&mut self) -> &mut OwnedWriteHalf {
        &mut self.writer
    }

    pub fn set_writer(&mut self, writer: OwnedWriteHalf) {
        self.writer = writer;
    }
}

impl Client<TcpClientData> for TcpClient {
    type Item = TcpClientData;

    async fn setup_clients(
        &self,
        client_data: &mut [TcpClientData],
        host: String,
        max_connect_per_second: usize,
    ) -> Result<Vec<TcpClientData>, Box<dyn std::error::Error>> {
        let mut clients = vec![];
        let mut enable_register = false;

        if let Some(enable) = ENABLE_REGISTER.get() {
            if *enable {
                enable_register = true;
            }
        }
        let semaphore = Arc::new(Semaphore::new(max_connect_per_second));

        for client in client_data.iter_mut() {
            let semaphore = Arc::clone(&semaphore);
            let permit = semaphore.acquire().await.unwrap();

            let start = Instant::now();
            let conn = TcpStream::connect(host.clone()).await?;

            let (reader, writer) = conn.into_split();

            clients.push(TcpClientData::new(
                client.get_mac(),
                enable_register,
                writer,
            ));
            tokio::spawn(async move {
                Self::process_read(reader).await;
            });

            let elapsed = start.elapsed();
            if elapsed < Duration::from_secs(1) {
                tokio::time::sleep(Duration::from_secs(1) - elapsed).await;
            }

            drop(permit);
        }

        Ok(clients)
    }

    async fn wait_for_connections(clients: &mut [Self::Item]) {
        for client in clients {
            let writer = client.get_writer();
            match writer.writable().await {
                Ok(_) => {
                    Self::on_connect_success(client).await;
                }
                Err(e) => {
                    println!("{}", format!("连接失败: {}", e))
                }
            }
        }
    }

    async fn on_connect_success(client: &mut TcpClientData) {
        let writer = client.get_writer();

        if let Some(enable) = ENABLE_REGISTER.get() {
            if *enable {
                // 发送注册包
                match writer.write("abc".as_bytes()).await {
                    Ok(_) => todo!(),
                    Err(_) => todo!(),
                }
            }
        }
    }

    async fn spawn_message(
        &self,
        clients: Vec<Self::Item>,
        counter: Arc<AtomicU32>,
        thread_size: usize,
        setting_send_interval: u64,
    ) {
        todo!()
    }
}
