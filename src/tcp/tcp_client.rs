use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{command::BenchmarkConfig, mqtt::Client, TCP_CLIENT_CONTEXT};
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedReadHalf, TcpStream},
    sync::Semaphore,
    time::Instant,
};

#[derive(Debug, Clone)]
pub struct TcpSendData {
    pub send_data: Arc<Vec<u8>>,
    pub len_index: usize,
    pub len_size: usize,
    pub enable_register: bool,
}

impl TcpSendData {
    pub fn new(
        send_data: Arc<Vec<u8>>,
        len_index: usize,
        len_size: usize,
        enable_register: bool,
    ) -> Self {
        Self {
            send_data,
            len_index,
            len_size,
            enable_register,
        }
    }

    pub fn get_enable_register(&self) -> bool {
        self.enable_register
    }

    pub fn set_enable_register(&mut self, enable_register: bool) {
        self.enable_register = enable_register
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

#[derive(Debug, Clone, Deserialize)]
pub struct TcpClientData {
    pub mac: String,
    pub is_connected: bool,
    pub is_register: bool,
}

impl TcpClientData {
    pub fn new(mac: String) -> Self {
        Self {
            mac,
            is_connected: false,
            is_register: false,
        }
    }

    pub fn set_mac(&mut self, mac: String) {
        self.mac = mac;
    }

    pub fn get_mac(&self) -> String {
        self.mac.clone()
    }

    pub fn set_is_connected(&mut self, is_connected: bool) {
        self.is_connected = is_connected;
    }

    pub fn get_is_connected(&self) -> bool {
        self.is_connected
    }

    pub fn set_is_register(&mut self, is_register: bool) {
        self.is_register = is_register;
    }

    pub fn get_is_register(&self) -> bool {
        self.is_register
    }
}

impl Client<TcpSendData, TcpClientData> for TcpSendData {
    type Item = TcpClientData;

    async fn setup_clients(
        &self,
        config: &BenchmarkConfig<TcpSendData, TcpClientData>,
    ) -> Result<Vec<TcpClientData>, Box<dyn std::error::Error>> {
        let clients = vec![];

        let semaphore = Arc::new(Semaphore::new(config.get_max_connect_per_second()));

        for client in config.get_clients() {
            let semaphore = Arc::clone(&semaphore);
            let permit = semaphore.acquire().await.unwrap();

            let start = Instant::now();
            let conn = TcpStream::connect(config.get_broker()).await?;

            let (reader, writer) = conn.into_split();

            TCP_CLIENT_CONTEXT.insert(client.get_mac(), writer);
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

    async fn wait_for_connections(&self, clients: &mut [TcpClientData]) {
        for client in clients {
            if let Some(writer) = TCP_CLIENT_CONTEXT.get(&client.get_mac()) {
                match writer.writable().await {
                    Ok(_) => {
                        self.on_connect_success(client).await;
                    }
                    Err(e) => {
                        println!("{}", format!("连接失败: {}", e))
                    }
                }
            }
        }
    }

    async fn on_connect_success(&self, client: &mut TcpClientData) {
        if let Some(mut writer) = TCP_CLIENT_CONTEXT.get_mut(&client.get_mac()) {
            if self.get_enable_register() {
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
        config: &BenchmarkConfig<TcpSendData, TcpClientData>,
    ) {
        // 确定每个线程处理的客户端数量
        let startup_thread_size = clients.len() / config.thread_size
        + if clients.len() % config.thread_size != 0 {
            1
        } else {
            0
        };
        // 按线程大小将客户端分组
        let clients_group = clients.chunks(startup_thread_size);

        for group in clients_group {
            let mut groups = group.to_vec();
            let msg_value = Arc::clone(&self.send_data);
            let counter = counter.clone();
            let send_interval = config.get_send_interval();
            let enable_register = self.get_enable_register();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(send_interval));
                loop {
                    interval.tick().await;
                    for client in groups.iter_mut() {
                        if let Some(mut writer) = TCP_CLIENT_CONTEXT.get_mut(&client.get_mac()) {
                            if enable_register && writer.writable().await.is_ok() {
                                let _ = writer.write(&msg_value).await;
                                counter.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                }
            });
        }
    }
}
