use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{command::BenchmarkConfig, mqtt::Client, TCP_CLIENT_CONTEXT};
use anyhow::Error;
use serde::{Deserialize, Deserializer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedReadHalf, TcpStream},
    sync::Semaphore,
    time::sleep,
};
use tracing::info;

#[derive(Debug, Clone, Deserialize)]
pub struct TcpSendData {
    #[serde(deserialize_with = "deserialize_bytes")]
    pub data: Vec<u8>,
}

// 修改函数名以避免与标准库中的 deserialize 冲突，并明确指定是什么类型的反序列化
pub fn deserialize_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let bytes = hex::decode(s)
        .map_err(|e| serde::de::Error::custom(format!("无效的十六进制字符串: {}", e)))?;
    Ok(bytes)
}

#[derive(Debug, Clone)]
pub struct TcpClientContext {
    pub send_data: Arc<TcpSendData>,
    pub enable_register: bool,
}

impl TcpClientContext {
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
                    let hex = hex::encode(&buf[..n]);
                }
                Err(e) => {
                    eprintln!("读取数据错误: {:?}", e);
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TcpClient {
    pub mac: String,
    #[serde(skip)]
    pub is_connected: bool,
    #[serde(skip)]
    pub is_register: bool,
}

impl TcpClient {
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

impl Client<TcpSendData, TcpClient> for TcpClientContext {
    type Item = TcpClient;

    async fn setup_clients(
        &self,
        config: &BenchmarkConfig<TcpSendData, TcpClient>,
    ) -> Result<Vec<TcpClient>, Box<dyn std::error::Error>> {
        let mut clients = config.get_clients().clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel(clients.len());

        let semaphore = Arc::new(Semaphore::new(config.get_max_connect_per_second()));

        for (idx, client) in clients.iter().enumerate() {
            let permit = semaphore.acquire().await?;
            let broker = config.broker.clone();
            let mac = client.get_mac();
            let tx = tx.clone();

            tokio::spawn(async move {
                if let Ok(conn) = TcpStream::connect(broker).await {
                    let (reader, writer) = conn.into_split();
                    TCP_CLIENT_CONTEXT.insert(mac, writer);
                    tokio::spawn(async move {
                        Self::process_read(reader).await;
                    });
                    let _ = tx.send((idx, true)).await;
                };
            });

            drop(permit);
            if clients.len() % config.get_max_connect_per_second() == 0 {
                sleep(Duration::from_secs(1)).await;
            }
        }

        drop(tx);
        info!("等待所有客户端连接...");
        while let Some((idx, success)) = rx.recv().await {
            if success {
                clients[idx].set_is_connected(true);
            }
        }
        info!("所有客户端连接成功");
        Ok(clients)
    }

    async fn wait_for_connections(&self, clients: &mut [TcpClient]) {
        for client in clients {
            let _ = self.on_connect_success(client).await;
        }
    }

    async fn on_connect_success(&self, client: &mut TcpClient) -> Result<(), Error> {
        if let Some(mut writer) = TCP_CLIENT_CONTEXT.get_mut(&client.get_mac()) {
            if self.get_enable_register() {
                let reg_code = hex::decode(client.get_mac()).unwrap();
                writer.write_all(&reg_code).await?;
            }
        } else {
            println!("没有找到客户端: {}", client.get_mac());
        }
        Ok(())
    }

    async fn spawn_message(
        &self,
        clients: Vec<Self::Item>,
        counter: Arc<AtomicU32>,
        config: &BenchmarkConfig<TcpSendData, TcpClient>,
    ) {
        // 确定每个线程处理的客户端数量
        let clients_per_thread = (clients.len() + config.thread_size - 1) / config.thread_size;
        let clients_group = clients.chunks(clients_per_thread);

        for group in clients_group {
            let mut groups = group.to_vec();
            let msg_value = Arc::clone(&self.send_data);
            let counter = counter.clone();
            let send_interval = config.get_send_interval();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(send_interval));
                loop {
                    interval.tick().await;
                    for client in groups.iter_mut() {
                        if let Some(mut writer) = TCP_CLIENT_CONTEXT.get_mut(&client.get_mac()) {
                            if writer.writable().await.is_ok() {
                                let _ = writer.write_all(&msg_value.data).await;
                                counter.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                }
            });
        }
    }
}
