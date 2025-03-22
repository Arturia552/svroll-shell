use std::{fmt::Debug, fs::File};

use anyhow::{Context, Ok, Result};
use clap::{arg, Parser, ValueEnum};
use serde::de::DeserializeOwned;
use tokio::fs;
use tracing::info;

use crate::tcp::tcp_client::{TcpClient, TcpSendData};

#[derive(Debug, Clone, ValueEnum, PartialEq, Eq)]
pub enum Protocol {
    Mqtt,
    Tcp,
}

#[derive(Debug, Clone, ValueEnum, PartialEq, Eq)]
pub enum Flag {
    True,
    False,
}

#[derive(Parser, Debug)]
pub struct CommandConfig {
    /// 设置需发送的数据文件路径,默认为当前目录下的data.json
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = "./data.json",
        help = "设置需发送的数据文件路径,默认为当前目录下的data.json"
    )]
    pub data_file: String,

    /// 使用的数据传输协议，默认为mqtt
    #[arg(
        short,
        long,
        value_name = "PROTOCOL",
        value_enum,
        default_value = "mqtt",
        help = "使用的数据传输协议，默认为mqtt"
    )]
    pub protocol_type: Protocol,

    /// 设置客户端文件,默认为当前目录下的client.csv
    #[arg(
        short = 'c',
        long,
        value_name = "FILE",
        default_value = "./client.csv",
        help = "设置客户端文件,默认为当前目录下的client.csv"
    )]
    pub client_file: String,

    /// 设置启动协程数量,默认为200
    #[arg(short = 't', long, value_parser = clap::value_parser!(usize), default_value_t = 200,help="设置启动协程数量,默认为200")]
    pub thread_size: usize,

    /// 设置是否启用注册包机制
    #[arg(
        short = 'r',
        long,
        value_name = "Flag",
        value_enum,
        default_value = "true",
        help = "设置是否启用注册包机制,默认为false"
    )]
    pub enable_register: Flag,

    /// 设置mqtt broker地址,默认为mqtt://localhost:1883
    #[arg(
        short = 'b',
        long,
        value_name = "BROKER",
        default_value = "mqtt://localhost:1883",
        help = "设置mqtt broker地址,默认为mqtt://localhost:1883"
    )]
    pub broker: String,

    /// 每秒最多启动连接数
    #[arg(short = 'm', long, value_parser = clap::value_parser!(usize), default_value_t = 100,help="每秒最多启动连接数,默认为100")]
    pub max_connect_per_second: usize,

    /// 设置发送间隔,默认为1秒
    #[arg(short = 'i', long, value_parser = clap::value_parser!(u64), default_value_t = 1,help="设置发送间隔,默认为1秒")]
    pub send_interval: u64,
}

#[derive(Debug)]
pub struct BenchmarkConfig<T, C> {
    /// 使用的数据传输协议，默认为mqtt
    pub protocol_type: Protocol,

    /// 设置需发送的数据内容
    pub send_data: T,

    /// 客户端集合
    pub clients: Vec<C>,

    /// 设置启动协程数量,默认为200
    pub thread_size: usize,

    /// 设置是否启用注册包机制
    pub enable_register: bool,

    /// 设置mqtt broker地址,默认为mqtt://localhost:1883
    pub broker: String,

    /// 每秒最多启动连接数
    pub max_connect_per_second: usize,

    /// 设置发送间隔,默认为1秒
    pub send_interval: u64,
}

impl BenchmarkConfig<TcpSendData, TcpClient> {
    pub async fn from_tcp_config(config: CommandConfig) -> Result<Self> {
        let data = load_tcp_hex_data_from_file(&config.data_file).await?;

        let clients = read_from_csv_into_struct::<TcpClient>(&config.client_file).await?;
        Ok(Self {
            protocol_type: config.protocol_type,
            send_data: TcpSendData { data },
            clients,
            thread_size: config.thread_size,
            enable_register: match config.enable_register {
                Flag::True => true,
                Flag::False => false,
            },
            broker: config.broker,
            max_connect_per_second: config.max_connect_per_second,
            send_interval: config.send_interval,
        })
    }
}

impl<T, C> BenchmarkConfig<T, C>
where
    T: Debug,
    C: Debug,
{
    pub async fn from_mqtt_config(config: CommandConfig) -> Result<BenchmarkConfig<T, C>>
    where
        T: DeserializeOwned + Debug,
        C: DeserializeOwned + Debug,
    {
        let data = load_send_data_from_json_file(&config.data_file).await?;

        let clients = read_from_csv_into_struct(&config.client_file).await?;
        Ok(Self {
            protocol_type: config.protocol_type,
            send_data: data,
            clients,
            thread_size: config.thread_size,
            enable_register: match config.enable_register {
                Flag::True => true,
                Flag::False => false,
            },
            broker: config.broker,
            max_connect_per_second: config.max_connect_per_second,
            send_interval: config.send_interval,
        })
    }

    pub fn set_send_data(&mut self, data: T) {
        self.send_data = data;
    }

    pub fn get_send_data(&self) -> &T {
        &self.send_data
    }

    pub fn get_broker(&self) -> &str {
        &self.broker
    }

    pub fn set_clients(&mut self, clients: Vec<C>) {
        self.clients = clients;
    }

    pub fn get_mut_clients(&mut self) -> &mut Vec<C> {
        &mut self.clients
    }

    pub fn get_clients(&self) -> &Vec<C> {
        &self.clients
    }

    pub fn set_protocol_type(&mut self, protocol_type: Protocol) {
        self.protocol_type = protocol_type;
    }

    pub fn set_thread_size(&mut self, thread_size: usize) {
        self.thread_size = thread_size;
    }

    pub fn set_enable_register(&mut self, enable_register: bool) {
        self.enable_register = enable_register;
    }

    pub fn set_broker(&mut self, broker: String) {
        self.broker = broker;
    }

    pub fn set_max_connect_per_second(&mut self, max_connect_per_second: usize) {
        self.max_connect_per_second = max_connect_per_second;
    }

    pub fn get_max_connect_per_second(&self) -> usize {
        self.max_connect_per_second
    }

    pub fn set_send_interval(&mut self, send_interval: u64) {
        self.send_interval = send_interval;
    }

    pub fn get_send_interval(&self) -> u64 {
        self.send_interval
    }
}
pub async fn load_send_data_from_json_file<T>(file_path: &str) -> Result<T>
where
    T: DeserializeOwned + Debug,
{
    let contents = fs::read_to_string(file_path)
        .await
        .with_context(|| format!("Failed to read the file: {}", file_path))?;

    // 解析 JSON 内容，并在出错时添加上下文信息
    let msg: T = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse JSON from file: {}", file_path))?;

    Ok(msg)
}

pub async fn load_tcp_hex_data_from_file(file_path: &str) -> Result<Vec<u8>> {
    let contents = fs::read_to_string(file_path)
        .await
        .with_context(|| format!("Failed to read the file: {}", file_path))?;
    info!("读取到的数据: {}", contents);
    let bytes = hex::decode(contents)
        .with_context(|| format!("Failed to decode hex string from file: {}", file_path))?;

    Ok(bytes)
}

pub async fn read_from_csv_into_struct<C>(file_path: &str) -> Result<Vec<C>>
where
    C: DeserializeOwned + Debug,
{
    let file = File::open(file_path)?;
    let mut rdr = csv::ReaderBuilder::new().delimiter(b',').from_reader(file);
    let mut csv_content_vec: Vec<C> = vec![];
    for result in rdr.deserialize::<C>() {
        let record = result?;
        csv_content_vec.push(record);
    }
    Ok(csv_content_vec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use tempfile::tempdir;
    use tokio::{fs::File, io::AsyncWriteExt};

    #[tokio::test]
    async fn test_load_send_data_from_json_file() -> Result<()> {
        // 创建一个临时目录
        let dir = tempdir()?;
        let file_path = dir.path().join("test_data.json");

        // 创建临时 JSON 文件
        let mut file = File::create(&file_path).await?;
        file.write_all(b"{\"key\": \"value\"}").await?;

        // 测试数据加载
        let result: serde_json::Value =
            load_send_data_from_json_file(file_path.to_str().unwrap()).await?;
        assert_eq!(result["key"], "value");

        Ok(())
    }

    #[tokio::test]
    async fn test_read_from_csv_into_struct() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Client {
            id: String,
            name: String,
        }

        // 创建一个临时目录
        let dir = tempdir()?;
        let file_path = dir.path().join("test_clients.csv");

        // 写入临时 CSV 数据
        let mut file = File::create(&file_path).await?;
        file.write_all(b"id,name\n1,client1\n2,client2").await?;

        // 测试 CSV 加载
        let clients: Vec<Client> = read_from_csv_into_struct(file_path.to_str().unwrap()).await?;
        assert_eq!(
            clients,
            vec![
                Client {
                    id: "1".to_string(),
                    name: "client1".to_string()
                },
                Client {
                    id: "2".to_string(),
                    name: "client2".to_string()
                }
            ]
        );

        Ok(())
    }

    #[test]
    fn test_command_config_default_values() {
        let args = vec!["test_app"];
        let config = CommandConfig::parse_from(args);

        // 检查默认值
        assert_eq!(config.data_file, "./data.json");
        assert_eq!(config.protocol_type, Protocol::Mqtt);
        assert_eq!(config.client_file, "./client.csv");
        assert_eq!(config.thread_size, 200);
        assert_eq!(config.enable_register, Flag::True);
        assert_eq!(config.broker, "mqtt://localhost:1883");
        assert_eq!(config.max_connect_per_second, 100);
        assert_eq!(config.send_interval, 1);
    }
}
