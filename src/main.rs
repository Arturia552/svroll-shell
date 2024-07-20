extern crate iot_benchmark;
extern crate paho_mqtt as mqtt;
use clap::Parser;
use iot_benchmark::{
    command::{ConfigCommand, Protocol},
    load_config,
    mqtt::{client_data::MqttClient, context::init_context, Client},
    ClientData, DeviceData,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    fmt::Debug,
    fs::File,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use sysinfo::System;
use tokio::{io::AsyncReadExt, time::sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析命令行参数
    let command_matches = ConfigCommand::parse();
    let protocol_type = &command_matches.protocol_type;

    let msg = get_data_from_json_file::<DeviceData>(command_matches.data_file.as_str()).await?;
    let config = load_config("./config.yaml").await?;
    let mqtt_config = config.mqtt.unwrap_or_else(|| panic!("没有配置"));

    match protocol_type {
        Protocol::Mqtt => {
            init_context(&command_matches, mqtt_config)?;

            // 读取CSV文件，获取消息内容和客户端数据
            let client_data: Vec<ClientData> =
                read_from_csv_into_struct::<ClientData>(command_matches.client_file.as_str())?;

            // 设置MQTT客户端
            let mqtt_client = MqttClient::new(msg);
            let clients = mqtt_client
                .setup_clients(&client_data, command_matches.broker)
                .await?;

            println!("等待连接...");

            // 等待所有客户端连接成功
            MqttClient::wait_for_connections(&clients).await;

            println!("客户端已全部连接!");

            // 用于统计发送消息数量的计数器
            let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

            // 启动发送消息的线程
            mqtt_client
                .spawn_message(
                    clients,
                    counter.clone(),
                    command_matches.thread_size,
                    command_matches.send_interval,
                )
                .await;

            // 初始化系统信息获取器
            let mut sys = System::new_all();
            let pid = sysinfo::get_current_pid().expect("Failed to get current PID");

            // 循环输出已发送的消息数和系统信息
            loop {
                sys.refresh_all();

                // 获取当前应用程序的CPU和内存使用信息
                if let Some(process) = sys.process(pid) {
                    let cpu_usage = process.cpu_usage();
                    let memory_used = process.memory();
                    println!("已发送消息数: {}", counter.load(Ordering::SeqCst));
                    println!("CPU使用率: {:.2}%", cpu_usage);
                    // 转化为MB并打印
                    let memory_used = memory_used / 1024 / 1024;
                    println!("内存使用: {} MB", memory_used);
                } else {
                    println!("无法获取当前进程的信息");
                }

                sleep(Duration::from_secs(1)).await;
            }
        }
        Protocol::Tcp => {
            todo!()
        }
    }
}

/// 从指定路径读取CSV文件，将内容转化为serde_json格式，返回Vec<Value>
#[allow(dead_code)]
fn read_from_csv(file_path: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let mut rdr = csv::ReaderBuilder::new().delimiter(b',').from_reader(file);
    let mut csv_content_vec: Vec<Value> = vec![];

    for result in rdr.deserialize::<String>() {
        let record = result?;
        let json: Value = serde_json::from_str(&record)?;
        csv_content_vec.push(json);
    }
    Ok(csv_content_vec)
}

/// 从指定路径读取CSV文件，将内容转化为指定的结构体，返回Vec<T>
fn read_from_csv_into_struct<T>(file_path: &str) -> Result<Vec<T>, Box<dyn std::error::Error>>
where
    T: DeserializeOwned + Debug,
{
    let file = File::open(file_path)?;
    let mut rdr = csv::ReaderBuilder::new().delimiter(b',').from_reader(file);
    let mut csv_content_vec: Vec<T> = vec![];
    for result in rdr.deserialize::<T>() {
        let record = result?;
        csv_content_vec.push(record);
    }
    Ok(csv_content_vec)
}

async fn get_data_from_json_file<T>(file_path: &str) -> Result<T, Box<dyn std::error::Error>>
where
    T: DeserializeOwned + Debug,
{
    use tokio::fs::File;

    let mut file = File::open(file_path).await?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).await?;
    let msg: T = serde_json::from_str(&contents)?;
    Ok(msg)
}
