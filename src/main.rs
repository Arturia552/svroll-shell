mod basic;
mod client_data;
mod command;
mod device_data;
pub mod random_value;
mod tcp_client;
mod tcp_data;
mod traits;
use std::{
    fmt::Debug,
    fs::File,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use basic::TopicWrap;
use chrono::{DateTime, Local};
use clap::Parser;
use client_data::ClientData;
use command::{ConfigCommand, Protocol};
use dashmap::DashMap;
use device_data::DeviceData;
use mqtt::Message;
use once_cell::sync::{Lazy, OnceCell};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sysinfo::System;
use tokio::{io::AsyncReadExt, time::sleep};
extern crate paho_mqtt as mqtt;

// 全局静态变量，用于存储客户端上下文
static CLIENT_CONTEXT: Lazy<DashMap<String, ClientData>> = Lazy::new(DashMap::new);
// 存储注册topic
static REGISTER_INFO: OnceCell<TopicWrap> = OnceCell::new();
// 存储数据上报主题
static DATA_INFO: OnceCell<TopicWrap> = OnceCell::new();
// 是否启用注册包
static ENABLE_REGISTER: OnceCell<bool> = OnceCell::new();
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析命令行参数
    let command_matches = ConfigCommand::parse();

    let protocol_type = command_matches.protocol_type;

    let msg = get_data_from_json_file::<DeviceData>(command_matches.data_file.as_str()).await?;
    let config = basic::load_config(command_matches.topic_file.as_str()).await?;
    let mqtt_config = config.mqtt.unwrap_or_else(|| panic!("没有配置"));

    match protocol_type {
        Protocol::Mqtt => {
            if let Some(topic) = mqtt_config.topic {
                if let Some(register) = topic.register {
                    let _ = REGISTER_INFO.set(register);
                } else {
                    println!("无注册包配置");
                }
                if let Some(data) = topic.data {
                    let _ = DATA_INFO.set(data);
                } else {
                    println!("No data");
                }
            } else {
                println!("无mqtt topic配置信息");
            }

            let _ = ENABLE_REGISTER.set(command_matches.enable_register);

            // 读取CSV文件，获取消息内容和客户端数据
            let client_data =
                read_from_csv_into_struct::<ClientData>(command_matches.client_file.as_str())?;

            // 设置MQTT客户端
            let clients = setup_clients(&client_data, command_matches.broker).await?;

            println!("等待连接...");

            // 等待所有客户端连接成功
            wait_for_connections(&clients).await;

            println!("客户端已全部连接!");

            // 用于统计发送消息数量的计数器
            let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

            // 启动发送消息的线程
            spawn_message_threads(
                clients,
                msg,
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
                    print!("\x1B[2J\x1B[H");
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

    Ok(())
}

/// 设置MQTT客户端
async fn setup_clients(
    client_data: &[ClientData],
    broker: String,
) -> Result<Vec<mqtt::AsyncClient>, Box<dyn std::error::Error>> {
    let mut clients = vec![];

    for client in client_data.iter() {
        CLIENT_CONTEXT.insert(client.get_client_id().to_string(), client.clone());

        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(broker.as_str())
            .client_id(&client.client_id)
            .finalize();
        let cli: mqtt::AsyncClient = mqtt::AsyncClient::new(create_opts)?;

        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .clean_start(true)
            .automatic_reconnect(Duration::from_secs(2), Duration::from_secs(2))
            .keep_alive_interval(Duration::from_secs(20))
            .user_name(&client.client_id)
            .password(client.get_password())
            .finalize();

        cli.set_message_callback(on_message_callback);

        clients.push(cli.clone());

        tokio::spawn(async move {
            cli.connect(conn_opts).await.unwrap();
            on_connect_success(&cli).await;
        });

        tokio::time::sleep(Duration::from_nanos(2)).await;
    }

    Ok(clients)
}

/// 等待所有客户端连接成功
async fn wait_for_connections(clients: &[mqtt::AsyncClient]) {
    for ele in clients {
        while !ele.is_connected() {
            sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn spawn_message_threads(
    clients: Vec<mqtt::AsyncClient>, // MQTT客户端向量
    msg: DeviceData,                 // 要发送的数据消息
    counter: Arc<AtomicU32>,         // 用于跟踪发送消息的原子计数器
    thread_size: usize,              // 要生成的线程数量
    setting_send_interval: u64,      // 发送间隔时间
) {
    // 确定每个线程处理的客户端数量
    let startup_thread_size = clients.len() / thread_size
        + if clients.len() % thread_size != 0 {
            1
        } else {
            0
        };

    // 按线程大小将客户端分组
    let clients_group = clients.chunks(startup_thread_size);

    // 遍历每个客户端组
    for group in clients_group {
        let group = group.to_vec(); // 将组转换为向量以获得所有权
        let msg_value: DeviceData = msg.clone(); // 克隆消息数据
        let counter: Arc<AtomicU32> = counter.clone(); // 克隆原子计数器
        let topic = DATA_INFO.get().unwrap(); // 获取数据上报主题

        // 为每个客户端组生成一个异步任务
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(setting_send_interval));

            loop {
                // 遍历每个组中的客户端
                for cli in group.iter() {
                    if !cli.is_connected() {
                        continue;
                    }
                    let client_id = cli.client_id().to_string();
                    if let Some(client_data) = CLIENT_CONTEXT.get(&client_id) {
                        // 创建发布的主题
                        let device_key = client_data.get_device_key();
                        if device_key.is_empty() {
                            on_connect_success(&cli).await;
                            continue;
                        }
                        let real_topic =
                            topic.get_publish_real_topic(Some(client_data.get_device_key()));
                        // 获取当前本地时间
                        let now: DateTime<Local> = Local::now();
                        // 格式化时间用于消息
                        let formatted_time = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();

                        let mut msg_value = msg_value.clone(); // 克隆消息数据
                        msg_value.set_timestamp(formatted_time.into()); // 设置时间戳

                        // 将消息数据序列化为JSON
                        let json_msg = match serde_json::to_string(&msg_value) {
                            Ok(msg) => msg,
                            Err(e) => {
                                eprintln!("序列化JSON失败: {}", e);
                                return;
                            }
                        };
                        // 创建带有主题和负载的MQTT消息
                        let payload: mqtt::Message = mqtt::Message::new(
                            real_topic,
                            json_msg.clone(),
                            topic.get_publish_qos(),
                        );
                        counter.fetch_add(1, Ordering::SeqCst); // 增加计数器
                        let _ = cli.publish(payload); // 发布消息
                    }
                }
                // 等待指定的间隔时间再进行下一次发送
                interval.tick().await;
            }
        });
    }
}

/// 连接成功后执行的函数
async fn on_connect_success(cli: &mqtt::AsyncClient) {
    // 注册包机制启用判断
    if let Some(enable) = ENABLE_REGISTER.get() {
        if *enable {
            // 创建包含SN的JSON对象
            let sn_json = serde_json::json!({"sn": cli.client_id()});

            // 将JSON对象序列化为字符串，并处理可能的错误
            match serde_json::to_string(&sn_json) {
                Ok(sn_json_str) => {
                    // 创建订阅主题并订阅
                    let sub_topic = REGISTER_INFO.get().unwrap();

                    if sub_topic.is_exist_subscribe() {
                        let sub_topic_str =
                            sub_topic.get_subscribe_real_topic(Some(cli.client_id().as_str()));

                        let _ = cli
                            .subscribe(sub_topic_str, sub_topic.get_subscribe_qos())
                            .await;
                    }

                    // 创建注册消息并发布
                    let pub_topic = REGISTER_INFO.get().unwrap();
                    let pub_topic_str = pub_topic.get_publish_topic();

                    let register_msg =
                        mqtt::Message::new(pub_topic_str, sn_json_str, pub_topic.get_publish_qos());
                    cli.publish(register_msg);
                }
                Err(e) => {
                    eprintln!("设备注册失败，失败信息： {}", e);
                }
            }
        }
    }
}

/// 消息回调函数
fn on_message_callback(_: &mqtt::AsyncClient, msg: Option<Message>) {
    if let Some(msg) = msg {
        let topic = msg.topic(); // 获取消息主题

        // 检查主题是否以"/sub"开头
        if topic.starts_with("/sub") {
            // 获取真实的主题和MAC地址
            let (real_topic, mac) = get_real_topic_mac(topic);
            let data = msg.payload(); // 获取消息负载
                                      // 尝试将负载解析为JSON
            if let Ok(data) = serde_json::from_slice::<serde_json::Value>(data) {
                if let Some(enable) = ENABLE_REGISTER.get() {
                    if *enable {
                        let register_topic = REGISTER_INFO.get().unwrap();
                        let reg_sub_topic = register_topic.get_subscribe_topic().unwrap();

                        // 检查真实主题是否为注册包回复
                        if real_topic == reg_sub_topic {
                            // 检查JSON对象中是否存在"device_key"
                            if let Some(device_key) = data.get("device_key") {
                                // 将device_key转换为字符串
                                if let Some(device_key_str) = device_key.as_str() {
                                    // 更新CLIENT_CONTEXT中的device_key
                                    CLIENT_CONTEXT.entry(mac.to_string()).and_modify(|v| {
                                        v.set_device_key(device_key_str.to_string());
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// 从指定路径读取CSV文件，将内容转化为serde_json格式，返回Vec<Value>
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

/// 从主题字符串中提取实际主题和MAC地址
fn get_real_topic_mac(topic: &str) -> (String, String) {
    let topic = topic.to_string();
    let mut topic = topic.split('/').collect::<Vec<&str>>();
    let mac = topic.remove(2);
    (topic.join("/"), mac.to_string())
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
