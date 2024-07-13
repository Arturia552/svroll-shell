mod client_data;
mod device_data;
pub mod random_value;
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

use chrono::{DateTime, Local};
use clap::{value_parser, Arg, Command};
use client_data::ClientData;
use dashmap::DashMap;
use device_data::DeviceData;
use mqtt::{Message, QOS_1};
use once_cell::sync::Lazy;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::{io::AsyncReadExt, time::sleep};
extern crate paho_mqtt as mqtt;

// 全局静态变量，用于存储客户端上下文
static CLIENT_CONTEXT: Lazy<DashMap<String, ClientData>> = Lazy::new(DashMap::new);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析命令行参数
    let matches = parse_args();

    // 获取命令行参数的值
    let data_file_path: &String = matches.get_one::<String>("data-file").unwrap();
    let client_file_path = matches.get_one::<String>("client-file").unwrap();
    let setting_thread_size = matches.get_one::<usize>("thread-size").unwrap();
    let setting_broker = matches.get_one::<String>("broker").unwrap();
    let setting_send_interval = matches.get_one::<u64>("send-interval").unwrap();

    // 读取CSV文件，获取消息内容和客户端数据
    let msg = get_data_from_json_file(data_file_path).await?;
    let client_data = read_from_csv_into_struct::<ClientData>(client_file_path)?;

    // 设置MQTT客户端
    let clients = setup_clients(&client_data, setting_broker).await?;

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
        *setting_thread_size,
        *setting_send_interval,
    )
    .await;

    // 循环输出已发送的消息数
    loop {
        println!("已发送消息数: {}", counter.load(Ordering::SeqCst));
        sleep(Duration::from_secs(1)).await;
    }
}

/// 解析命令行参数
fn parse_args() -> clap::ArgMatches {
    Command::new("Mqtt Benchmark")
        .version("1.0")
        .author("arturia zheng")
        .about("mqtt benchmark")
        .arg(
            Arg::new("data-file")
                .short('d')
                .long("data-file")
                .value_name("FILE")
                .required(false)
                .default_value("./data.json")
                .help("设置需发送的数据文件路径,默认为当前目录下的data.json"),
        )
        .arg(
            Arg::new("client-file")
                .short('c')
                .long("client-file")
                .value_name("FILE")
                .required(false)
                .default_value("./client.csv")
                .help("设置客户端文件,默认为当前目录下的client.csv"),
        )
        .arg(
            Arg::new("thread-size")
                .short('t')
                .long("thread-size")
                .value_parser(value_parser!(usize))
                .value_name("THREAD")
                .required(false)
                .default_value("200")
                .help("设置启动协程数量,默认为200"),
        )
        .arg(
            Arg::new("broker")
                .short('b')
                .long("broker")
                .value_name("BROKER")
                .required(false)
                .default_value("mqtt://localhost:1883")
                .help("设置mqtt broker地址,默认为mqtt://localhost:1883"),
        )
        .arg(
            Arg::new("send-interval")
                .short('i')
                .long("send-interval")
                .value_parser(value_parser!(u64))
                .value_name("INTERVAL")
                .required(false)
                .default_value("1")
                .help("设置发送间隔,默认为1秒"),
        )
        .get_matches()
}

/// 设置MQTT客户端
async fn setup_clients(
    client_data: &[ClientData],
    broker: &str,
) -> Result<Vec<mqtt::AsyncClient>, Box<dyn std::error::Error>> {
    let mut clients = vec![];

    for client in client_data.iter() {
        CLIENT_CONTEXT.insert(client.get_client_id().to_string(), client.clone());

        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(broker.to_string())
            .client_id(client.get_client_id())
            .finalize();
        let cli: mqtt::AsyncClient = mqtt::AsyncClient::new(create_opts)?;

        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .clean_start(true)
            .keep_alive_interval(Duration::from_secs(30))
            .user_name(client.get_username())
            .password(client.get_password())
            .finalize();

        cli.set_message_callback(on_message_callback);

        clients.push(cli.clone());

        tokio::spawn(async move {
            cli.connect_with_callbacks(conn_opts, on_connect_success, on_connect_failure)
        });
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

/// 启动发送消息的线程
async fn spawn_message_threads(
    clients: Vec<mqtt::AsyncClient>,
    msg: DeviceData,
    counter: Arc<AtomicU32>,
    thread_size: usize,
    setting_send_interval: u64,
) {
    let startup_thread_size = clients.len() / thread_size
        + if clients.len() % thread_size != 0 {
            1
        } else {
            0
        };
    let clients_group = clients.chunks(startup_thread_size);

    for group in clients_group {
        let group = group.to_vec();
        let msg_value: DeviceData = msg.clone();
        let counter: Arc<AtomicU32> = counter.clone();
        tokio::spawn(async move {
            loop {
                for cli in group.iter() {
                    let client_id = cli.client_id();
                    let client_data = match CLIENT_CONTEXT.get(&client_id.to_string()) {
                        Some(data) => data,
                        None => {
                            continue;
                        }
                    };
                    let topic = format!("/pub/{}/long_freq/data", client_data.get_device_key());
                    let now: DateTime<Local> = Local::now();
                    let formatted_time = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();

                    let mut msg_value = msg_value.clone();
                    msg_value.set_timestamp(formatted_time.into());

                    // 随机数据
                    // let data = msg_value.get_data();

                    let json_msg = match serde_json::to_string(&msg_value) {
                        Ok(msg) => msg,
                        Err(e) => {
                            eprintln!("Failed to serialize JSON: {}", e);
                            return;
                        }
                    };

                    let payload: mqtt::Message =
                        mqtt::Message::new(topic, json_msg.clone(), mqtt::QOS_0);
                    counter.fetch_add(1, Ordering::SeqCst);
                    let _ = cli.publish(payload);
                }
                sleep(Duration::from_secs(setting_send_interval)).await;
            }
        });
    }
}

/// 连接成功的回调函数
fn on_connect_success(cli: &mqtt::AsyncClient, _msgid: u16) {
    let sn_json: Value = serde_json::json!({"sn": cli.client_id()});
    let sn_json_str = serde_json::to_string(&sn_json).unwrap();

    let register_msg = mqtt::Message::new("/pub/register", sn_json_str, QOS_1);
    let _ = cli.publish(register_msg);
    let topic = format!("/sub/{}/register/ack", cli.client_id());
    cli.subscribe(topic, QOS_1);
}

/// 连接失败的回调函数
fn on_connect_failure(_cli: &mqtt::AsyncClient, _msgid: u16, rc: i32) {
    println!("Connection attempt failed with error code {}.", rc);
}

/// 消息回调函数
fn on_message_callback(_: &mqtt::AsyncClient, msg: Option<Message>) {
    if let Some(msg) = msg {
        let topic = msg.topic().to_string();
        let path = topic.starts_with("/sub");

        if path {
            let (real_topic, mac) = get_real_topic_mac(&topic);
            let data = msg.payload();
            let data_json: Value = serde_json::from_slice(data).unwrap();
            if real_topic == "/sub/register/ack" {
                let obj = data_json.as_object().unwrap();
                let device_key: &Value = obj.get("device_key").unwrap();

                CLIENT_CONTEXT.alter(&mac, |_, mut v| {
                    let key_string = device_key.to_string();
                    let key = key_string.trim_matches('"');
                    v.set_device_key(key.to_string());
                    v
                });
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

async fn get_data_from_json_file(
    file_path: &str,
) -> Result<DeviceData, Box<dyn std::error::Error>> {
    use tokio::fs::File;

    let mut file = File::open(file_path).await?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).await?;

    let msg: DeviceData = serde_json::from_str(&contents)?;
    Ok(msg)
}
