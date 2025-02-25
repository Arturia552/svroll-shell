use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{DateTime, Local};
use paho_mqtt::{AsyncClient, ConnectOptionsBuilder, CreateOptionsBuilder, Message};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    sync::Semaphore,
    time::{sleep, Instant},
};

use crate::{command::BenchmarkConfig, DeviceData, TopicWrap, CLIENT_CONTEXT};

use super::{basic::TimestampConfig, Client};
#[derive(Clone, Debug)]
pub struct MqttClient {
    pub send_data: Arc<DeviceData>,
    pub enable_register: bool,
    pub register_topic: Arc<TopicWrap>,
    pub data_topic: Arc<TopicWrap>,
    pub time_config: Option<TimestampConfig>,
}

impl MqttClient {
    pub fn new(
        send_data: DeviceData,
        enable_register: bool,
        register_topic: TopicWrap,
        data_topic: TopicWrap,
        time_config: Option<TimestampConfig>,
    ) -> Self {
        MqttClient {
            send_data: Arc::new(send_data),
            enable_register,
            register_topic: Arc::new(register_topic),
            data_topic: Arc::new(data_topic),
            time_config,
        }
    }

    pub fn get_send_data(&self) -> &DeviceData {
        &self.send_data
    }

    pub fn get_enable_register(&self) -> bool {
        self.enable_register
    }

    pub fn set_enable_register(&mut self, enable: bool) {
        self.enable_register = enable;
    }

    pub fn set_register_topic(&mut self, topic: TopicWrap) {
        self.register_topic = Arc::new(topic);
    }

    pub fn get_register_topic(&self) -> &TopicWrap {
        &self.register_topic
    }

    pub fn set_data_topic(&mut self, topic: TopicWrap) {
        self.data_topic = Arc::new(topic);
    }

    pub fn get_data_topic(&self) -> &TopicWrap {
        &self.data_topic
    }

    fn get_real_topic_mac(topic: &str) -> (String, String) {
        let topic = topic.to_string();
        let mut topic = topic.split('/').collect::<Vec<&str>>();
        let mac = topic.remove(2);
        (topic.join("/"), mac.to_string())
    }

    pub fn on_message_callback(&self, _: &AsyncClient, msg: Option<Message>) {
        if let Some(msg) = msg {
            let topic = msg.topic(); // 获取消息主题

            // 获取真实的主题和MAC地址
            let (real_topic, mac) = Self::get_real_topic_mac(topic);
            let data = msg.payload();
            if let Ok(data) = serde_json::from_slice::<serde_json::Value>(data) {
                if self.get_enable_register() {
                    let register_topic = self.get_register_topic();
                    let reg_sub_topic = register_topic
                        .get_subscribe_topic()
                        .expect("没有配置注册订阅主题");

                    let key_label = register_topic.subscribe.clone().unwrap_or_else(||{
                        panic!("没有配置注册包的subscribe")
                    }).key_label.unwrap_or_else(||{
                        panic!("没有配置注册包的key_label")
                    });

                    // 检查真实主题是否为注册包回复
                    if real_topic == reg_sub_topic {
                        // 检查JSON对象中是否存在"device_key"
                        if let Some(device_key) = data.get(key_label) {
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

impl Client<DeviceData, ClientData> for MqttClient {
    type Item = AsyncClient;

    async fn setup_clients(
        &self,
        config: &BenchmarkConfig<DeviceData, ClientData>,
    ) -> Result<Vec<AsyncClient>, Box<dyn std::error::Error>> {
        let mut clients = vec![];
        let broker = config.get_broker();

        let semaphore = Arc::new(Semaphore::new(config.get_max_connect_per_second()));

        for client in config.get_clients() {
            CLIENT_CONTEXT.insert(client.get_client_id().to_string(), client.clone());
            let create_opts = CreateOptionsBuilder::new()
                .server_uri(broker)
                .client_id(&client.client_id)
                .finalize();
            let mut cli: AsyncClient = AsyncClient::new(create_opts)?;

            let conn_opts = ConnectOptionsBuilder::new_v5()
                .clean_start(true)
                .automatic_reconnect(Duration::from_secs(2), Duration::from_secs(2))
                .keep_alive_interval(Duration::from_secs(20))
                .user_name(&client.client_id)
                .password(client.get_password())
                .finalize();

            let mqtt_client = self.clone();
            cli.set_message_callback(move |client, message| {
                Self::on_message_callback(&mqtt_client, client, message);
            });
            let mqtt_client = self.clone();
            clients.push(cli.clone());

            let semaphore = Arc::clone(&semaphore);
            tokio::spawn(async move {
                let permit = semaphore.acquire().await.unwrap();

                let start = Instant::now();
                match cli.connect(conn_opts).await {
                    Ok(_) => {
                        mqtt_client.on_connect_success(&mut cli).await;
                    }
                    Err(_) => {}
                }

                let elapsed = start.elapsed();
                if elapsed < Duration::from_secs(1) {
                    tokio::time::sleep(Duration::from_secs(1) - elapsed).await;
                }

                drop(permit);
            });
        }

        Ok(clients)
    }

    async fn on_connect_success(&self, cli: &mut Self::Item) {
        // 注册包机制启用判断
        if self.get_enable_register() {
            // 创建包含SN的JSON对象
            let key_label = self.register_topic.publish.key_label.clone().unwrap_or_else(||{
                panic!("没有配置注册包的key_label")
            });
            // 创建订阅主题并订阅
            let sub_topic = self.get_register_topic();
            if sub_topic.is_exist_subscribe() {
                let sub_topic_str =
                    sub_topic.get_subscribe_real_topic(Some(cli.client_id().as_str()));

                let _ = cli.subscribe(sub_topic_str, sub_topic.get_subscribe_qos());
            }

            // 创建注册消息并发布
            let pub_topic = self.get_register_topic();
            let pub_topic_str = pub_topic.get_publish_topic();

            let register_json_str = format!(r#"{{"{}": "{}"}}"#, key_label, cli.client_id());
            let register_msg = Message::new(
                pub_topic_str,
                register_json_str,
                pub_topic.get_publish_qos(),
            );
            cli.publish(register_msg);
        }
    }

    async fn spawn_message(
        &self,
        clients: Vec<Self::Item>,
        counter: Arc<AtomicU32>,
        config: &BenchmarkConfig<DeviceData, ClientData>,
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

        // 遍历每个客户端组
        for group in clients_group {
            let mut group = group.to_vec(); // 将组转换为数组以获得所有权
            let send_data = Arc::clone(&self.send_data);
            let counter: Arc<AtomicU32> = counter.clone(); // 克隆原子计数器
            let mqtt_client = self.clone();
            let send_interval = config.get_send_interval();
            let topic = Arc::clone(&self.data_topic);
            let time_config = self.time_config.clone();

            // 为每个客户端组生成一个异步任务
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(send_interval));
                loop {
                    // 等待指定的间隔时间再进行下一次发送
                    interval.tick().await;
                    // 将消息数据序列化为JSON
                    let mut msg_value: Value =
                        serde_json::to_value(&*send_data).expect("序列化失败");
                    // 遍历每个组中的客户端
                    for cli in group.iter_mut() {
                        if !cli.is_connected() {
                            continue;
                        }
                        let client_id = cli.client_id().to_string();
                        if let Some(client_data) = CLIENT_CONTEXT.get(&client_id) {
                            // 创建发布的主题
                            let device_key = client_data.get_device_key();
                            if device_key.is_empty() && client_data.is_enable_register() {
                                mqtt_client.on_connect_success(cli).await;
                                continue;
                            }
                            let real_topic =
                                topic.get_publish_real_topic(Some(client_data.get_device_key()));
                            // 获取当前本地时间
                            let now: DateTime<Local> = Local::now();

                            if let Some(obj) = msg_value.as_object_mut() {
                                if let Some(time_cfg) = &time_config {
                                    if time_cfg.time_type == "instant" {
                                        let formatted_time =
                                            now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
                                        obj.insert(
                                            time_cfg.code.to_owned(),
                                            Value::String(formatted_time),
                                        );
                                    } else {
                                        obj.insert(
                                            time_cfg.code.to_owned(),
                                            Value::Number(now.timestamp_millis().into()),
                                        );
                                    }
                                } else {
                                    obj.insert(
                                        "timestamp".to_string(),
                                        Value::Number(now.timestamp_millis().into()),
                                    );
                                }
                            }
                            let json_msg = match serde_json::to_string(&msg_value) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!("序列化JSON失败: {}", e);
                                    return;
                                }
                            };
                            // 创建带有主题和负载的MQTT消息
                            let payload: Message =
                                Message::new(real_topic, json_msg, topic.get_publish_qos());
                            counter.fetch_add(1, Ordering::SeqCst); // 增加计数器
                            let _ = cli.publish(payload); // 发布消息
                        }
                    }
                }
            });
        }
    }

    async fn wait_for_connections(&self, clients: &mut [AsyncClient]) {
        for ele in clients {
            while !ele.is_connected() {
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClientData {
    pub client_id: String,
    pub username: String,
    pub password: String,
    #[serde(skip)]
    pub device_key: String,
    #[serde(skip)]
    pub enable_register: bool,
}

impl ClientData {
    pub fn get_client_id(&self) -> &str {
        &self.client_id
    }

    pub fn get_username(&self) -> &str {
        &self.username
    }

    pub fn get_password(&self) -> &str {
        &self.password
    }

    pub fn get_device_key(&self) -> &str {
        &self.device_key
    }

    pub fn set_client_id(&mut self, client_id: String) {
        self.client_id = client_id;
    }

    pub fn set_device_key(&mut self, device_key: String) {
        self.device_key = device_key;
    }

    pub fn set_enable_register(&mut self, enable_register: bool) {
        self.enable_register = enable_register;
    }

    pub fn is_enable_register(&self) -> bool {
        self.enable_register
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}
