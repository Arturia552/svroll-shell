use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Error;
use chrono::{DateTime, Local};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet};
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinHandle,
    time::sleep,
};
use tracing::{error, info};

use crate::{command::BenchmarkConfig, TopicWrap, CLIENT_CONTEXT};

use super::{basic::TimestampConfig, Client};
#[derive(Clone, Debug)]
pub struct MqttClient {
    pub send_data: Value,
    pub enable_register: bool,
    pub register_topic: Arc<TopicWrap>,
    pub data_topic: Arc<TopicWrap>,
    pub time_config: Option<TimestampConfig>,
}

impl MqttClient {
    pub fn new(
        send_data: Value,
        enable_register: bool,
        register_topic: TopicWrap,
        data_topic: TopicWrap,
        time_config: Option<TimestampConfig>,
    ) -> Self {
        MqttClient {
            send_data: send_data,
            enable_register,
            register_topic: Arc::new(register_topic),
            data_topic: Arc::new(data_topic),
            time_config,
        }
    }

    pub fn get_send_data(&self) -> &Value {
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

    fn parse_topic_mac(topic: &str, key_index: usize) -> (String, String) {
        let topic = topic.to_string();
        let mut topic = topic.split('/').collect::<Vec<&str>>();
        let mac = topic.remove(key_index);
        (topic.join("/"), mac.to_string())
    }

    async fn handle_event_loop(
        client_id: String,
        mut event_loop: EventLoop,
        self_clone: Arc<MqttClient>,
    ) -> JoinHandle<()> {
        info!("启动事件循环: {}", client_id);
        // 返回JoinHandle以便后续可以取消
        tokio::spawn(async move {
            loop {
                // 每次循环前检查客户端是否还存在
                if !CLIENT_CONTEXT.contains_key(&client_id) {
                    break;
                }

                match event_loop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        self_clone.on_message_callback(&publish.topic, &publish.payload);
                    }
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        info!("连接成功: {}", client_id);
                        if let Some(mut client) = CLIENT_CONTEXT.get_mut(&client_id) {
                            match self_clone.on_connect_success(&mut client).await {
                                Ok(_) => {
                                    client.set_is_connected(true);
                                }
                                Err(e) => {
                                    error!("连接初始化失败: {:?}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let Some(mut client_entry) = CLIENT_CONTEXT.get_mut(&client_id) {
                            client_entry.set_is_connected(false);

                            if !client_entry.disconnecting.load(Ordering::SeqCst) {
                                error!("MQTT事件循环错误: {:?}", e);
                            }

                            let client_entry_clone = client_entry.clone();
                            tokio::spawn(async move {
                                if let Err(e) = client_entry_clone.safe_disconnect().await {
                                    error!("断开连接失败: {:?}", e);
                                }
                            });
                            break;
                        } else {
                            error!("MQTT事件循环错误: {:?}", e);
                        }

                        if !CLIENT_CONTEXT.contains_key(&client_id) {
                            break;
                        }
                        sleep(Duration::from_secs(2)).await;
                    }
                    _ => {}
                }
            }
            if let Some(mut client) = CLIENT_CONTEXT.get_mut(&client_id) {
                client.set_is_connected(false);
                if client.client.is_some() {
                    client.client = None;
                }
            }
        })
    }

    fn on_message_callback(&self, topic: &str, payload: &[u8]) {
        if let Ok(data) = serde_json::from_slice::<serde_json::Value>(payload) {
            if self.get_enable_register() {
                let register_topic = self.get_register_topic();
                let Some(reg_subscribe) = &register_topic.subscribe else {
                    return;
                };

                let (real_topic, mac) =
                    Self::parse_topic_mac(topic, reg_subscribe.key_index.unwrap());

                let Some(reg_sub_topic) = register_topic.get_subscribe_topic() else {
                    return;
                };
                let Some(extra_key) = &reg_subscribe.key_label else {
                    return;
                };

                if real_topic != reg_sub_topic {
                    return;
                }

                if let Some(device_key) = data.get(extra_key) {
                    if let Some(device_key_str) = device_key.as_str() {
                        CLIENT_CONTEXT.entry(mac.to_string()).and_modify(|v| {
                            v.set_device_key(device_key_str.to_string());
                        });
                    }
                }
            }
        }
    }
}

impl Client<Value, ClientData> for MqttClient {
    type Item = ClientData;

    async fn setup_clients(
        &self,
        config: &BenchmarkConfig<Value, ClientData>,
    ) -> Result<Vec<ClientData>, Box<dyn std::error::Error>> {
        let mut clients = vec![];

        let self_arc = Arc::new(self.clone());
        let broker = config.get_broker();
        let semaphore = Arc::new(Semaphore::new(config.get_max_connect_per_second()));

        // 解析broker地址和端口
        let broker_parts: Vec<&str> = broker.split(':').collect();
        let host = broker_parts[0].trim_start_matches("mqtt://");
        let port = broker_parts
            .get(1)
            .unwrap_or(&"1883")
            .parse::<u16>()
            .unwrap_or(1883);

        for client_ref in config.get_clients() {
            let permit = semaphore.acquire().await?;

            let mut client = client_ref.clone();
            let mut mqtt_options = MqttOptions::new(&client.client_id, host, port);

            mqtt_options.set_clean_session(true);
            mqtt_options.set_keep_alive(Duration::from_secs(20));
            mqtt_options.set_credentials(&client.client_id, client.get_password());
            mqtt_options.set_request_channel_capacity(10);

            // 创建客户端和事件循环
            let (cli, event_loop) = AsyncClient::new(mqtt_options, 10);

            // 启动事件循环处理
            let client_id = client.client_id.clone();
            let event_loop_handle =
                Self::handle_event_loop(client_id.clone(), event_loop, Arc::clone(&self_arc)).await;
            client.event_loop_handle = Some(Arc::new(Mutex::new(Some(event_loop_handle))));
            client.set_client(Some(cli.clone()));
            CLIENT_CONTEXT.insert(client.get_client_id().to_string(), client.clone());
            clients.push(client.clone());

            drop(permit);

            // 添加小延迟确保连接速率控制更平滑
            if clients.len() % config.get_max_connect_per_second() == 0 {
                sleep(Duration::from_secs(1)).await;
            }
        }

        Ok(clients)
    }

    async fn on_connect_success(&self, cli: &mut ClientData) -> Result<(), Error> {
        let client = cli
            .get_client()
            .ok_or_else(|| Error::msg("客户端未初始化"))?;

        // 注册包机制启用判断
        if self.get_enable_register() {
            let register_topic = self.get_register_topic();

            // 检查 extra_key
            let extra_key = match &register_topic.publish.key_label {
                Some(key) => key,
                None => {
                    // 断开连接并返回错误
                    if let Err(e) = client.disconnect().await {
                        error!("断开连接失败: {:?}", e);
                    }
                    return Err(Error::msg("注册主题配置错误"));
                }
            };

            // 订阅主题处理
            if register_topic.is_exist_subscribe() {
                let sub_topic_str = register_topic.get_subscribe_real_topic(Some(&cli.client_id));
                let qos = register_topic.get_subscribe_qos();

                if let Err(e) = client.subscribe(sub_topic_str, qos).await {
                    error!("订阅主题失败: {:?}", e);
                    return Err(Error::msg(format!("订阅主题失败: {:?}", e)));
                }
            }

            // 发布注册消息
            let pub_topic_str = register_topic.get_publish_topic();
            let register_json_str = format!(r#"{{"{}": "{}"}}"#, extra_key, cli.client_id);
            let qos = register_topic.get_publish_qos();

            if let Err(e) = client
                .publish(pub_topic_str, qos, false, register_json_str)
                .await
            {
                error!("发布注册消息失败: {:?}", e);
                return Err(Error::msg(format!("发布注册消息失败: {:?}", e)));
            }
        }
        Ok(())
    }

    async fn spawn_message(
        &self,
        clients: Vec<Self::Item>,
        counter: Arc<AtomicU32>,
        config: &BenchmarkConfig<Value, ClientData>,
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
            let send_data = self.send_data.clone();
            let counter: Arc<AtomicU32> = counter.clone(); // 克隆原子计数器
            let mqtt_client = self.clone();
            let send_interval = config.get_send_interval();
            let topic = Arc::clone(&self.data_topic);
            let enable_register = config.enable_register;
            let time_config = self.time_config.clone();

            // 为每个客户端组生成一个异步任务
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(send_interval));
                loop {
                    // 等待指定的间隔时间再进行下一次发送
                    interval.tick().await;
                    // 遍历每个组中的客户端
                    for cli in group.iter_mut() {
                        let client_id = cli.get_client_id().to_string();
                        if let Some(client_data) = CLIENT_CONTEXT.get(&client_id) {
                            // 创建发布的主题
                            let device_key = client_data.get_device_key();
                            if device_key.is_empty() && enable_register {
                                let _ = mqtt_client.on_connect_success(cli).await;
                                continue;
                            }
                            let real_topic = match client_data.get_identify_key() {
                                Some(identify_key) => {
                                    topic.get_pushlish_real_topic_identify_key(identify_key.clone())
                                }
                                None => {
                                    topic.get_publish_real_topic(Some(client_data.get_device_key()))
                                }
                            };
                            let now: DateTime<Local> = Local::now();

                            let mut payload = send_data.clone();

                            if let Some(obj) = payload.as_object_mut() {
                                if let Some(time_cfg) = &time_config {
                                    if time_cfg.time_type == "iso" {
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
                            let json_msg = match serde_json::to_string(&payload) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!("序列化JSON失败: {}", e);
                                    return;
                                }
                            };
                            let qos = topic.get_publish_qos();
                            let client = match cli.get_client() {
                                Some(c) => c,
                                None => {
                                    error!("客户端未初始化");
                                    continue;
                                }
                            };

                            if let Err(e) = client.publish(real_topic, qos, false, json_msg).await {
                                error!("发布消息失败: {:?}", e);
                                continue;
                            }
                            counter.fetch_add(1, Ordering::SeqCst); // 增加计数器
                        }
                    }
                }
            });
        }
    }

    async fn wait_for_connections(&self, clients: &mut [ClientData]) {
        let mut futures = Vec::with_capacity(clients.len());

        for client in clients.iter() {
            let client_id = client.get_client_id().to_string();
            futures.push(tokio::spawn(async move {
                let mut attempts = 0;
                const MAX_ATTEMPTS: usize = 100; // 10秒超时

                while attempts < MAX_ATTEMPTS {
                    if let Some(client_data) = CLIENT_CONTEXT.get(&client_id) {
                        if client_data.is_connected {
                            break;
                        }
                    }
                    sleep(Duration::from_millis(100)).await;
                    attempts += 1;
                }

                if attempts >= MAX_ATTEMPTS {
                    error!("客户端 {} 连接超时", client_id);
                }
            }));
        }

        for future in futures {
            // 更好的错误处理
            if let Err(e) = future.await {
                error!("等待连接任务失败: {:?}", e);
            }
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClientData {
    pub client_id: String,
    pub username: String,
    pub password: String,
    pub identify_key: Option<String>,
    #[serde(skip)]
    pub device_key: String,
    #[serde(skip)]
    pub enable_register: bool,
    #[serde(skip)]
    pub is_connected: bool,
    #[serde(skip)]
    pub client: Option<AsyncClient>,
    #[serde(skip)]
    pub event_loop_handle: Option<Arc<Mutex<Option<JoinHandle<()>>>>>,
    #[serde(skip)]
    pub disconnecting: Arc<AtomicBool>,
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

    pub fn get_identify_key(&self) -> &Option<String> {
        &self.identify_key
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

    pub fn set_is_connected(&mut self, is_connected: bool) {
        self.is_connected = is_connected
    }

    pub fn set_client(&mut self, client: Option<AsyncClient>) {
        self.client = client;
    }

    pub fn get_client(&self) -> Option<AsyncClient> {
        self.client.clone()
    }

    pub async fn safe_disconnect(&self) -> Result<(), Error> {
        // 仅当未在断开连接状态时执行
        if !self.disconnecting.swap(true, Ordering::SeqCst) {
            if let Some(client) = &self.client {
                client.disconnect().await?;
            }
        }
        Ok(())
    }
}
