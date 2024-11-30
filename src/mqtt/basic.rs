use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::AsyncReadExt};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicInfo {
    pub key_index: Option<usize>, // 将 key_index 改为 Option<u8> 以处理 null 值
    pub topic: String,
    #[serde(default = "default_qos")]
    pub qos: i32,
}

pub fn default_qos() -> i32 {
    0
}

impl TopicInfo {
    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn get_qos(&self) -> i32 {
        self.qos
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicWrap {
    pub publish: TopicInfo,
    #[serde(default)] // 为 subscribe 提供一个默认值，因为 data 变体中没有这个字段
    pub subscribe: Option<TopicInfo>, // 使用 Option 来处理可能不存在的字段
}

pub fn wrap_real_topic<'a>(topic: &'a TopicInfo, key_value: Option<&str>) -> &'a str {
    if topic.key_index.unwrap_or(0) == 0
        || key_value.is_none()
        || key_value.is_some_and(|val| val.is_empty())
    {
        &topic.topic
    } else {
        let key_index = topic.key_index.unwrap_or(0);
        let parts: Vec<&str> = topic.topic.split('/').collect();

        if key_index < parts.len() {
            let mut new_topic_parts = parts[..key_index].to_vec();
            if let Some(value) = key_value {
                new_topic_parts.push(value);
            }
            new_topic_parts.extend(&parts[key_index..]);

            let new_topic = new_topic_parts.join("/");
            Box::leak(new_topic.into_boxed_str())
        } else {
            &topic.topic
        }
    }
}

impl TopicWrap {
    pub fn is_exist_subscribe(&self) -> bool {
        self.subscribe.is_some()
    }

    pub fn get_publish_topic(&self) -> &str {
        &self.publish.get_topic()
    }

    pub fn get_subscribe_topic(&self) -> Option<&str> {
        self.subscribe
            .as_ref()
            .map(|sub_topic| sub_topic.get_topic())
    }

    pub fn get_publish_qos(&self) -> i32 {
        self.publish.get_qos()
    }

    pub fn get_subscribe_qos(&self) -> i32 {
        self.subscribe.as_ref().unwrap().qos
    }

    pub fn get_publish_real_topic(&self, key_value: Option<&str>) -> &str {
        let topic = &self.publish;
        wrap_real_topic(topic, key_value)
    }

    pub fn get_subscribe_real_topic(&self, key_value: Option<&str>) -> &str {
        let topic = self.subscribe.as_ref().unwrap();
        wrap_real_topic(topic, key_value)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TotalTopics {
    pub register: Option<TopicWrap>,
    pub data: Option<TopicWrap>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MqttConfig {
    pub topic: Option<TotalTopics>,
    pub timestamp: Option<TimestampConfig>
}

impl MqttConfig {
    pub fn get_register_topic(&self) -> Option<&str> {
        self.topic.as_ref().and_then(|topics| {
            topics
                .register
                .as_ref()
                .map(|topic| topic.get_publish_topic())
        })
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TcpConfig {}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub mqtt: Option<MqttConfig>,
    pub tcp: Option<TcpConfig>,
}

pub async fn load_config(file_path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let mut file = File::open(file_path).await?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).await?;
    let config: Config = serde_yaml::from_str(&contents)?;
    Ok(config)
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TimestampConfig {
    pub enable: bool,
    pub code: String,
    pub time_type: String
}