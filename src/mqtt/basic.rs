use std::borrow::Cow;
use std::str::FromStr;

use rumqttc::QoS;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::{fs::File, io::AsyncReadExt};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicInfo {
    #[serde(deserialize_with = "deserialize_key_index")]
    pub key_index: Option<usize>,
    pub key_label: Option<String>,
    pub topic: String,
    #[serde(default = "default_qos")]
    pub qos: i32,
}

fn deserialize_key_index<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_yaml::Value::deserialize(deserializer)?;
    
    match value {
        // 如果是null值，直接返回None
        serde_yaml::Value::Null => Ok(None),
        
        // 如果是数字，直接返回对应的值
        serde_yaml::Value::Number(num) => {
            if let Some(n) = num.as_u64() {
                Ok(Some(n as usize))
            } else {
                Err(serde::de::Error::custom("Expected an unsigned integer"))
            }
        },
        
        // 如果是字符串，尝试解析，去除可能的逗号等字符
        serde_yaml::Value::String(s) => {
            if s == "null" {
                return Ok(None);
            }
            
            // 去除所有非数字字符
            let clean_str: String = s.chars().filter(|c| c.is_digit(10)).collect();
            
            if clean_str.is_empty() {
                Ok(None)
            } else {
                match usize::from_str(&clean_str) {
                    Ok(n) => Ok(Some(n)),
                    Err(_) => Err(serde::de::Error::custom(format!("Invalid usize: {}", s)))
                }
            }
        },
        
        // 其他类型直接报错
        _ => Err(serde::de::Error::custom("Expected null, number or string"))
    }
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

pub fn wrap_real_topic<'a>(topic: &'a TopicInfo, key_value: Option<&str>) -> Cow<'a, str> {
    if topic.key_index.unwrap_or(0) == 0
        || key_value.is_none()
        || key_value.is_some_and(|val| val.is_empty())
    {
        return Cow::Borrowed(&topic.topic);
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
            Cow::Owned(new_topic)
        } else {
            Cow::Borrowed(&topic.topic)
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

    pub fn get_publish_qos(&self) -> QoS {
        match self.publish.get_qos() {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtMostOnce,
        }
    }

    pub fn get_subscribe_qos(&self) -> QoS {
        match self.subscribe.as_ref().unwrap().qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtMostOnce,
        }
    }

    pub fn get_publish_real_topic<'a>(&'a self, key_value: Option<&str>) -> Cow<'a, str> {
        wrap_real_topic(&self.publish, key_value)
    }

    pub fn get_pushlish_real_topic_identify_key<'a>(&'a self, identify_key: String) -> Cow<'a,str> {
        let topic = &self.publish;
        let key_index = topic.key_index;
        if key_index.is_none() || identify_key.trim().is_empty() {
            return Cow::Borrowed(&topic.topic);
        }else {
            let key_index = key_index.unwrap();
            let parts: Vec<&str> = topic.topic.split('/').collect();

            if key_index < parts.len() {
                let mut new_topic_parts = parts[..key_index].to_vec();
                new_topic_parts.push(&identify_key);
                new_topic_parts.extend(&parts[key_index..]);

                let new_topic = new_topic_parts.join("/");
                return Cow::Owned(new_topic);
            }
            return Cow::Borrowed(&topic.topic);
        }
        
    }

    pub fn get_subscribe_real_topic<'a>(&'a self, key_value: Option<&str>) -> Cow<'a, str> {
        if let Some(topic) = &self.subscribe {
            wrap_real_topic(topic, key_value)
        } else {
            Cow::Borrowed("")
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TotalTopics {
    pub data: Option<TopicWrap>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MqttConfig {
    pub topic: Option<TotalTopics>,
    pub timestamp: Option<TimestampConfig>,
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
    pub time_type: String,
}
