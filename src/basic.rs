use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicInfo {
    key_index: u8,
    topic: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicWrap {
    publish: TopicInfo,
    subcribe: TopicInfo,
}

impl TopicWrap {
    pub fn init() -> Self {
        TopicWrap {
            publish: TopicInfo {
                key_index: 0,
                topic: "".to_string(),
            },
            subcribe: TopicInfo {
                key_index: 2,
                topic: "".to_string(),
            },
        }
    }

    pub fn modify(&mut self, source: TopicWrap) {
        self.publish = source.publish;
        self.subcribe = source.subcribe;
    }

    pub fn get_real_topic(&self, key_value: Option<&str>) -> &str {
        let topic = &self.publish;

        if topic.key_index == 0 {
            &topic.topic
        } else {
            // 在第key_index个"/"后面增加key_value
            let index = topic.topic.rfind('/').unwrap_or(0); // 处理未找到 '/' 的情况
            let new_topic = match key_value {
                Some(value) => format!("{}/{}", &topic.topic[..index + 1], value),
                None => topic.topic.clone(), // 如果 key_value 为 None，则返回原始的 topic
            };
            Box::leak(new_topic.into_boxed_str())
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "topic")]
pub enum TotalTopics {
    #[serde(rename = "register")]
    REGISTER(TopicWrap),
    #[serde(rename = "data")]
    DATA(TopicWrap),
}
