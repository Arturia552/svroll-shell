use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicInfo {
    key_index: Option<usize>, // 将 key_index 改为 Option<u8> 以处理 null 值
    topic: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicWrap {
    publish: TopicInfo,
    #[serde(default)] // 为 subscribe 提供一个默认值，因为 data 变体中没有这个字段
    subscribe: Option<TopicInfo>, // 使用 Option 来处理可能不存在的字段
}

impl TopicWrap {

    pub fn get_publish_real_topic(&self, key_value: Option<&str>) -> &str {
        let topic = &self.publish;
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
                // If key_index is out of range, return the original topic.
                &topic.topic
            }
        }
    }

    pub fn get_subscribe_real_topic(&self, key_value: Option<&str>) -> &str {
        let topic = self.subscribe.as_ref().unwrap();

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
                // If key_index is out of range, return the original topic.
                &topic.topic
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "topics")]
pub struct TotalTopics {
    pub register: Option<TopicWrap>,
    pub data: Option<TopicWrap>,
}
