use dashmap::DashMap;
use once_cell::sync::{Lazy, OnceCell};

use crate::{command::ConfigCommand, mqtt::basic::MqttConfig, ClientData, TopicWrap};


// 全局静态变量，用于存储客户端上下文
pub static CLIENT_CONTEXT: Lazy<DashMap<String, ClientData>> = Lazy::new(DashMap::new);
// 存储注册topic
pub static REGISTER_INFO: OnceCell<TopicWrap> = OnceCell::new();
// 存储数据上报主题
pub static DATA_INFO: OnceCell<TopicWrap> = OnceCell::new();
// 是否启用注册包
pub static ENABLE_REGISTER: OnceCell<bool> = OnceCell::new();

pub fn init_context(
    command_matches: &ConfigCommand,
    mqtt_config: MqttConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(topic) = mqtt_config.topic {
        if let Some(register) = topic.register {
            let _ = REGISTER_INFO.set(register);
        } else {
            return Err("没有配置注册主题".into());
        }
        if let Some(data) = topic.data {
            let _ = DATA_INFO.set(data);
        } else {
            return Err("没有配置数据上报主题".into());
        }
    } else {
        return Err("没有配置MQTT主题".into());
    }
    match command_matches.enable_register {
        crate::command::Flag::True => {
            let _ = ENABLE_REGISTER.set(true);
        }
        crate::command::Flag::False => {
            let _ = ENABLE_REGISTER.set(false);
        }
    }

    Ok(())
}
