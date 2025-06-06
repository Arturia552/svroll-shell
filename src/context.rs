use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde_json::Value;
use tokio::net::tcp::OwnedWriteHalf;

use crate::{
    command::BenchmarkConfig,
    mqtt::{basic::MqttConfig, client_data::MqttClient},
    ClientData,
};

// 全局静态变量，用于存储客户端上下文
pub static CLIENT_CONTEXT: Lazy<DashMap<String, ClientData>> = Lazy::new(DashMap::new);

pub static TCP_CLIENT_CONTEXT: Lazy<DashMap<String, OwnedWriteHalf>> = Lazy::new(DashMap::new);

pub fn init_mqtt_context(
    benchmark_config: &BenchmarkConfig<Value, ClientData>,
    mqtt_config: MqttConfig,
) -> Result<MqttClient, Box<dyn std::error::Error>> {
    let data_topic;
    if let Some(topic) = mqtt_config.topic {
        if let Some(data) = topic.data {
            data_topic = data;
        } else {
            return Err("没有配置数据上报主题".into());
        }
    } else {
        return Err("没有配置MQTT主题".into());
    }

    let mqtt_client: MqttClient = MqttClient::new(
        benchmark_config.get_send_data().clone(),
        data_topic,
        mqtt_config.timestamp
    );

    Ok(mqtt_client)
}
