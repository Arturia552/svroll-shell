use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::net::tcp::OwnedWriteHalf;

use crate::{
    command::BenchmarkConfig,
    mqtt::{basic::MqttConfig, client_data::MqttClient},
    ClientData, DeviceData,
};

// 全局静态变量，用于存储客户端上下文
pub static CLIENT_CONTEXT: Lazy<DashMap<String, ClientData>> = Lazy::new(DashMap::new);

pub static TCP_CLIENT_CONTEXT: Lazy<DashMap<String, OwnedWriteHalf>> = Lazy::new(DashMap::new);

pub fn init_mqtt_context(
    benchmark_config: &BenchmarkConfig<DeviceData, ClientData>,
    mqtt_config: MqttConfig,
) -> Result<MqttClient, Box<dyn std::error::Error>> {
    let register_topic;
    let data_topic;
    if let Some(topic) = mqtt_config.topic {
        if let Some(register) = topic.register {
            register_topic = register;
        } else {
            return Err("没有配置注册主题".into());
        }
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
        benchmark_config.enable_register,
        register_topic,
        data_topic,
    );

    Ok(mqtt_client)
}
