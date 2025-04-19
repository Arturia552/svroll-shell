extern crate iot_benchmark;
use clap::Parser;
use comfy_table::Table;
use iot_benchmark::{
    command::{BenchmarkConfig, CommandConfig, Protocol},
    init_mqtt_context, load_config,
    mqtt::Client,
    tcp::tcp_client::TcpClientContext,
};
use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use sysinfo::System;
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    // 解析命令行参数
    let command_config = CommandConfig::parse();
    // 用于统计发送消息数量的计数器
    let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    match command_config.protocol_type {
        Protocol::Mqtt => {
            let benchmark_config = BenchmarkConfig::from_mqtt_config(command_config).await?;

            let config = load_config("./config.yaml").await?;
            let mqtt_config = config.mqtt.expect("没有配置");
            let mqtt_client = init_mqtt_context(&benchmark_config, mqtt_config)?;
            let mut clients = mqtt_client.setup_clients(&benchmark_config).await?;
            info!("等待连接...");
            mqtt_client.wait_for_connections(&mut clients).await;
            info!("客户端已全部连接!");
            mqtt_client
                .spawn_message(clients, counter.clone(), &benchmark_config)
                .await;
        }
        Protocol::Tcp => {
            let mut benchmark_config = BenchmarkConfig::from_tcp_config(command_config).await?;

            let tcp_client_context = TcpClientContext {
                send_data: Arc::new(benchmark_config.send_data.clone()),
                enable_register: benchmark_config.enable_register,
            };
            info!("TCP客户端上下文: {:?}", tcp_client_context);
            info!("正在初始化TCP客户端...");
            tcp_client_context.setup_clients(&benchmark_config).await?;
            tcp_client_context
                .wait_for_connections(&mut benchmark_config.clients)
                .await;
            info!("客户端已全部连接!");
            tcp_client_context
                .spawn_message(
                    benchmark_config.clients.clone(),
                    counter.clone(),
                    &benchmark_config,
                )
                .await;
        }
    }
    // 初始化系统信息获取器
    let mut sys = System::new_all();
    let pid = sysinfo::get_current_pid().expect("Failed to get current PID");
    // 循环输出已发送的消息数和系统信息
    loop {
        // 刷新系统信息
        sys.refresh_all();
        // 清空终端
        print!("\x1B[2J\x1B[1;1H");
        let mut table = Table::new();
        table.set_header(vec!["指标", "值"]);
        // 获取当前应用程序的CPU和内存使用信息
        if let Some(process) = sys.process(pid) {
            let cpu_usage = process.cpu_usage();
            let memory_used = process.memory();
            table.add_row(vec![
                "已发送消息数",
                counter.load(Ordering::SeqCst).to_string().as_str(),
            ]);
            table.add_row(vec!["CPU使用率", format!("{:.2}%", cpu_usage).as_str()]);
            // 转化为MB并添加到表格
            let memory_used = memory_used / 1024 / 1024;
            table.add_row(vec!["内存使用", format!("{} MB", memory_used).as_str()]);
        } else {
            table.add_row(vec!["错误", "无法获取当前进程的信息"]);
        }
        println!("{}", table);
        sleep(Duration::from_secs(1)).await;
    }
}
