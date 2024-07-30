use clap::{arg, command, Parser, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
pub enum Protocol {
    Mqtt,
    Tcp,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum Flag {
    True,
    False,
}

#[derive(Parser, Debug, Clone)]
#[command(name = "Iot Benchmark")]
#[command(version = "1.0")]
#[command(author = "arturia zheng")]
#[command(about = "Iot benchmark")]
pub struct ConfigCommand {
    /// 设置需发送的数据文件路径,默认为当前目录下的data.json
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = "./data.json",
        help = "设置需发送的数据文件路径,默认为当前目录下的data.json"
    )]
    pub data_file: String,

    /// 使用的数据传输协议，默认为mqtt
    #[arg(
        short,
        long,
        value_name = "PROTOCOL",
        value_enum,
        default_value = "mqtt",
        help = "使用的数据传输协议，默认为mqtt"
    )]
    pub protocol_type: Protocol,

    /// 设置客户端文件,默认为当前目录下的client.csv
    #[arg(
        short = 'c',
        long,
        value_name = "FILE",
        default_value = "./client.csv",
        help = "设置客户端文件,默认为当前目录下的client.csv"
    )]
    pub client_file: String,

    /// 设置启动协程数量,默认为200
    #[arg(short = 't', long, value_parser = clap::value_parser!(usize), default_value_t = 200,help="设置启动协程数量,默认为200")]
    pub thread_size: usize,

    /// 设置是否启用注册包机制
    #[arg(
        short = 'r',
        long,
        value_name = "Flag",
        value_enum,
        default_value = "true",
        help = "设置是否启用注册包机制,默认为false"
    )]
    pub enable_register: Flag,

    /// 设置mqtt broker地址,默认为mqtt://localhost:1883
    #[arg(
        short = 'b',
        long,
        value_name = "BROKER",
        default_value = "mqtt://localhost:1883",
        help = "设置mqtt broker地址,默认为mqtt://localhost:1883"
    )]
    pub broker: String,

    /// 每秒最多启动连接数
    #[arg(short = 'p', long, value_parser = clap::value_parser!(usize), default_value_t = 100,help="每秒最多启动连接数,默认为100")]
    pub max_connect_per_second: usize,

    /// 设置发送间隔,默认为1秒
    #[arg(short = 'i', long, value_parser = clap::value_parser!(u64), default_value_t = 1,help="设置发送间隔,默认为1秒")]
    pub send_interval: u64,
}
