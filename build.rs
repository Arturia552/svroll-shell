use std::{env, fs};

fn main() {
    let profile = env::var("PROFILE").unwrap();
    let target_dir = if profile == "release" {
        "target/release"
    } else {
        "target/debug"
    };

    // 创建目标目录
    fs::create_dir_all(target_dir).unwrap();

    // 复制配置文件
    let files = ["config.yaml", "client.csv", "data.json"];
    for file in &files {
        fs::copy(
            format!("src/{}", file),
            format!("{}/{}", target_dir, file)
        ).unwrap();
    }

    // 让 cargo 在配置文件改变时重新运行构建脚本
    println!("cargo:rerun-if-changed=src/config.yaml");
}