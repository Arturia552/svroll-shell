use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

pub fn random_value(input: &mut Value) {
    match input {
        Value::Null => {}
        Value::Bool(ref mut b) => {
            *b = manual_random_bool();
        }
        Value::Number(ref mut n) => {
            let number = manual_random_number(0, 100); // 生成0到99的随机数
            *n = serde_json::Number::from(number);
        }
        Value::String(ref mut s) => {
            *s = generate_random_string();
        }
        Value::Array(ref mut arr) => {
            *arr = generate_random_array();
        }
        Value::Object(ref mut obj) => {
            *obj = generate_random_object();
        }
    }
}

pub fn manual_random_number(min: u64, max: u64) -> u64 {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    (duration.as_secs() + duration.subsec_nanos() as u64) % (max - min) + min
}

pub fn manual_random_bool() -> bool {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    (duration.as_secs() + duration.subsec_nanos() as u64) % 2 == 0
}

pub fn generate_random_string() -> String {
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        .chars()
        .collect();
    let length = manual_random_number(5, 15) as usize; // 生成长度为5到15的随机字符串
    (0..length)
        .map(|_| chars[manual_random_number(0, chars.len() as u64) as usize])
        .collect()
}

pub fn generate_random_array() -> Vec<Value> {
    let length = manual_random_number(1, 10) as usize; // 生成1到10个随机元素
    let mut result = Vec::with_capacity(length);
    for _ in 0..length {
        let mut val = Value::Null;
        random_value(&mut val); // 直接修改val
        result.push(val);
    }
    result
}

pub fn generate_random_object() -> Map<String, Value> {
    let length = manual_random_number(1, 10) as usize; // 生成1到10对随机键值对
    let mut map = Map::new();
    for _ in 0..length {
        let mut val = Value::Null;
        random_value(&mut val); // 直接修改val
        map.insert(generate_random_string(), val);
    }
    map
}
