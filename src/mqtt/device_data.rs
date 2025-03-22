use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeviceData {
    pub data: Value,
}

impl DeviceData {

    pub fn get_data(&self) -> &Value {
        &self.data
    }
}
