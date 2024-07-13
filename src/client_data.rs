use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct ClientData {
    pub client_id: String,
    pub username: String,
    pub password: String,
    #[serde(skip)]
    pub device_key: String,
}

impl ClientData {
    pub fn new(client_id: String, username: String, password: String, device_key: String) -> Self {
        ClientData {
            client_id,
            username,
            password,
            device_key,
        }
    }

    pub fn get_client_id(&self) -> &str {
        &self.client_id
    }

    pub fn get_username(&self) -> &str {
        &self.username
    }

    pub fn get_password(&self) -> &str {
        &self.password
    }

    pub fn get_device_key(&self) -> &str {
        &self.device_key
    }

    pub fn set_client_id(&mut self, client_id: String) {
        self.client_id = client_id;
    }

    pub fn set_device_key(&mut self, device_key: String) {
        self.device_key = device_key;
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}
