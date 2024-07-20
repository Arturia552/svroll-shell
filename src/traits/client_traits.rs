use std::{fmt::Debug, sync::{atomic::AtomicU32, Arc}};

pub trait Client<T>
where
    T: Debug,
{
    type Item;

    fn setup_clients(
        &self,
        client_data: &mut [T],
        broker: String,
    ) -> impl std::future::Future<Output = Result<Vec<Self::Item>, Box<dyn std::error::Error>>> + Send;

    fn wait_for_connections( clients: &[Self::Item]) -> impl std::future::Future<Output = ()> + Send;

    fn on_connect_success(
        client: &Self::Item,
    ) -> impl std::future::Future<Output = ()> + Send;
    fn spawn_message(
        &self,
        clients: Vec<Self::Item>,
        counter: Arc<AtomicU32>,
        thread_size: usize,
        setting_send_interval: u64,
    ) -> impl std::future::Future<Output = ()> + Send;
    
}
