use std::{
    fmt::Debug,
    sync::{atomic::AtomicU32, Arc},
};

use crate::command::BenchmarkConfig;

pub trait Client<T, C>: Send + Sync + Debug
where
    T: Debug,
{
    type Item;

    fn setup_clients(
        &self,
        config: &BenchmarkConfig<T, C>,
    ) -> impl std::future::Future<Output = Result<Vec<Self::Item>, Box<dyn std::error::Error>>> + Send;

    fn wait_for_connections(
        &self,
        clients: &mut [Self::Item],
    ) -> impl std::future::Future<Output = ()> + Send;

    fn on_connect_success(
        &self,
        client: &mut Self::Item,
    ) -> impl std::future::Future<Output = ()> + Send;
    fn spawn_message(
        &self,
        clients: Vec<Self::Item>,
        counter: Arc<AtomicU32>,
        config: &BenchmarkConfig<T, C>,
    ) -> impl std::future::Future<Output = ()> + Send;
}
