use std::marker::PhantomData;

use std::time::Duration;
use tracing::info;

use crate::{
    MapConfig, MapShutdownBehaviour, Mapper, ReduceConfig, ReduceShutdownBehaviour, Reducer,
    ReducerWhenFullBehaviour, ShutdownCondition,
};

pub struct NoopMapper<T> {
    phantom: PhantomData<T>,
    id: String,
}

impl<T> NoopMapper<T> {
    pub fn new(id: &str) -> Self {
        Self {
            phantom: PhantomData,
            id: id.into(),
        }
    }
}

impl<T> Mapper for NoopMapper<T>
where
    T: Send + Sync,
{
    type Input = T;

    type Output = ();

    async fn map(&self, _t: Self::Input) -> Result<Self::Output, anyhow::Error> {
        info!("Noop mapper id: {} mapped", self.id);
        Ok(())
    }

    fn get_map_config(&self) -> MapConfig {
        MapConfig {
            concurrency: 16,
            shutdown_condition: ShutdownCondition::Drain,
            shutdown_behaviour: MapShutdownBehaviour::Drain,
        }
    }
}

pub struct NoopReducer<T> {
    phantom: PhantomData<T>,
    id: String,
}

impl<T> NoopReducer<T> {
    pub fn new(id: &str) -> Self {
        Self {
            phantom: PhantomData,
            id: id.into(),
        }
    }
}

impl<T> Reducer for NoopReducer<T>
where
    T: Send + Sync,
{
    type Input = T;
    type Output = ();

    async fn reduce(&mut self, _t: Self::Input) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<()>, anyhow::Error> {
        info!("Noop reducer id: {} flushed", self.id);
        Ok(Some(()))
    }

    fn reset(&mut self) {}

    async fn is_full(&self) -> bool {
        false
    }

    fn get_reduce_config(&self) -> crate::ReduceConfig {
        ReduceConfig {
            shutdown_condition: ShutdownCondition::Drain,
            shutdown_behaviour: ReduceShutdownBehaviour::Flush,
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            flush_interval: Some(Duration::from_secs(1)),
        }
    }
}
