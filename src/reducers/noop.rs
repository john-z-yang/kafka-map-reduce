use std::{marker::PhantomData, time::Duration};

use tracing::info;

use crate::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};

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
            shutdown_condition: ReduceShutdownCondition::Drain,
            shutdown_behaviour: ReduceShutdownBehaviour::Flush,
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            flush_interval: Some(Duration::from_secs(1)),
        }
    }
}
