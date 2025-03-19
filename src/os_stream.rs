use crate::{
    ReduceConfig, ReduceShutdownBehaviour, Reducer, ReducerWhenFullBehaviour, ShutdownCondition,
};
use std::{fmt::Debug, marker::PhantomData, time::Duration};
use tokio::time::sleep;

pub enum OsStream {
    StdOut,
    StdErr,
}

pub struct OsStreamWriter<T> {
    data: Option<T>,
    print_duration: Duration,
    os_stream: OsStream,
    phantom: PhantomData<T>,
}

impl<T> OsStreamWriter<T> {
    pub fn new(print_duration: Duration, os_stream: OsStream) -> Self {
        Self {
            data: None,
            print_duration,
            os_stream,
            phantom: PhantomData::<T>,
        }
    }
}

impl<T> Reducer for OsStreamWriter<T>
where
    T: Debug + Send + Sync,
{
    type Input = T;
    type Output = ();

    async fn reduce(&mut self, t: Self::Input) -> Result<(), anyhow::Error> {
        self.data = Some(t);
        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<()>, anyhow::Error> {
        let Some(data) = self.data.take() else {
            return Ok(None);
        };
        match self.os_stream {
            OsStream::StdOut => println!("{:?}", data),
            OsStream::StdErr => eprintln!("{:?}", data),
        }
        sleep(self.print_duration).await;
        Ok(Some(()))
    }

    fn reset(&mut self) {
        self.data.take();
    }

    async fn is_full(&self) -> bool {
        self.data.is_some()
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        ReduceConfig {
            shutdown_condition: ShutdownCondition::Signal,
            shutdown_behaviour: ReduceShutdownBehaviour::Flush,
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            flush_interval: None,
        }
    }
}
