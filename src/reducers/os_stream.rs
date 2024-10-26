use crate::{ReduceConfig, ReduceShutdownBehaviour, Reducer, ReducerWhenFullBehaviour};
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
    T: Debug + Send,
{
    type Item = T;

    async fn reduce(&mut self, t: Self::Item) -> Result<(), anyhow::Error> {
        self.data = Some(t);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        match self.os_stream {
            OsStream::StdOut => println!("{:?}", self.data.take().unwrap()),
            OsStream::StdErr => eprintln!("{:?}", self.data.take().unwrap()),
        }
        sleep(self.print_duration).await;
        Ok(())
    }

    fn reset(&mut self) {
        self.data.take();
    }

    fn is_full(&self) -> bool {
        self.data.is_some()
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        ReduceConfig {
            shutdown_behaviour: ReduceShutdownBehaviour::Flush,
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            flush_interval: None,
        }
    }
}
