use crate::{ReduceConfig, ReduceShutdownBehaviour, Reducer};
use std::{fmt::Debug, marker::PhantomData, time::Duration};
use tokio::time::sleep;

pub enum OsStream {
    StdOut,
    StdErr,
}

pub struct OsStreamWriter<T> {
    print_duration: Duration,
    os_stream: OsStream,
    phantom: PhantomData<T>,
}

impl<T> OsStreamWriter<T> {
    pub fn new(print_duration: Duration, os_stream: OsStream) -> Self {
        Self {
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
        match self.os_stream {
            OsStream::StdOut => println!("{:?}", t),
            OsStream::StdErr => eprintln!("{:?}", t),
        }
        sleep(self.print_duration).await;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn reset(&mut self) {}

    fn is_full(&self) -> bool {
        false
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        ReduceConfig {
            shutdown_behaviour: ReduceShutdownBehaviour::Flush,
            flush_interval: self.print_duration,
        }
    }
}
