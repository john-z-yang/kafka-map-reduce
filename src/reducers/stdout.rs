use std::fmt::Debug;

use crate::{ReduceConfig, Reducer};

pub struct StdoutWriter<T> {
    buffer: Vec<T>,
    max_buf_size: usize,
    reduce_config: ReduceConfig,
}

impl<T> StdoutWriter<T> {
    pub fn new(max_buf_size: usize, reduce_config: ReduceConfig) -> Self {
        Self {
            buffer: Vec::with_capacity(max_buf_size),
            max_buf_size,
            reduce_config,
        }
    }
}

impl<T> Reducer for StdoutWriter<T>
where
    T: Debug + Send,
{
    type Item = T;

    fn reduce(&mut self, t: Self::Item) -> Result<(), anyhow::Error> {
        self.buffer.push(t);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        if !self.buffer.is_empty() {
            println!("================ BATCH BEGINS ================");
            for e in &self.buffer {
                println!("{:?}", e);
            }
            println!("================ BATCH ENDS ================");
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.buffer.clear()
    }

    fn is_full(&self) -> bool {
        self.buffer.len() >= self.max_buf_size
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        self.reduce_config.clone()
    }
}
