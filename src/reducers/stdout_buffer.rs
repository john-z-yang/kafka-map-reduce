use std::fmt::Debug;

use crate::Reducer;

pub struct StdoutBuffer<T> {
    buffer: Vec<T>,
    max_buf_size: usize,
    flush_interval: u64,
}

impl<T> StdoutBuffer<T> {
    pub fn new(max_buf_size: usize, flush_interval: u64) -> Self {
        Self {
            buffer: Vec::with_capacity(max_buf_size),
            max_buf_size,
            flush_interval,
        }
    }
}

impl<T> Reducer for StdoutBuffer<T>
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

    fn get_flush_interval(&self) -> u64 {
        self.flush_interval
    }
}
