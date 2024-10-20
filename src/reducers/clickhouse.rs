use crate::{ReduceConfig, ReduceShutdownBehaviour, Reducer};
use anyhow::{anyhow, Ok};
use reqwest::Client;
use serde::Serialize;
use std::{collections::HashMap, mem::take, time::Duration};
use tracing::info;

pub struct ClickhouseWriter<T> {
    buffer: Vec<T>,
    http_client: Client,
    max_buf_size: usize,
    url: String,
    reduce_config: ReduceConfig,
}

impl<T> ClickhouseWriter<T> {
    pub fn new(
        host: &str,
        port: &str,
        table: &str,
        max_buf_size: usize,
        flush_interval: Duration,
        shutdown_behaviour: ReduceShutdownBehaviour,
    ) -> Self {
        Self {
            buffer: Vec::with_capacity(max_buf_size),
            http_client: Client::new(),
            max_buf_size,
            url: format!(
                "http://{}:{}/?query=INSERT%20INTO%20{}%20FORMAT%20JSONEachRow",
                host, port, table
            ),
            reduce_config: ReduceConfig {
                shutdown_behaviour,
                flush_interval: Some(flush_interval),
            },
        }
    }
}

impl<T> Reducer for ClickhouseWriter<T>
where
    T: Serialize + Send,
{
    type Item = T;

    async fn reduce(&mut self, t: Self::Item) -> Result<(), anyhow::Error> {
        self.buffer.push(t);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let res = self
            .http_client
            .post(self.url.clone())
            .json(&take(&mut self.buffer))
            .send()
            .await?;

        if res.status().as_str() == "200" {
            info!(
                "Inserted: {:?} rows, query ID: {:?}",
                res.headers()
                    .get("x-clickhouse-summary")
                    .and_then(|val| val.to_str().ok())
                    .and_then(|s| serde_json::from_str::<HashMap<String, String>>(s).ok())
                    .and_then(|map| map.get("written_rows").cloned())
                    .and_then(|s| s.parse::<u32>().ok()),
                res.headers().get("x-clickhouse-query-id"),
            );
            Ok(())
        } else {
            Err(anyhow!("Write failed: {:?}", res))
        }
    }

    fn reset(&mut self) {
        self.buffer.clear();
    }

    fn is_full(&self) -> bool {
        self.buffer.len() >= self.max_buf_size
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        self.reduce_config.clone()
    }
}
