use crate::{ReduceConfig, ReduceShutdownBehaviour, Reducer};
use anyhow::{anyhow, Ok};
use reqwest::Client;
use std::{collections::HashMap, mem::replace, time::Duration};
use tracing::info;

pub struct ClickhouseWriter {
    buffer: Vec<u8>,
    http_client: Client,
    max_buf_size: usize,
    url: String,
    reduce_config: ReduceConfig,
}

impl ClickhouseWriter {
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

impl Reducer for ClickhouseWriter {
    type Item = Vec<u8>;

    async fn reduce(&mut self, t: Self::Item) -> Result<(), anyhow::Error> {
        self.buffer.extend(&t);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let res = self
            .http_client
            .post(self.url.clone())
            .body(replace(
                &mut self.buffer,
                Vec::with_capacity(self.max_buf_size),
            ))
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
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap(),
                res.headers().get("x-clickhouse-query-id").unwrap(),
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
