use crate::{
    MapConfig, MapShutdownBehaviour, Mapper, ReduceConfig, ReduceShutdownBehaviour, Reducer,
    ReducerWhenFullBehaviour, ShutdownCondition,
};
use anyhow::{Error, Ok, anyhow};
use reqwest::{Client, Response};
use std::collections::HashMap;
use std::time::Duration;
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinHandle},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

struct WriteHandle {
    max_size: usize,
    cur_size: usize,
    write_stream: mpsc::Sender<Result<Vec<u8>, ::std::io::Error>>,
    response_handle: JoinHandle<Result<Response, Error>>,
}

impl WriteHandle {
    fn new(client: Client, url: String, max_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(max_size);

        Self {
            max_size,
            cur_size: 0,
            write_stream: sender,
            response_handle: tokio::spawn(async move {
                let res = client
                    .post(url)
                    .body(reqwest::Body::wrap_stream(ReceiverStream::new(receiver)))
                    .send()
                    .await?;
                Ok(res)
            }),
        }
    }

    async fn write(&mut self, data: Vec<u8>) -> Result<(), Error> {
        self.write_stream
            .send(Result::<_, _>::Ok(data))
            .await
            .map_err(|err| anyhow!("Unable to write to socket, got SendError: {:?}", err))
            .expect("We are always sending Ok during write");
        self.cur_size += 1;
        Ok(())
    }

    fn flush(self) -> JoinHandle<Result<Response, Error>> {
        self.response_handle
    }

    fn abort(&self) {
        self.response_handle.abort();
    }

    fn is_full(&self) -> bool {
        self.cur_size >= self.max_size
    }
}

pub struct ClickhouseBatchWriter {
    write_handle: Option<WriteHandle>,
    http_client: Client,
    max_buf_size: usize,
    url: String,
    reduce_config: ReduceConfig,
}

impl ClickhouseBatchWriter {
    pub fn new(
        host: &str,
        port: &str,
        table: &str,
        max_buf_size: usize,
        flush_interval: Duration,
        shutdown_behaviour: ReduceShutdownBehaviour,
    ) -> Self {
        Self {
            write_handle: None,
            http_client: Client::new(),
            max_buf_size,
            url: format!(
                "http://{}:{}/?query=INSERT%20INTO%20{}%20FORMAT%20JSONEachRow",
                host, port, table
            ),
            reduce_config: ReduceConfig {
                shutdown_condition: ShutdownCondition::Signal,
                shutdown_behaviour,
                when_full_behaviour: ReducerWhenFullBehaviour::Backpressure,
                flush_interval: Some(flush_interval),
            },
        }
    }
}

impl Reducer for ClickhouseBatchWriter {
    type Input = Vec<u8>;
    type Output = JoinHandle<Result<Result<Response, anyhow::Error>, JoinError>>;

    async fn reduce(&mut self, t: Self::Input) -> Result<(), anyhow::Error> {
        self.write_handle
            .get_or_insert_with(|| {
                WriteHandle::new(
                    self.http_client.clone(),
                    self.url.clone(),
                    self.max_buf_size,
                )
            })
            .write(t)
            .await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<Self::Output>, anyhow::Error> {
        let Some(write_handle) = self.write_handle.take() else {
            return Ok(None);
        };

        Ok(Some(tokio::spawn(write_handle.flush())))
    }

    fn reset(&mut self) {
        self.write_handle.take();
    }

    async fn is_full(&self) -> bool {
        self.write_handle
            .as_ref()
            .map_or(false, WriteHandle::is_full)
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        self.reduce_config.clone()
    }
}

impl Drop for ClickhouseBatchWriter {
    fn drop(&mut self) {
        self.write_handle.as_ref().map(WriteHandle::abort);
    }
}

pub struct ClickhouseAckHandler {
    pub concurrency: usize,
}

impl Mapper for ClickhouseAckHandler {
    type Input = JoinHandle<Result<Result<Response, anyhow::Error>, JoinError>>;

    type Output = ();

    async fn map(&self, t: Self::Input) -> Result<Self::Output, anyhow::Error> {
        let resp = t.await???;

        if resp.status().as_str() == "200" {
            info!(
                "Inserted: {:?} rows, query ID: {:?}",
                resp.headers()
                    .get("x-clickhouse-summary")
                    .and_then(|val| val.to_str().ok())
                    .and_then(|s| serde_json::from_str::<HashMap<String, String>>(s).ok())
                    .and_then(|map| map.get("written_rows").cloned())
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap(),
                resp.headers().get("x-clickhouse-query-id").unwrap(),
            );
            Ok(())
        } else {
            Err(anyhow!("Write failed: {:?}", resp))
        }
    }

    fn get_map_config(&self) -> MapConfig {
        MapConfig {
            concurrency: self.concurrency,
            shutdown_condition: ShutdownCondition::Drain,
            shutdown_behaviour: MapShutdownBehaviour::Drain,
        }
    }
}
