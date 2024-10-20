use anyhow::{anyhow, Error};
use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use kafka_map_reduce::reducers::clickhouse::ClickhouseWriter;
use kafka_map_reduce::reducers::os_stream::{OsStream, OsStreamWriter};
use kafka_map_reduce::{processing_strategy, start_consumer, ReduceShutdownBehaviour};
use rdkafka::{config::RDKafkaLogLevel, message::OwnedMessage, ClientConfig, Message};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::FmtSubscriber;

#[derive(Debug, Serialize)]
struct Data {
    partition: u32,
    offset: u64,
    #[serde(with = "ts_seconds")]
    timestamp: DateTime<Utc>,
}

async fn parse(msg: Arc<OwnedMessage>) -> Result<Data, Error> {
    match msg.payload_view::<str>() {
        Some(res) => Ok(res.map(|_| Data {
            partition: msg.partition() as u32,
            offset: msg.offset() as u64,
            timestamp: Utc::now(),
        })?),
        None => Err(anyhow!("Message has no data")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    /*
    Create the table:

    CREATE TABLE kmr_consumer_ingest
    (
        `partition` UInt32,
        `offset` UInt64,
        `timestamp` DateTime
    )
    ENGINE = MergeTree
    PRIMARY KEY (partition, offset, timestamp)
    */

    /*
    Check for more-than-once delivery, delta > 0 means messages are missing:

    SELECT
        partition,
        (max(offset) - min(offset)) + 1 AS offset_diff,
        count(offset) AS occ,
        offset_diff - occ AS delta
    FROM (
        SELECT DISTINCT * FROM kmr_consumer_ingest
    )
    GROUP BY partition
    ORDER BY partition
     */

    /*
    Check for double writes:

    SELECT
        partition,
        offset,
        count() AS occ
    FROM kmr_consumer_ingest
    GROUP BY
        partition,
        offset
    HAVING occ > 1
     */

    let topic = "ingest-performance-metrics";
    let consumer_group = "test-map-reduce-consumer";
    let bootstrap_servers = "127.0.0.1:9092";

    let host = "localhost";
    let port = "8123";
    let table = "kmr_consumer_ingest";

    start_consumer(
        [topic].as_ref(),
        ClientConfig::new()
            .set("group.id", consumer_group)
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            .set("enable.auto.offset.store", "false")
            .set_log_level(RDKafkaLogLevel::Debug),
        processing_strategy!({
            map => parse,
            reduce => ClickhouseWriter::new(
                host,
                port,
                table,
                64,
                Duration::from_secs(4),
                ReduceShutdownBehaviour::Flush,
            ),
            reduce_err => OsStreamWriter::new(
                Duration::from_secs(1),
                OsStream::StdErr,
            ),
        }),
    )
    .await
}
