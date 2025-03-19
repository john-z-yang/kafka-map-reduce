# kafka-map-reduce

A simple toy kafka consumer framework implementation in Rust. Built on top of [Tokio](https://tokio.rs/) and [rdkafka](https://docs.rs/rdkafka/latest/rdkafka/index.html)'s [StreamConsumer](https://docs.rs/rdkafka/latest/rdkafka/consumer/stream_consumer/struct.StreamConsumer.html) to be fully asynchronous. Most techniques are stolen from [Vector](https://vector.dev/) and [Arroyo](https://github.com/getsentry/arroyo).

There are 4 kinds of processing stages provided by the `processing_strategy` macro. The first 2, map and reduce, can be arranged in any arbitrary pre-defined order. The latter 2, `par_map` and `err` can only be defined once.

1. **map**: 1 → 1 transformation that runs concurrently.
2. **reduce**: N → 1 transformation that runs concurrently.
3. **par_map**: 1 → 1 transformation is defined once at the beginning of the `processing_strategy`. It will be run in *parallel* on each messages as n separate tokio tasks with n being the number of partitions assigned to the consumer.
4. **err**: N → 1 transformation is defined once at the beginning of the `processing_strategy`. It will be recieve all the *original* kafka messages when any processing stage returned an `anyhow::Error` instead of `Ok(T)`.

## Demo

There is a demo in [main.rs](https://github.com/john-z-yang/kafka-map-reduce-consumer/blob/main/src/main.rs).

```rust
#[tokio::main]
async fn main() -> Result<(), Error> {
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
            err: OsStreamWriter::new(
                Duration::from_secs(1),
                OsStream::StdErr,
            ),

            par_map: parse,
            reduce: ClickhouseBatchWriter::new(
                host,
                port,
                table,
                128,
                Duration::from_secs(2),
                ReduceShutdownBehaviour::Flush,
            ),
            map: ClickhouseAckHandler{
                concurrency: 1,
            },
        }),
    )
    .await
}

```

Start the demo with:

```console
cargo run
```

The demo expects a Kafka broker on `127.0.0.1:9092`, clickhouse server on `127.0.0.1:8123`, with a table `kmr_consumer_ingest`.

Create the table like so:

```sql
CREATE TABLE kmr_consumer_ingest
(
    `partition` UInt32,
    `offset` UInt64,
    `timestamp` DateTime
)
ENGINE = MergeTree
PRIMARY KEY (partition, offset, timestamp)
```

Send some messages to `ingest-performance-metrics`, any messages will do.

Check for more-than-once delivery, `delta` > 0 means messages are missing:

```sql
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
```

Check for double writes:

```sql
SELECT
    partition,
    offset,
    count() AS occ
FROM kmr_consumer_ingest
GROUP BY
    partition,
    offset
HAVING occ > 1
```

## How it works

Identical to [Vector](https://vector.dev/docs/about/under-the-hood/architecture/concurrency-model/). Each step is a Tokio task. Tasks forward data to the next phase using a bounded MPSC channel for backpressure and buffering. The event handler task performs all the coordination necessary between events (such as Kafka partition reassignment or SIGKILL) and tasks. It uses a cancellation token for graceful shutdown signal and oneshot channels for synchronous rendezvous.

### Architecture

```text
┌───────────────────────────────────────────┐                                                       
│ SIG: Signals  MSG: Messages  OFF: Offsets │                                                       
└───────────────────────────────────────────┘                                                       
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│  consumer                                                                                        │
│  ┌──────────────┐                      ┌──────────┐                                              │
│  │RdKafka client│◄──SIG──┐   ┌───SIG───┤OS signals│                                              │
│  └──────────────┘        │   │         └──────────┘                                              │
│                          ▼   ▼                                                                   │
│                     ┌─────────────┐                                                              │
│                     │Event Handler│                                                              │
│                     └─────────────┘                                                              │
│                            ▲                                                                     │
│                            │                                                                     │
│                           SIG                                                                    │
│                            │                                                                     │
│                            ▼                                                                     │
│ ┌──────────────────────────────────────────────────────────────────────────────────────────────┐ │
│ │ processing_strategy                                                                          │ │
│ │ ┌─────────────┐                                                                              │ │
│ │ │ParMap       │                                                                              │ │
│ │ │┌───────────┐├────┐                                                                         │ │
│ │ ││Partition_0││    │                                                                         │ │
│ │ │└───────────┘│    │                                                                         │ │
│ │ └─────────────┘    │       ┌──────────┐                   ┌──────────┐                       │ │
│ │        .           │       │ Reduce_0 │                   │ Reduce_m │        ┌──────┐       │ │
│ │        .           ├──MSG─►│    /     ├──MSG──►...──MSG──►│    /     │──OFF──►│Commit│       │ │
│ │        .           │       │  Map_0   │         │         │  Map_m   │        └──────┘       │ │
│ │ ┌─────────────┐    │       └────┬─────┘         │         └────┬─────┘                       │ │
│ │ │ParMap       │    │            │               │              │                             │ │
│ │ │┌───────────┐│    │            └───────────────┼──────────────┘                             │ │
│ │ ││Partition_n│├────┤                            │                                            │ │
│ │ │└───────────┘│    │                            │                                            │ │
│ │ └─────────────┘    │                            │                                            │ │
│ │                    │                            ▼                                            │ │
│ │                    │                       ┌──────────┐                                      │ │
│ │                    └──────────────────────►│Reduce Err│                                      │ │
│ │                                            └──────────┘                                      │ │
│ │                                                                                              │ │
│ │                                                                                              │ │
│ └──────────────────────────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```
