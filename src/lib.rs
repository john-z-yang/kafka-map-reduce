use anyhow::{anyhow, Error};
use futures::{pin_mut, Stream, StreamExt};
use rdkafka::{
    consumer::{
        stream_consumer::StreamPartitionQueue, Consumer, ConsumerContext, Rebalance, StreamConsumer,
    },
    error::{KafkaError, KafkaResult},
    message::{BorrowedMessage, OwnedMessage},
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
};
use std::{
    cmp,
    collections::HashMap,
    fmt::Debug,
    future::Future,
    mem::take,
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc,
    },
    time::Duration,
};
use tokio::{
    select, signal,
    sync::{
        mpsc::{self, unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task::JoinSet,
    time::{self, sleep},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

pub mod reducers;

pub async fn start_consumer(
    topics: &[&str],
    kafka_client_config: &ClientConfig,
    spawn_actors: impl FnMut(Arc<StreamConsumer<KafkaContext>>, &[(String, i32)]) -> ActorHandles,
) -> Result<(), Error> {
    let (client_shutdown_sender, client_shutdown_receiver) = oneshot::channel();
    let (event_sender, event_receiver) = unbounded_channel();

    let context = KafkaContext::new(event_sender.clone());

    let consumer: Arc<StreamConsumer<KafkaContext>> = Arc::new(
        kafka_client_config
            .create_with_context(context)
            .expect("Consumer creation failed"),
    );

    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");

    handle_os_signals(event_sender.clone());
    handle_consumer_client(consumer.clone(), client_shutdown_receiver);
    handle_events(
        consumer,
        event_receiver,
        client_shutdown_sender,
        spawn_actors,
    )
    .await
}

pub fn handle_os_signals(event_sender: UnboundedSender<(Event, SyncSender<()>)>) {
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        let (rendezvous_sender, rendezvous_receiver) = sync_channel(0);
        let _ = event_sender.send((Event::Shutdown, rendezvous_sender));
        let _ = rendezvous_receiver.recv();
    });
}

#[instrument(skip(consumer, shutdown))]
pub fn handle_consumer_client(
    consumer: Arc<StreamConsumer<KafkaContext>>,
    shutdown: oneshot::Receiver<()>,
) {
    tokio::spawn(async move {
        select! {
            biased;
            _ = shutdown => {
                debug!("Received shutdown signal, commiting state in sync mode...");
                let _ = consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Sync);
            }
            _ = consumer.recv() => {
                panic!("We're cooked");
            }
        }
        debug!("Shutdown complete");
    });
}

#[derive(Debug)]
pub struct KafkaContext {
    event_sender: UnboundedSender<(Event, SyncSender<()>)>,
}

impl KafkaContext {
    pub fn new(event_sender: UnboundedSender<(Event, SyncSender<()>)>) -> Self {
        Self { event_sender }
    }
}

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
    #[instrument(skip(self, rebalance))]
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        let (rendezvous_sender, rendezvous_receiver) = sync_channel(0);
        match rebalance {
            Rebalance::Assign(tpl) => {
                info!("Got pre-rebalance callback, kind: Assign");
                let _ = self.event_sender.send((
                    Event::Assign(tpl.to_topic_map().keys().cloned().collect()),
                    rendezvous_sender,
                ));
                info!("Parition assignment event sent, waiting for rendezvous...");
                let _ = rendezvous_receiver.recv();
                info!("Rendezvous complete");
            }
            Rebalance::Revoke(tpl) => {
                info!("Got pre-rebalance callback, kind: Revoke");
                let _ = self.event_sender.send((
                    Event::Revoke(tpl.to_topic_map().keys().cloned().collect()),
                    rendezvous_sender,
                ));
                info!("Parition assignment event sent, waiting for rendezvous...");
                let _ = rendezvous_receiver.recv();
                info!("Rendezvous complete");
            }
            Rebalance::Error(err) => {
                info!("Got pre-rebalance callback, kind: Error");
                error!("Got rebalance error: {}", err);
            }
        }
    }

    #[instrument(skip(self))]
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        debug!("Got commit callback");
    }
}

#[derive(Debug)]
pub enum Event {
    Assign(Vec<(String, i32)>),
    Revoke(Vec<(String, i32)>),
    Shutdown,
}

pub type ActorHandles = (
    JoinSet<Result<(), Error>>,
    CancellationToken,
    oneshot::Receiver<()>,
);

#[macro_export]
macro_rules! processing_strategy {
    (
        {
            map => $map_fn:ident,
            reduce => $reduce:expr,
            reduce_err => $reduce_err:expr$(,)?
        }
    ) => {{
        |consumer: Arc<rdkafka::consumer::StreamConsumer<$crate::KafkaContext>>,
         tpl: &[(String, i32)]|
         -> $crate::ActorHandles {
            let mut handles = tokio::task::JoinSet::new();
            let mut shutdown_signal = tokio_util::sync::CancellationToken::new();

            let (rendezvous_sender, rendezvous_receiver) = tokio::sync::oneshot::channel();

            let reducer = $reduce;
            let err_reducer = $reduce_err;

            const MAX_CHANNEL_BUF_SIZE: usize = 1048576;
            let (reduce_sender, reduce_receiver) = tokio::sync::mpsc::channel(MAX_CHANNEL_BUF_SIZE);
            let (commit_sender, commit_receiver) = tokio::sync::mpsc::channel(MAX_CHANNEL_BUF_SIZE);
            let (err_sender, err_receiver) = tokio::sync::mpsc::channel(MAX_CHANNEL_BUF_SIZE);

            for (topic, partition) in tpl.iter() {
                let queue = consumer
                    .split_partition_queue(topic, *partition)
                    .expect("Topic and parition should always be splittable");

                handles.spawn($crate::map(
                    queue,
                    $map_fn,
                    reduce_sender.clone(),
                    err_sender.clone(),
                    shutdown_signal.clone(),
                ));
            }

            handles.spawn($crate::reduce(
                reducer,
                reduce_receiver,
                commit_sender.clone(),
                err_sender.clone(),
                shutdown_signal.clone(),
            ));

            handles.spawn($crate::reduce_err(
                err_reducer,
                err_receiver,
                commit_sender.clone(),
                shutdown_signal.clone(),
            ));

            handles.spawn($crate::commit(
                commit_receiver,
                consumer.clone(),
                rendezvous_sender,
            ));

            (handles, shutdown_signal, rendezvous_receiver)
        }
    }};
}

#[derive(Debug)]
enum ActorState {
    Ready,
    Consuming(ActorHandles),
    Stopped,
}

#[instrument(skip(consumer, events, shutdown_client, spawn_actors))]
pub async fn handle_events(
    consumer: Arc<StreamConsumer<KafkaContext>>,
    events: UnboundedReceiver<(Event, SyncSender<()>)>,
    shutdown_client: oneshot::Sender<()>,
    mut spawn_actors: impl FnMut(Arc<StreamConsumer<KafkaContext>>, &[(String, i32)]) -> ActorHandles,
) -> Result<(), anyhow::Error> {
    const CALLBACK_DURATION: Duration = Duration::from_secs(1);

    let mut shutdown_client = Some(shutdown_client);
    let mut events_stream = UnboundedReceiverStream::new(events);

    let mut state = ActorState::Ready;

    while let ActorState::Ready { .. } | ActorState::Consuming { .. } = state {
        state = select! {
            biased;
            Some((event, _rendezvous_guard)) = &mut events_stream.next() => {
                info!("Recieved event: {:?}", event);
                match (state, event) {
                    (ActorState::Ready, Event::Assign(tpl)) => {
                        let handles = spawn_actors(consumer.clone(), &tpl);

                        ActorState::Consuming(handles)
                    }
                    (ActorState::Ready, Event::Revoke(_)) => {
                        unreachable!("Got partition revocation before the consumer has started")
                    },
                    (ActorState::Ready, Event::Shutdown) => {
                        ActorState::Stopped
                    },
                    (ActorState::Consuming { .. }, Event::Assign(_)) => {
                        unreachable!("Got partition assignment after consumer has started")
                    },
                    (ActorState::Consuming((_, shutdown_actors, mut rendezvous)),
                        Event::Revoke(_),
                    ) => {
                        debug!("Signaling shutdown to actors...");
                        shutdown_actors.cancel();
                        info!("Actor shutdown signaled, waiting for rendezvous...");

                        select! {
                            _ = &mut rendezvous => {
                                info!(
                                    "Rendezvous complete within callback deadline,\
                                     transitioning actor state to Ready"
                                );
                                ActorState::Ready
                            }
                            _ = sleep(CALLBACK_DURATION) => {
                                debug!(
                                    "Unable to rendezvous within callback deadline, \
                                    transitioning actor state to Draining"
                                );
                                todo!(
                                    "schedule a drain deadline here, \
                                    poll it in the select arm, evaluate to ActorState::Draining"
                                );
                            }
                        }
                    }
                    (
                        ActorState::Consuming((_, shutdown_actors, mut rendezvous)),
                        Event::Shutdown,
                    ) => {
                        debug!("Signaling shutdown to actors...");
                        shutdown_actors.cancel();
                        info!("Actor shutdown signaled, waiting for rendezvous...");

                        select! {
                            _ = &mut rendezvous => {
                                info!(
                                    "Rendezvous complete within callback deadline, \
                                    transitioning actor state to Stopped"
                                );
                                debug!("Signaling shutdown to client...");
                                shutdown_client.take();
                                ActorState::Stopped
                            }
                            _ = sleep(CALLBACK_DURATION) => {
                                debug!(
                                    "Unable to rendezvous within callback deadline, \
                                    transitioning actor state to Closing"
                                );
                                todo!(
                                    "schedule a drain deadline here, \
                                    poll it in the select arm, evaluate to ActorState::Closing"
                                );
                            }
                        }
                    }
                    (ActorState::Stopped, _) => {
                        unreachable!("Got event after consumer has stopped")
                    },
                }
            }
            else => unreachable!("Unexpected end to event stream")
        }
    }
    debug!("Shutdown complete");
    Ok(())
}

trait KafkaMessage {
    fn detach(&self) -> OwnedMessage;
}

impl KafkaMessage for BorrowedMessage<'_> {
    fn detach(&self) -> OwnedMessage {
        self.detach()
    }
}

trait MessageQueue {
    fn stream(&self) -> impl Stream<Item = Result<impl KafkaMessage, KafkaError>>;
}

impl MessageQueue for StreamPartitionQueue<KafkaContext> {
    fn stream(&self) -> impl Stream<Item = Result<impl KafkaMessage, KafkaError>> {
        self.stream()
    }
}

#[instrument(skip(queue, transform, ok, err, shutdown))]
pub async fn map<Fut, R>(
    queue: impl MessageQueue,
    transform: impl Fn(Arc<OwnedMessage>) -> Fut,
    ok: mpsc::Sender<(OwnedMessage, R)>,
    err: mpsc::Sender<OwnedMessage>,
    shutdown: CancellationToken,
) -> Result<(), Error>
where
    Fut: Future<Output = Result<R, Error>> + Send,
{
    let stream = queue.stream();
    pin_mut!(stream);

    loop {
        select! {
            biased;

            _ = shutdown.cancelled() => {
                break;
            }

            val = stream.next() => {
                let Some(Ok(msg)) = val else {
                    break;
                };
                let msg = Arc::new(msg.detach());
                match transform(msg.clone()).await {
                    Ok(transformed) => {
                        ok.send((
                            Arc::try_unwrap(msg).expect("msg should only have a single strong ref"),
                            transformed,
                        ))
                        .await
                        .map_err(|err| anyhow!("{}", err))?;
                    }
                    Err(e) => {
                        error!(
                            "Failed to map message at (topic: {}, partition: {}, offset: {}), reason: {}",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                            e,
                        );
                        err.send(
                            Arc::try_unwrap(msg).expect("msg should only have a single strong ref"),
                        )
                        .await
                        .expect("reduce_err should always be available");
                    }
                }
            }
        }
    }
    debug!("Shutdown complete");
    Ok(())
}

#[derive(Default)]
struct HighwaterMark {
    data: HashMap<(String, i32), i64>,
}

impl HighwaterMark {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    fn track(&mut self, msg: &OwnedMessage) {
        let cur_offset = self
            .data
            .entry((msg.topic().to_string(), msg.partition()))
            .or_insert(msg.offset() + 1);
        *cur_offset = cmp::max(*cur_offset, msg.offset() + 1);
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl From<HighwaterMark> for TopicPartitionList {
    fn from(val: HighwaterMark) -> Self {
        let mut tpl = TopicPartitionList::with_capacity(val.len());
        for ((topic, partition), offset) in val.data.iter() {
            tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset))
                .expect("Partition offset should always be valid");
        }
        tpl
    }
}

#[derive(Debug, Clone)]
pub struct ReduceConfig {
    pub shutdown_behaviour: ReduceShutdownBehaviour,
    pub flush_interval: Duration,
}

#[derive(Debug, Clone)]
pub enum ReduceShutdownBehaviour {
    Flush,
    Drop,
}

pub trait Reducer {
    type Item;

    fn reduce(&mut self, t: Self::Item) -> Result<(), anyhow::Error>;
    fn flush(&mut self) -> impl std::future::Future<Output = Result<(), anyhow::Error>> + Send;
    fn reset(&mut self);
    fn is_full(&self) -> bool;
    fn get_reduce_config(&self) -> ReduceConfig;
}

async fn handle_reducer_failure<T>(
    reducer: &mut impl Reducer<Item = T>,
    batched_msg: &mut Vec<OwnedMessage>,
    highwater_mark: &mut HighwaterMark,
    err: &mpsc::Sender<OwnedMessage>,
) {
    for msg in take(batched_msg).into_iter() {
        err.send(msg)
            .await
            .expect("reduce_err should always be available");
    }
    highwater_mark.clear();
    reducer.reset();
}

#[instrument(skip(reducer, batched_msg, highwater_mark, ok, err))]
async fn flush_reducer<T>(
    reducer: &mut impl Reducer<Item = T>,
    batched_msg: &mut Vec<OwnedMessage>,
    highwater_mark: &mut HighwaterMark,
    ok: &mpsc::Sender<TopicPartitionList>,
    err: &mpsc::Sender<OwnedMessage>,
) -> Result<(), Error> {
    match reducer.flush().await {
        Err(e) => {
            error!("Failed to flush reducer, reason: {}", e);
            handle_reducer_failure(reducer, batched_msg, highwater_mark, err).await;
        }
        Ok(()) => {
            batched_msg.clear();
            if !highwater_mark.is_empty() {
                ok.send(take(highwater_mark).into())
                    .await
                    .map_err(|err| anyhow!("{}", err))?;
            }
            reducer.reset();
        }
    }
    Ok(())
}

#[instrument(skip(reducer, receiver, ok, err, shutdown))]
pub async fn reduce<T>(
    mut reducer: impl Reducer<Item = T>,
    mut receiver: mpsc::Receiver<(OwnedMessage, T)>,
    ok: mpsc::Sender<TopicPartitionList>,
    err: mpsc::Sender<OwnedMessage>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let config = reducer.get_reduce_config();
    let mut interval = time::interval(config.flush_interval);

    let mut highwater_mark = HighwaterMark::new();
    let mut batched_msg = Vec::new();

    loop {
        select! {
            biased;

            _ = shutdown.cancelled() => {
                match config.shutdown_behaviour {
                    ReduceShutdownBehaviour::Flush => {
                        debug!("Received shutdown signal, flushing batch...");
                        flush_reducer(
                            &mut reducer,
                            &mut batched_msg,
                            &mut highwater_mark,
                            &ok,
                            &err
                        ).await?;
                    },
                    ReduceShutdownBehaviour::Drop => {
                        debug!("Received shutdown signal, dropping batch...");
                        drop(reducer);
                    },
                }
                break;
            }

            _ = interval.tick() => {
                flush_reducer(
                    &mut reducer,
                    &mut batched_msg,
                    &mut highwater_mark,
                    &ok,
                    &err
                ).await?;
            }

            val = receiver.recv(), if !reducer.is_full() => {
                let Some((msg, value)) = val else {
                    debug!("Received end of stream, flushing batch...");
                    flush_reducer(
                        &mut reducer,
                        &mut batched_msg,
                        &mut highwater_mark,
                        &ok,
                        &err
                    ).await?;
                    break;
                };
                highwater_mark.track(&msg);
                batched_msg.push(msg);

                if let Err(e) = reducer.reduce(value) {
                    error!(
                        "Failed to reduce message at (topic: {}, partition: {}, offset: {}), reason: {}",
                        batched_msg.last().unwrap().topic(),
                        batched_msg.last().unwrap().partition(),
                        batched_msg.last().unwrap().offset(),
                        e
                    );
                    handle_reducer_failure(
                        &mut reducer,
                        &mut batched_msg,
                        &mut highwater_mark,
                        &err
                    ).await;
                }
            }
        }
    }
    debug!("Shutdown complete");
    Ok(())
}

#[instrument(skip(reducer, receiver, ok, shutdown))]
pub async fn reduce_err(
    mut reducer: impl Reducer<Item = OwnedMessage>,
    mut receiver: mpsc::Receiver<OwnedMessage>,
    ok: mpsc::Sender<TopicPartitionList>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let config = reducer.get_reduce_config();
    let mut interval = time::interval(config.flush_interval);

    let mut highwater_mark = HighwaterMark::new();

    loop {
        select! {
            biased;

            _ = shutdown.cancelled() => {
                match config.shutdown_behaviour {
                    ReduceShutdownBehaviour::Flush => {
                        debug!("Received shutdown signal, flushing batch...");
                        reducer
                            .flush()
                            .await
                            .expect("error reducer flush should always be successful");
                    },
                    ReduceShutdownBehaviour::Drop => {
                        debug!("Received shutdown signal, dropping batch...");
                        drop(reducer);
                    },
                }
                break;
            }

            _ = interval.tick() => {
                reducer
                    .flush()
                    .await
                    .expect("error reducer flush should always be successful");
                reducer.reset();

                if !highwater_mark.is_empty() {
                    ok.send(take(&mut highwater_mark).into())
                        .await
                        .map_err(|err| anyhow!("{}", err))?;
                }
            }

            val = receiver.recv(), if !reducer.is_full() => {
                let Some(msg) = val else {
                    debug!("Received end of stream, flushing batch...");
                    reducer
                        .flush()
                        .await
                        .expect("error reducer flush should always be successful");
                    break;
                };
                highwater_mark.track(&msg);

                reducer
                    .reduce(msg)
                    .expect("error reducer reduce should always be successful");
            }
        }
    }
    debug!("Shutdown complete");
    Ok(())
}

trait CommitClient {
    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()>;
}

impl CommitClient for StreamConsumer<KafkaContext> {
    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        Consumer::store_offsets(self, tpl)
    }
}

#[instrument(skip(receiver, consumer, _rendezvous_guard))]
pub async fn commit(
    mut receiver: mpsc::Receiver<TopicPartitionList>,
    consumer: Arc<impl CommitClient>,
    _rendezvous_guard: oneshot::Sender<()>,
) -> Result<(), Error> {
    while let Some(ref tpl) = receiver.recv().await {
        debug!("Storing offsets");
        consumer.store_offsets(tpl).unwrap();
    }
    debug!("Shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicU8, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    };

    use anyhow::anyhow;
    use rdkafka::{
        error::KafkaResult, message::OwnedMessage, Message, Offset, Timestamp, TopicPartitionList,
    };
    use tokio::{
        sync::{mpsc, oneshot},
        time::sleep,
    };
    use tokio_util::sync::CancellationToken;

    use crate::{
        commit, reduce, reduce_err, CommitClient, ReduceConfig, ReduceShutdownBehaviour, Reducer,
    };

    struct MockCommitClient {
        offsets: Arc<RwLock<Vec<TopicPartitionList>>>,
    }

    impl CommitClient for MockCommitClient {
        fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
            self.offsets.write().unwrap().push(tpl.clone());
            Ok(())
        }
    }

    struct MockReducer<T> {
        buffer: Arc<RwLock<Vec<T>>>,
        pipe: Arc<RwLock<Vec<T>>>,
    }

    impl<T> MockReducer<T> {
        fn new() -> Self {
            Self {
                buffer: Arc::new(RwLock::new(Vec::new())),
                pipe: Arc::new(RwLock::new(Vec::new())),
            }
        }

        fn get_buffer(&self) -> Arc<RwLock<Vec<T>>> {
            self.buffer.clone()
        }

        fn get_pipe(&self) -> Arc<RwLock<Vec<T>>> {
            self.pipe.clone()
        }
    }

    impl<T> Reducer for MockReducer<T>
    where
        T: Send + Sync + Clone,
    {
        type Item = T;

        fn reduce(&mut self, t: Self::Item) -> Result<(), anyhow::Error> {
            self.buffer.write().unwrap().push(t);
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), anyhow::Error> {
            self.pipe
                .write()
                .unwrap()
                .extend(self.buffer.read().unwrap().clone().into_iter());
            Ok(())
        }

        fn reset(&mut self) {
            self.buffer.write().unwrap().clear();
        }

        fn is_full(&self) -> bool {
            self.buffer.read().unwrap().len() >= 32
        }

        fn get_reduce_config(&self) -> crate::ReduceConfig {
            ReduceConfig {
                shutdown_behaviour: ReduceShutdownBehaviour::Flush,
                flush_interval: Duration::from_secs(1),
            }
        }
    }

    enum ReducerFailure {
        FailOnReduce,
        FailOnFlush,
    }
    struct MockAlwaysErrReducer {
        failure: ReducerFailure,
        num_reduce: Arc<AtomicU8>,
        num_flush: Arc<AtomicU8>,
        num_reset: Arc<AtomicU8>,
    }

    impl MockAlwaysErrReducer {
        fn new(failure: ReducerFailure) -> Self {
            Self {
                failure,
                num_reduce: Arc::new(AtomicU8::new(0)),
                num_flush: Arc::new(AtomicU8::new(0)),
                num_reset: Arc::new(AtomicU8::new(0)),
            }
        }

        fn get_num_reduce(&self) -> Arc<AtomicU8> {
            self.num_reduce.clone()
        }

        fn get_num_flush(&self) -> Arc<AtomicU8> {
            self.num_flush.clone()
        }

        fn get_num_reset(&self) -> Arc<AtomicU8> {
            self.num_reset.clone()
        }
    }

    impl Reducer for MockAlwaysErrReducer {
        type Item = ();

        fn reduce(&mut self, _: Self::Item) -> Result<(), anyhow::Error> {
            self.num_reduce.fetch_add(1, Ordering::Relaxed);
            match self.failure {
                ReducerFailure::FailOnReduce => Err(anyhow!("Oops")),
                ReducerFailure::FailOnFlush => Ok(()),
            }
        }

        async fn flush(&mut self) -> Result<(), anyhow::Error> {
            self.num_flush.fetch_add(1, Ordering::Relaxed);
            match self.failure {
                ReducerFailure::FailOnReduce => Ok(()),
                ReducerFailure::FailOnFlush => Err(anyhow!("Oops")),
            }
        }

        fn reset(&mut self) {
            self.num_reset.fetch_add(1, Ordering::Relaxed);
        }

        fn is_full(&self) -> bool {
            false
        }

        fn get_reduce_config(&self) -> ReduceConfig {
            ReduceConfig {
                shutdown_behaviour: ReduceShutdownBehaviour::Flush,
                flush_interval: Duration::from_secs(1),
            }
        }
    }

    #[tokio::test]
    async fn test_commit() {
        let offsets = Arc::new(RwLock::new(Vec::new()));

        let commit_client = Arc::new(MockCommitClient {
            offsets: offsets.clone(),
        });
        let (sender, receiver) = mpsc::channel(1);
        let (rendezvou_sender, rendezvou_receiver) = oneshot::channel();

        let tpl = TopicPartitionList::from_topic_map(&HashMap::from([
            (("topic".to_string(), 0), Offset::Offset(0)),
            (("topic".to_string(), 1), Offset::Offset(0)),
        ]))
        .unwrap();

        assert!(sender.send(tpl.clone()).await.is_ok());

        tokio::spawn(commit(receiver, commit_client, rendezvou_sender));

        drop(sender);
        let _ = rendezvou_receiver.await;

        assert_eq!(offsets.read().unwrap().len(), 1);
        assert_eq!(offsets.read().unwrap()[0], tpl);
    }

    #[tokio::test]
    async fn test_reduce_err() {
        let reducer = MockReducer::new();
        let buffer = reducer.get_buffer();
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(1);
        let (commit_sender, mut commit_receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let msg = OwnedMessage::new(
            Some(vec![0, 1, 2, 3, 4, 5, 6, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );

        tokio::spawn(reduce_err(
            reducer,
            receiver,
            commit_sender,
            shutdown.clone(),
        ));

        assert!(sender.send(msg.clone()).await.is_ok());
        assert_eq!(
            commit_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(1)
            )]))
            .unwrap()
        );

        drop(sender);
        shutdown.cancel();

        assert!(buffer.read().unwrap().is_empty());
        assert_eq!(pipe.read().unwrap().len(), 1);
        assert_eq!(pipe.read().unwrap()[0].payload(), msg.payload());
    }

    #[tokio::test]
    async fn test_reduce() {
        let reducer = MockReducer::new();
        let buffer = reducer.get_buffer();
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(2);
        let (ok_sender, mut ok_receiver) = mpsc::channel(2);
        let (err_sender, err_receiver) = mpsc::channel(2);
        let shutdown = CancellationToken::new();

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 2, 4, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 3, 5, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((msg_0.clone(), 1)).await.is_ok());
        assert!(sender.send((msg_1.clone(), 2)).await.is_ok());

        sleep(Duration::from_secs(2)).await;

        assert!(buffer.read().unwrap().is_empty());
        assert_eq!(pipe.read().unwrap().as_slice(), &[1, 2]);
        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(2)
            )]))
            .unwrap()
        );
        assert!(err_receiver.is_empty());

        drop(sender);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_fail_on_reduce() {
        let reducer = MockAlwaysErrReducer::new(ReducerFailure::FailOnReduce);
        let num_reduce = reducer.get_num_reduce();
        let num_flush = reducer.get_num_flush();
        let num_reset = reducer.get_num_reset();

        let (sender, receiver) = mpsc::channel(1);
        let (ok_sender, ok_receiver) = mpsc::channel(1);
        let (err_sender, mut err_receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let msg = OwnedMessage::new(
            Some(vec![0, 1, 2, 3, 4, 5, 6, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((msg.clone(), ())).await.is_ok());
        assert_eq!(err_receiver.recv().await.unwrap().payload(), msg.payload());

        drop(sender);
        shutdown.cancel();

        assert!(ok_receiver.is_empty());
        assert_eq!(num_reduce.load(Ordering::Relaxed), 1);
        assert_eq!(num_flush.load(Ordering::Relaxed), 0);
        assert_eq!(num_reset.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_fail_on_flush() {
        let reducer = MockAlwaysErrReducer::new(ReducerFailure::FailOnFlush);
        let num_reduce = reducer.get_num_reduce();
        let num_flush = reducer.get_num_flush();
        let num_reset = reducer.get_num_reset();

        let (sender, receiver) = mpsc::channel(1);
        let (ok_sender, ok_receiver) = mpsc::channel(1);
        let (err_sender, mut err_receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 2, 4, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 3, 5, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((msg_0.clone(), ())).await.is_ok());
        assert!(sender.send((msg_1.clone(), ())).await.is_ok());

        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_0.payload()
        );
        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_1.payload()
        );
        assert!(ok_receiver.is_empty());

        assert_eq!(num_reduce.load(Ordering::Relaxed), 2);
        assert_eq!(num_flush.load(Ordering::Relaxed), 1);
        assert_eq!(num_reset.load(Ordering::Relaxed), 1);

        drop(sender);
        shutdown.cancel();
    }
}
