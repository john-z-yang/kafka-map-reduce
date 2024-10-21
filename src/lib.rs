use anyhow::{anyhow, Error};
use futures::{
    future::{self},
    pin_mut, Stream, StreamExt,
};
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
use tokio_util::{either::Either, sync::CancellationToken};
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

            const CHANNEL_BUFF_SIZE: usize = 64;
            let (reduce_sender, reduce_receiver) = tokio::sync::mpsc::channel(CHANNEL_BUFF_SIZE);
            let (commit_sender, commit_receiver) = tokio::sync::mpsc::channel(CHANNEL_BUFF_SIZE);
            let (err_sender, err_receiver) = tokio::sync::mpsc::channel(CHANNEL_BUFF_SIZE);

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
    fn detach(&self) -> Result<OwnedMessage, Error>;
}

impl KafkaMessage for Result<BorrowedMessage<'_>, KafkaError> {
    fn detach(&self) -> Result<OwnedMessage, Error> {
        match self {
            Ok(borrowed_msg) => Ok(borrowed_msg.detach()),
            Err(err) => Err(anyhow!(
                "Cannot detach message, got error from kafka: {:?}",
                err
            )),
        }
    }
}

trait MessageQueue {
    fn stream(&self) -> impl Stream<Item = impl KafkaMessage>;
}

impl MessageQueue for StreamPartitionQueue<KafkaContext> {
    fn stream(&self) -> impl Stream<Item = impl KafkaMessage> {
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
                let Some(msg) = val else {
                    break;
                };
                let msg = Arc::new(msg.detach()?);
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
                            "Failed to map message at \
                            (topic: {}, partition: {}, offset: {}), reason: {}",
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
    pub flush_interval: Option<Duration>,
}

#[derive(Debug, Clone)]
pub enum ReduceShutdownBehaviour {
    Flush,
    Drop,
}

pub trait Reducer {
    type Item;

    fn reduce(
        &mut self,
        t: Self::Item,
    ) -> impl std::future::Future<Output = Result<(), anyhow::Error>> + Send;
    fn flush(&mut self) -> impl std::future::Future<Output = Result<(), anyhow::Error>> + Send;
    fn reset(&mut self);
    fn is_full(&self) -> bool;
    fn get_reduce_config(&self) -> ReduceConfig;
}

async fn handle_reducer_failure<T>(
    reducer: &mut impl Reducer<Item = T>,
    inflight_msgs: &mut Vec<OwnedMessage>,
    highwater_mark: &mut HighwaterMark,
    err: &mpsc::Sender<OwnedMessage>,
) {
    for msg in take(inflight_msgs).into_iter() {
        err.send(msg)
            .await
            .expect("reduce_err should always be available");
    }
    highwater_mark.clear();
    reducer.reset();
}

#[instrument(skip(reducer, inflight_msgs, highwater_mark, ok, err))]
async fn shutdown_reducer<T>(
    shutdown_behaviour: ReduceShutdownBehaviour,
    mut reducer: impl Reducer<Item = T>,
    inflight_msgs: &mut Vec<OwnedMessage>,
    highwater_mark: &mut HighwaterMark,
    ok: &mpsc::Sender<TopicPartitionList>,
    err: &mpsc::Sender<OwnedMessage>,
) -> Result<(), Error> {
    match shutdown_behaviour {
        ReduceShutdownBehaviour::Flush => {
            debug!("Received shutdown signal, flushing reducer...");
            flush_reducer(&mut reducer, inflight_msgs, highwater_mark, ok, err).await?;
        }
        ReduceShutdownBehaviour::Drop => {
            debug!("Received shutdown signal, dropping reducer...");
            drop(reducer);
        }
    };
    Ok(())
}

#[instrument(skip(reducer, inflight_msgs, highwater_mark, ok, err))]
async fn flush_reducer<T>(
    reducer: &mut impl Reducer<Item = T>,
    inflight_msgs: &mut Vec<OwnedMessage>,
    highwater_mark: &mut HighwaterMark,
    ok: &mpsc::Sender<TopicPartitionList>,
    err: &mpsc::Sender<OwnedMessage>,
) -> Result<(), Error> {
    match reducer.flush().await {
        Err(e) => {
            error!("Failed to flush reducer, reason: {}", e);
            handle_reducer_failure(reducer, inflight_msgs, highwater_mark, err).await;
        }
        Ok(()) => {
            inflight_msgs.clear();
            if !highwater_mark.is_empty() {
                ok.send(take(highwater_mark).into())
                    .await
                    .map_err(|err| anyhow!("{}", err))?;
            }
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
    let mut highwater_mark = HighwaterMark::new();
    let mut flush_timer = config.flush_interval.map(time::interval);
    let mut inflight_msgs = Vec::new();

    loop {
        select! {
            biased;

            _ = shutdown.cancelled() => {
                shutdown_reducer(
                    config.shutdown_behaviour,
                    reducer,
                    &mut inflight_msgs,
                    &mut highwater_mark,
                    &ok,
                    &err
                ).await?;
                break;
            }

            _ = if let Some(ref mut flush_timer) = flush_timer {
                Either::Left(flush_timer.tick())
            } else {
                Either::Right(future::pending::<_>())
            } => {
                flush_reducer(
                    &mut reducer,
                    &mut inflight_msgs,
                    &mut highwater_mark,
                    &ok,
                    &err
                ).await?;
            }

            val = receiver.recv(), if !reducer.is_full() => {
                let Some((msg, value)) = val else {
                    debug!("Received end of stream, flushing reducer...");
                    flush_reducer(
                        &mut reducer,
                        &mut inflight_msgs,
                        &mut highwater_mark,
                        &ok,
                        &err
                    ).await?;
                    break;
                };

                highwater_mark.track(&msg);
                inflight_msgs.push(msg);

                if let Err(e) = reducer.reduce(value).await {
                    error!(
                        "Failed to reduce message at \
                        (topic: {}, partition: {}, offset: {}), reason: {}",
                        inflight_msgs.last().unwrap().topic(),
                        inflight_msgs.last().unwrap().partition(),
                        inflight_msgs.last().unwrap().offset(),
                        e
                    );
                    handle_reducer_failure(
                        &mut reducer,
                        &mut inflight_msgs,
                        &mut highwater_mark,
                        &err
                    ).await;
                }

                if flush_timer.is_none() {
                    inflight_msgs.clear();
                    if !highwater_mark.is_empty() {
                        ok.send(take(&mut highwater_mark).into())
                            .await
                            .map_err(|err| anyhow!("{}", err))?;
                    }
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
    let mut highwater_mark = HighwaterMark::new();
    let mut flush_timer = config.flush_interval.map(time::interval);

    loop {
        select! {
            biased;

            _ = shutdown.cancelled() => {
                match config.shutdown_behaviour {
                    ReduceShutdownBehaviour::Flush => {
                        debug!("Received shutdown signal, flushing reducer...");
                        reducer
                            .flush()
                            .await
                            .expect("error reducer flush should always be successful");
                    },
                    ReduceShutdownBehaviour::Drop => {
                        debug!("Received shutdown signal, dropping reducer...");
                        drop(reducer);
                    },
                }
                break;
            }

            _ = if let Some(ref mut flush_timer) = flush_timer {
                Either::Left(flush_timer.tick())
            } else {
                Either::Right(future::pending::<_>())
            } => {
                reducer
                    .flush()
                    .await
                    .expect("error reducer flush should always be successful");

                if !highwater_mark.is_empty() {
                    ok.send(take(&mut highwater_mark).into())
                        .await
                        .map_err(|err| anyhow!("{}", err))?;
                }
            }

            val = receiver.recv(), if !reducer.is_full() => {
                let Some(msg) = val else {
                    debug!("Received end of stream, flushing reducer...");
                    reducer
                        .flush()
                        .await
                        .expect("error reducer flush should always be successful");
                    break;
                };
                highwater_mark.track(&msg);

                reducer
                    .reduce(msg)
                    .await
                    .expect("error reducer reduce should always be successful");

                if flush_timer.is_none() && !highwater_mark.is_empty() {
                    ok.send(take(&mut highwater_mark).into())
                        .await
                        .map_err(|err| anyhow!("{}", err))?;
                }
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
        mem::take,
        sync::{Arc, RwLock},
        time::Duration,
    };

    use anyhow::{anyhow, Error};
    use futures::Stream;
    use rdkafka::{
        error::{KafkaError, KafkaResult},
        message::OwnedMessage,
        Message, Offset, Timestamp, TopicPartitionList,
    };
    use tokio::{
        sync::{broadcast, mpsc, oneshot},
        time::sleep,
    };
    use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
    use tokio_util::sync::CancellationToken;

    use crate::{
        commit, map, reduce, reduce_err, CommitClient, KafkaMessage, MessageQueue, ReduceConfig,
        ReduceShutdownBehaviour, Reducer,
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

    struct StreamingReducer<T> {
        pipe: Arc<RwLock<Vec<T>>>,
        error_on_idx: Option<usize>,
    }

    impl<T> StreamingReducer<T> {
        fn new(error_on_idx: Option<usize>) -> Self {
            Self {
                pipe: Arc::new(RwLock::new(Vec::new())),
                error_on_idx,
            }
        }

        fn get_pipe(&self) -> Arc<RwLock<Vec<T>>> {
            self.pipe.clone()
        }
    }

    impl<T> Reducer for StreamingReducer<T>
    where
        T: Send + Sync + Clone,
    {
        type Item = T;

        async fn reduce(&mut self, t: Self::Item) -> Result<(), anyhow::Error> {
            if let Some(idx) = self.error_on_idx {
                if idx == self.pipe.read().unwrap().len() {
                    self.error_on_idx.take();
                    return Err(anyhow!("err"));
                }
            }
            self.pipe.write().unwrap().push(t);
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
                shutdown_behaviour: ReduceShutdownBehaviour::Drop,
                flush_interval: None,
            }
        }
    }

    struct BatchingReducer<T> {
        buffer: Arc<RwLock<Vec<T>>>,
        pipe: Arc<RwLock<Vec<T>>>,
        error_on_nth_reduce: Option<usize>,
        error_on_nth_flush: Option<usize>,
    }

    impl<T> BatchingReducer<T> {
        fn new(error_on_reduce: Option<usize>, error_on_flush: Option<usize>) -> Self {
            Self {
                buffer: Arc::new(RwLock::new(Vec::new())),
                pipe: Arc::new(RwLock::new(Vec::new())),
                error_on_nth_reduce: error_on_reduce,
                error_on_nth_flush: error_on_flush,
            }
        }

        fn get_buffer(&self) -> Arc<RwLock<Vec<T>>> {
            self.buffer.clone()
        }

        fn get_pipe(&self) -> Arc<RwLock<Vec<T>>> {
            self.pipe.clone()
        }
    }

    impl<T> Reducer for BatchingReducer<T>
    where
        T: Send + Sync + Clone,
    {
        type Item = T;

        async fn reduce(&mut self, t: Self::Item) -> Result<(), anyhow::Error> {
            if let Some(idx) = self.error_on_nth_reduce {
                if idx == 0 {
                    self.error_on_nth_reduce.take();
                    return Err(anyhow!("err"));
                } else {
                    self.error_on_nth_reduce = Some(idx - 1);
                }
            }
            self.buffer.write().unwrap().push(t);
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), anyhow::Error> {
            if let Some(idx) = self.error_on_nth_flush {
                if idx == 0 {
                    self.error_on_nth_flush.take();
                    return Err(anyhow!("err"));
                } else {
                    self.error_on_nth_flush = Some(idx - 1);
                }
            }
            self.pipe
                .write()
                .unwrap()
                .extend(take(&mut self.buffer.write().unwrap() as &mut Vec<T>).into_iter());
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
                flush_interval: Some(Duration::from_secs(1)),
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
    async fn test_reduce_err_without_flush_interval() {
        let reducer = StreamingReducer::new(None);
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
        assert_eq!(
            pipe.read().unwrap().last().unwrap().payload().unwrap(),
            &[0, 1, 2, 3, 4, 5, 6, 7]
        );

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(commit_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_reduce_without_flush_interval() {
        let reducer = StreamingReducer::new(None);
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

        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(1)
            )]))
            .unwrap()
        );
        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(2)
            )]))
            .unwrap()
        );
        assert_eq!(pipe.read().unwrap().as_slice(), &[1, 2]);
        assert!(err_receiver.is_empty());

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_fail_on_reduce_without_flush_interval() {
        let reducer = StreamingReducer::new(Some(1));
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(2);
        let (ok_sender, mut ok_receiver) = mpsc::channel(2);
        let (err_sender, mut err_receiver) = mpsc::channel(2);
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
        let msg_2 = OwnedMessage::new(
            Some(vec![0, 0, 0, 0]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            2,
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
        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(1)
            )]))
            .unwrap()
        );
        assert_eq!(pipe.read().unwrap().as_slice(), &[1]);

        assert!(sender.send((msg_1.clone(), 2)).await.is_ok());
        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_1.payload()
        );
        assert_eq!(pipe.read().unwrap().as_slice(), &[1]);

        assert!(sender.send((msg_2.clone(), 3)).await.is_ok());
        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(3)
            )]))
            .unwrap()
        );
        assert_eq!(pipe.read().unwrap().as_slice(), &[1, 3]);

        assert!(ok_receiver.is_empty());
        assert!(err_receiver.is_empty());

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_reduce_err_with_flush_interval() {
        let reducer = BatchingReducer::new(None, None);
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
        assert_eq!(pipe.read().unwrap()[0].payload(), msg.payload());
        assert!(buffer.read().unwrap().is_empty());

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(commit_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_reduce_with_flush_interval() {
        let reducer = BatchingReducer::new(None, None);
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

        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(2)
            )]))
            .unwrap()
        );
        assert!(buffer.read().unwrap().is_empty());
        assert_eq!(pipe.read().unwrap().as_slice(), &[1, 2]);
        assert!(err_receiver.is_empty());

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_fail_on_reduce_with_flush_interval() {
        let reducer = BatchingReducer::new(Some(1), None);
        let buffer = reducer.get_buffer();
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(3);
        let (ok_sender, mut ok_receiver) = mpsc::channel(3);
        let (err_sender, mut err_receiver) = mpsc::channel(3);
        let shutdown = CancellationToken::new();

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 3, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 4, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );
        let msg_2 = OwnedMessage::new(
            Some(vec![2, 5, 8]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            2,
            None,
        );

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((msg_0.clone(), 0)).await.is_ok());
        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(1)
            )]))
            .unwrap()
        );
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0]);

        assert!(sender.send((msg_1.clone(), 1)).await.is_ok());
        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_1.payload()
        );
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0] as &[i32]);

        assert!(sender.send((msg_2.clone(), 2)).await.is_ok());
        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(3)
            )]))
            .unwrap()
        );
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0, 2] as &[i32]);

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_empty());
        assert!(err_receiver.is_empty());
    }

    #[tokio::test]
    async fn test_fail_on_flush() {
        let reducer = BatchingReducer::new(None, Some(1));
        let buffer = reducer.get_buffer();
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(1);
        let (ok_sender, mut ok_receiver) = mpsc::channel(1);
        let (err_sender, mut err_receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 3, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 4, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );
        let msg_2 = OwnedMessage::new(
            Some(vec![2, 5, 8]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            2,
            None,
        );
        let msg_3 = OwnedMessage::new(
            Some(vec![0, 0, 0]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            3,
            None,
        );

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((msg_0.clone(), 0)).await.is_ok());

        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(1)
            )]))
            .unwrap()
        );
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0]);

        assert!(sender.send((msg_1.clone(), 1)).await.is_ok());
        assert!(sender.send((msg_2.clone(), 2)).await.is_ok());
        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_1.payload()
        );
        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_2.payload()
        );
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0]);

        assert!(sender.send((msg_3, 3)).await.is_ok());
        assert_eq!(
            ok_receiver.recv().await.unwrap(),
            TopicPartitionList::from_topic_map(&HashMap::from([(
                ("topic".to_string(), 0),
                Offset::Offset(4)
            )]))
            .unwrap()
        );
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0, 3]);

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_empty());
        assert!(err_receiver.is_empty());
    }

    #[derive(Clone)]
    struct MockMessage {
        payload: Vec<u8>,
        topic: String,
        partition: i32,
        offset: i64,
    }

    impl KafkaMessage for Result<Result<MockMessage, KafkaError>, BroadcastStreamRecvError> {
        fn detach(&self) -> Result<OwnedMessage, Error> {
            let clone = self.clone().unwrap().unwrap();
            Ok(OwnedMessage::new(
                Some(clone.payload),
                None,
                clone.topic,
                Timestamp::now(),
                clone.partition,
                clone.offset,
                None,
            ))
        }
    }

    impl MessageQueue for broadcast::Receiver<Result<MockMessage, KafkaError>> {
        fn stream(&self) -> impl Stream<Item = impl KafkaMessage> {
            BroadcastStream::new(self.resubscribe())
        }
    }

    #[tokio::test]
    async fn test_map() {
        let (sender, receiver) = broadcast::channel(1);
        let (ok_sender, mut ok_receiver) = mpsc::channel(1);
        let (err_sender, err_receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        tokio::spawn(map(
            receiver,
            |msg| async move { Ok(msg.payload().unwrap()[0] * 2) },
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));
        sleep(Duration::from_secs(1)).await;

        let msg_0 = MockMessage {
            payload: vec![0],
            topic: "topic".to_string(),
            partition: 0,
            offset: 0,
        };
        let msg_1 = MockMessage {
            payload: vec![1],
            topic: "topic".to_string(),
            partition: 0,
            offset: 1,
        };
        assert!(sender.send(Ok(msg_0.clone())).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(res.0.payload(), Some(msg_0.payload.clone()).as_deref());
        assert_eq!(res.1, msg_0.payload[0] * 2);

        assert!(sender.send(Ok(msg_1.clone())).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(res.0.payload(), Some(msg_1.payload.clone()).as_deref());
        assert_eq!(res.1, msg_1.payload[0] * 2);

        shutdown.cancel();
        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_fail_on_map() {
        let (sender, receiver) = broadcast::channel(1);
        let (ok_sender, mut ok_receiver) = mpsc::channel(1);
        let (err_sender, mut err_receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        tokio::spawn(map(
            receiver,
            |msg| async move {
                if msg.payload().unwrap()[0] == 1 {
                    Err(anyhow!("Oh no"))
                } else {
                    Ok(msg.payload().unwrap()[0] * 2)
                }
            },
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));
        sleep(Duration::from_secs(1)).await;

        let msg_0 = MockMessage {
            payload: vec![0],
            topic: "topic".to_string(),
            partition: 0,
            offset: 0,
        };
        let msg_1 = MockMessage {
            payload: vec![1],
            topic: "topic".to_string(),
            partition: 0,
            offset: 1,
        };
        let msg_2 = MockMessage {
            payload: vec![2],
            topic: "topic".to_string(),
            partition: 0,
            offset: 2,
        };

        assert!(sender.send(Ok(msg_0.clone())).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(res.0.payload(), Some(msg_0.payload).as_deref());
        assert_eq!(res.1, 0);

        assert!(sender.send(Ok(msg_1.clone())).is_ok());
        assert!(ok_receiver.is_empty());
        let res = err_receiver.recv().await.unwrap();
        assert_eq!(res.payload(), Some(msg_1.payload).as_deref());

        assert!(sender.send(Ok(msg_2.clone())).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(res.0.payload(), Some(msg_2.payload).as_deref());
        assert_eq!(res.1, 4);

        shutdown.cancel();
        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }
}
