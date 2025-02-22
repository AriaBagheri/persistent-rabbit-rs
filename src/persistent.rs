use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;
use chrono::{DateTime, TimeDelta, Utc};
use colored::Colorize;
use crossbeam_queue::SegQueue;
use itertools::Itertools;
use lapin::{BasicProperties, Channel, Consumer};
use lapin::options::{BasicConsumeOptions, BasicPublishOptions};
use lapin::types::FieldTable;
use pis_aller::PersistentPisAller;
use serde::{Deserialize, Serialize};
use tokio::fs::ReadDir;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::configuration::RabbitConfiguration;
use crate::error::PersistentRabbitError;

/// # Overview
///
/// `PersistentRabbit` is an asynchronous, persistent RabbitMQ manager that handles
/// connection and channel lifecycle, message publishing, and consumer management.
/// It is designed to automatically recover from connection or channel failures,
/// queue publish requests when necessary, and manage dynamic consumer tasks.
///
/// # Key Features
///
/// - **Connection & Channel Management:**
///   - Establishes and maintains a RabbitMQ connection and channel.
///   - Automatically reconnects and recreates the channel if they become unavailable.
///
/// - **Message Publishing:**
///   - Immediately publishes messages if the channel is available.
///   - Queues publish requests using a thread-safe queue (`SegQueue`) for later delivery.
///   - Discards messages that exceed a configurable age (`clean_queue_after`).
///
/// - **Consumer Management:**
///   - Supports dynamic addition and removal of consumers along with their
///     asynchronous processing functions.
///   - Automatically spawns or aborts consumer tasks as needed.
///
/// - **Asynchronous Operations:**
///   - Uses Tokio's asynchronous primitives (`RwLock`, `Mutex`, and tasks)
///     to safely manage concurrent operations.
///   - Background monitoring and flushing tasks ensure resilient operation.
///
/// # Usage Flow
///
/// 1. **Initialization:**
///    - Create an instance with `PersistentRabbit::default(clean_queue_after)`.
///    - Set the RabbitMQ server address via `set_addr`, which initiates the connection,
///      creates a channel, and sets up channel listeners for consumers.
///
/// 2. **Monitoring:**
///    - Spawn the monitoring thread with `monitor_thread` to continuously check and
///      recover the channel if needed.
///    - Spawn the flush thread with `flush_thread` to process and publish queued messages.
///
/// 3. **Publishing Messages:**
///    - Call `publish` to send messages. If the channel is unavailable, messages are
///      queued for eventual delivery.
///
/// 4. **Consumer Handling:**
///    - Register consumers using `add_consumer`, providing an asynchronous callback to
///      process messages.
///    - Remove consumers using `remove_consumer` to stop their associated tasks.
///
/// # Detailed Methods
///
/// - `default(clean_queue_after: TimeDelta) -> PersistentRabbit`
///   Constructs a new instance with no active connection, channel, or consumers.
///   The `clean_queue_after` parameter defines the maximum duration a queued message
///   can wait before being discarded.
///
/// - `set_addr(&self, addr: String) -> Result<(), PersistentRabbitError>`
///   Sets the RabbitMQ address, initiates a connection and channel, and sets up channel
///   listeners for any registered consumers.
///
/// - `connection(&self) -> Result<RwLockReadGuard<Connection>, PersistentRabbitError>`
///   Retrieves the active connection, or attempts to establish a new one if the current
///   connection is lost or unavailable.
///
/// - `publish(...)`
///   Attempts to publish a message immediately. If the channel is unavailable or
///   publishing fails, the message is queued for later processing.
///
/// - `monitor_thread(&'static self) -> JoinHandle<()>`
///   Spawns a background task that periodically checks the channel's status and
///   recreates it if needed.
///
/// - `flush_thread(&'static self) -> JoinHandle<()>`
///   Spawns a background task that continuously processes the message queue,
///   attempting to re-publish queued messages when the channel is available.
///
/// - `add_consumer(...)` and `remove_consumer(&self, consumer_tag: &str)`
///   Allow dynamic registration and removal of consumers. Registered consumers
///   have an associated asynchronous callback that processes messages from RabbitMQ.
///
/// # Global Instance
///
/// - `RABBIT`:
///   A global static instance of `PersistentRabbit`, initialized with a message
///   expiration of 2 days (`TimeDelta::days(2)`). This instance is available throughout
///   the application for RabbitMQ operations.

pub struct PersistentRabbit {
    rabbit: RwLock<Option<RabbitConfiguration>>,

    queue: SegQueue<PublishRequest>,

    consumers: LazyLock<Mutex<Vec<ConsumerPayload>>>,
    consumer_threads: LazyLock<Mutex<HashMap<String, (Sender<()>, JoinHandle<()>)>>>,
    monitor_handle: Mutex<Option<JoinHandle<()>>>,
    flush_handle: Mutex<Option<JoinHandle<()>>>,

    shutdown_signal_channel: LazyLock<Sender<()>>,

    pis_aller: &'static PersistentPisAller,
    clean_queue_after: TimeDelta,
}

#[derive(Clone)]
pub struct ConsumerPayload {
    queue: String,
    consumer_tag: String,
    options: BasicConsumeOptions,
    arguments: FieldTable,

    function: fn(Consumer, Receiver<()>) -> JoinHandle<()>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PublishRequest {
    exchange: String,
    routing_key: String,
    options: BasicPublishOptions,
    payload: Vec<u8>,
    properties: BasicProperties,

    date_added: DateTime<Utc>,
}

impl PersistentRabbit {
    /// Constructs a new `PersistentRabbit` with no active connection, channel, or consumers.
    /// The `clean_queue_after` parameter specifies how long a queued message can wait before being discarded.
    pub const fn const_new(clean_queue_after: TimeDelta, pis_aller: &'static PersistentPisAller) -> PersistentRabbit {
        PersistentRabbit {
            rabbit: RwLock::const_new(None),

            queue: SegQueue::new(),

            consumers: LazyLock::new(|| Mutex::new(Vec::new())),
            consumer_threads: LazyLock::new(|| Mutex::new(HashMap::new())),
            monitor_handle: Mutex::const_new(None),
            flush_handle: Mutex::const_new(None),

            shutdown_signal_channel: LazyLock::new(|| Sender::new(1)),

            pis_aller,
            clean_queue_after,
        }
    }

    /// Initiates the background monitoring and queue flush tasks.
    ///
    /// This method spawns:
    /// - A monitoring thread to check the health of the connection pool.
    /// - A flush thread to prevent build up on the in-memory queue.
    pub async fn initiate(&'static self) -> Result<(), &'static str> {
        if let Err(e) = tokio::fs::create_dir_all("pis-aller/rabbit").await {
            println!(
                "{}",
                format!(
                    "RABBIT - INITIATE - Failed to initiate pis aller directories for rabbit | \
                    error = {e:?}"
                )
                    .red()
            );
            return Err("Failed to initiate pis aller directories for rabbit");
        }

        *self.monitor_handle.lock().await = Some(self.monitor_thread());
        *self.flush_handle.lock().await = Some(self.flush_thread());

        Ok(())
    }

    /// Sets the RabbitMQ server address, initiates the connection and channel,
    /// and sets up channel listeners for any registered consumers.
    pub async fn set_addr(&self, addr: String) -> Result<(), PersistentRabbitError> {
        if let Some(rabbit) = self.rabbit.read().await.as_ref() {
            if rabbit.address.as_str() == addr {
                println!(
                    "{}",
                    "RABBIT - SET_ADDRESS - Address unchanged. \
                    Skipping client re-establishment."
                        .green()
                        .dimmed()
                );
                return Ok(());
            }
        }
        let rabbit = RabbitConfiguration::new(addr).await?;
        self.setup_channel_listeners(&rabbit.channel).await;
        *self.rabbit.write().await = Some(rabbit);

        Ok(())
    }

    pub async fn consume_on_function(
        &self,
        channel: &Channel,
        queue: &str,
        consumer_tag: &str,
        options: BasicConsumeOptions,
        arguments: FieldTable,
        function: fn(consumer: Consumer, Receiver<()>) -> JoinHandle<()>,
    ) {
        match channel
            .basic_consume(queue, consumer_tag, options, arguments)
            .await
        {
            Ok(consumer) => {
                let mut consumer_threads_lock = self.consumer_threads.lock().await;
                Self::kill_consumer_thread(
                    &consumer_tag,
                    consumer_threads_lock.get_mut(consumer_tag),
                )
                    .await;
                let shutdown_signal = Sender::new(1);
                let future = function(consumer, shutdown_signal.subscribe());
                consumer_threads_lock.insert(consumer_tag.to_string(), (shutdown_signal, future));
            }
            Err(err) => {
                println!(
                    "{} {}",
                    "RABBIT - CONSUME_ON_FUNCTION - Failed to initiate consumer".red(),
                    format!(
                        "| queue = {}, consumer_tag = {}, error = {}",
                        queue,
                        consumer_tag,
                        err.to_string()
                    )
                        .red()
                        .dimmed()
                );
            }
        }
    }

    pub async fn kill_consumer_thread(
        consumer_tag: &str,
        record: Option<&mut (Sender<()>, JoinHandle<()>)>,
    ) {
        if let Some((shutdown_signal, old_join_handle)) = record {
            println!(
                "{} {}",
                "RABBIT - KILL_CONSUMER_THREAD - Sending termination signal to the consumer thread"
                    .cyan(),
                format!("| consumer_tag = {}", consumer_tag).cyan().dimmed()
            );
            let _ = shutdown_signal.send(());
            if let Err(_) =
                tokio::time::timeout(Duration::from_secs(10), &mut *old_join_handle).await
            {
                println!(
                    "{} {}",
                    "RABBIT - KILL_CONSUMER_THREAD - \
                    Consumer failed to terminate on time. \
                    Was left with no option but to abort it."
                        .yellow(),
                    format!("| consumer_tag = {}", consumer_tag)
                        .yellow()
                        .dimmed()
                );
                old_join_handle.abort();
            } else {
                println!(
                    "{} {}",
                    "RABBIT - KILL_CONSUMER_THREAD - Consumer thread terminated gracefully.".cyan(),
                    format!("| consumer_tag = {}", consumer_tag).cyan().dimmed()
                );
            }
        }
    }
    /// Sets up channel listeners for all registered consumers.
    async fn setup_channel_listeners(&self, channel: &Channel) {
        for consumer in self.consumers.lock().await.iter() {
            self.consume_on_function(
                channel,
                consumer.queue.as_str(),
                consumer.consumer_tag.as_str(),
                consumer.options.clone(),
                consumer.arguments.clone(),
                consumer.function,
            )
                .await;
        }
    }

    /// Spawns a background task that monitors the channel status and re-establishes it if lost.
    pub fn monitor_thread(&'static self) -> JoinHandle<()> {
        let mut shutdown = self.shutdown_signal_channel.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        break;
                    },
                    _ = interval.tick() => {
                        let rabbit_read = self.rabbit.read().await;
                        if let Some(rabbit) = rabbit_read.as_ref() {
                            if !rabbit.is_connected() {
                                let old_address = rabbit.address.clone();
                                drop(rabbit_read);
                                if let Ok(rabbit) = RabbitConfiguration::new(old_address).await {
                                    self.setup_channel_listeners(&rabbit.channel).await;
                                    *self.rabbit.write().await = Some(rabbit);
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    /// Attempts to publish a message immediately. If publishing fails, the request is queued for later delivery.
    pub async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        options: BasicPublishOptions,
        payload: &[u8],
        properties: BasicProperties,
    ) {
        if let Some(rabbit) = self.rabbit.read().await.as_ref() {
            if rabbit.is_connected() {
                let publish_result = rabbit
                    .channel
                    .basic_publish(exchange, routing_key, options, payload, properties.clone())
                    .await;

                if let Ok(confirm) = publish_result {
                    // Wait for server ACK if you want guaranteed confirmation
                    if confirm.await.is_ok() {
                        return; // success
                    }
                }
            }
        }

        // Fallthrough: queue the request for later
        self.queue.push(PublishRequest {
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            options,
            payload: payload.to_vec(),
            properties,

            date_added: Utc::now(),
        });

        // We succeed from the caller's perspective because we store for eventual publish.
        return;
    }

    pub async fn balance_queue(&self, force_dump: bool) {
        if self.queue.len() > 1000 || force_dump {
            let mut failed_items = Vec::new();
            while let Some(item) = self.queue.pop() {
                match serde_json::to_string(&item) {
                    Ok(r) => {
                        failed_items.push((item, r));
                    }
                    Err(e) => {
                        println!(
                            "{} {}",
                            "RABBIT - BALANCED_QUEUE - Failed to serialize queue item. \n"
                                .red()
                                .bold(),
                            e.to_string().bold().red()
                        );
                        self.queue.push(item);
                    }
                }
            }
            let chunks: Vec<Vec<(PublishRequest, String)>> = failed_items
                .into_iter()
                .chunks(1000)
                .into_iter()
                .map(|chunk| chunk.collect())
                .collect();
            for chunk in chunks {
                let uuid = Uuid::new_v4();
                let (chunk_objs, chunk_strings): (Vec<PublishRequest>, Vec<String>) =
                    chunk.into_iter().unzip();
                let chunk_str = chunk_strings.join("\n");
                if let Err(e) = tokio::fs::write(
                    format!("pis-aller/rabbit/{}.jsonl", uuid),
                    chunk_str.as_bytes(),
                )
                    .await
                {
                    println!(
                        "{} {}",
                        "RABBIT - BALANCED_QUEUE - Failed to write to file. Re-Queueing\n"
                            .red()
                            .bold(),
                        e.to_string().bold().red()
                    );
                    for item in chunk_objs.into_iter() {
                        self.queue.push(item);
                    }
                }
            }
        } else if self.queue.is_empty() {
            let mut entries = match tokio::fs::read_dir("pis-aller/rabbit/").await {
                Ok(e) => e,
                Err(e) => {
                    println!(
                        "{} {}",
                        "RABBIT - BALANCED_QUEUE - Failed to read entries at rabbit directory\n"
                            .red()
                            .bold(),
                        e.to_string().bold().red()
                    );
                    return;
                }
            };

            // Waits for queue to get empty. Aka connection to re-establish
            let mut error_counter = 0;
            while self.queue.len() < 1000 {
                match self.read_next_file(&mut entries).await {
                    Ok(None) => {
                        break;
                    }
                    Ok(Some(v)) => {
                        for item in v {
                            self.queue.push(item);
                        }
                    }
                    Err(e) => {
                        println!(
                            "{} {}",
                            "RABBIT - BALANCED_QUEUE - Failed to read file at rabbit directory\n"
                                .red()
                                .bold(),
                            e.to_string().bold().red()
                        );
                        error_counter += 1;
                        if error_counter > 10 {
                            break;
                        } else {
                            continue;
                        }
                    }
                }
            }
        }
    }

    async fn read_next_file(
        &self,
        entries: &mut ReadDir,
    ) -> tokio::io::Result<Option<Vec<PublishRequest>>> {
        let mut records = Vec::with_capacity(1000);
        if let Some(entry) = entries.next_entry().await? {
            let x = entry.file_type().await?;
            if x.is_file() {
                for line in tokio::fs::read_to_string(entry.path()).await?.split("\n") {
                    match serde_json::from_str(line) {
                        Ok(record) => {
                            records.push(record);
                        }
                        Err(e) => {
                            self.pis_aller.process("rabbit-publish-requests", line, e);
                        }
                    }
                }
            }
        }
        if !records.is_empty() {
            return Ok(Some(records));
        }
        Ok(None)
    }

    /// Spawns a background task that flushes the publish-queue, re-attempting to publish queued messages.
    pub fn flush_thread(&'static self) -> JoinHandle<()> {
        let mut shutdown = self.shutdown_signal_channel.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        break;
                    },
                    _ = interval.tick() => {
                        self.balance_queue(false).await;
                        let item = if let Some(item) = self.queue.pop() {
                            item
                        } else {
                            // if queue is empty wait till it has something!
                            continue;
                        };
                        if item.date_added.signed_duration_since(Utc::now()) > self.clean_queue_after {
                            println!(
                                "A rabbitMQ item never managed to publish after {}, queue = {:?}",
                                self.clean_queue_after, item
                            );
                            // We don't add it after this time delta has passed.
                            // We log it for further investigation!
                            continue;
                        }
                        if let Some(rabbit) = self.rabbit.read().await.as_ref() {
                            if rabbit.is_connected() {
                                let result = rabbit.channel
                                    .basic_publish(
                                        &item.exchange,
                                        &item.routing_key,
                                        item.options,
                                        &item.payload,
                                        item.properties.clone(),
                                    )
                                    .await;
                                match result {
                                    Ok(confirm) => {
                                        if confirm.await.is_err() {
                                            // If the broker NACKs or closes, re-queue.
                                            self.queue.push(item);
                                            continue;
                                        }
                                    }
                                    Err(_) => {
                                        // Publish call itself failed => re‐queue
                                        self.queue.push(item);
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            self.balance_queue(true).await;
        })
    }

    /// Registers a new consumer for a given queue with an asynchronous processing function.
    pub async fn add_consumer(
        &self,
        queue: &str,
        consumer_tag: &str,
        options: BasicConsumeOptions,
        arguments: FieldTable,
        consumer: fn(consumer: Consumer, Receiver<()>) -> JoinHandle<()>,
    ) {
        let mut consumers = self.consumers.lock().await;
        consumers.push(ConsumerPayload {
            queue: queue.to_string(),
            consumer_tag: consumer_tag.to_string(),
            options,
            arguments: arguments.clone(),
            function: consumer,
        });
        drop(consumers);
        if let Some(rabbit) = self.rabbit.read().await.as_ref() {
            self.consume_on_function(
                &rabbit.channel,
                queue,
                consumer_tag,
                options,
                arguments.clone(),
                consumer,
            )
                .await;
        } // If no channel, then adding to consumers is sufficient!
    }

    /// Removes a consumer with the specified tag and aborts its associated task.
    pub async fn remove_consumer(&self, consumer_tag: &str) {
        {
            let mut consumers = self.consumers.lock().await;
            let index_at = consumers
                .iter()
                .position(|value| value.consumer_tag == consumer_tag);
            if let Some(index) = index_at {
                consumers.swap_remove(index);
            }
        }
        {
            let mut consumer_threads = self.consumer_threads.lock().await;
            Self::kill_consumer_thread(consumer_tag, consumer_threads.get_mut(consumer_tag)).await;
        }
    }

    pub async fn shutdown(&self) {
        print!("\n");

        let mut consumer_threads = self.consumer_threads.lock().await;
        for (tag, record) in consumer_threads.iter_mut() {
            Self::kill_consumer_thread(tag, Some(record)).await;
        }
        drop(consumer_threads);

        let _ = self.shutdown_signal_channel.send(());
        println!(
            "{}",
            "RABBIT - SHUTDOWN - Shutdown signal was propagated to internal threads!".cyan()
        );
        // To prevent excessive wait time, 5 sec timeout set.
        if let Err(_) = tokio::time::timeout(Duration::from_secs(5), async {
            if let Some(monitor_handle) = self.monitor_handle.lock().await.as_mut() {
                let _ = monitor_handle.await;
                println!(
                    "{}",
                    "RABBIT - SHUTDOWN - Monitoring thread terminated gracefully!".cyan()
                );
            }
            if let Some(flush_handle) = self.flush_handle.lock().await.as_mut() {
                let _ = flush_handle.await;
                println!(
                    "{}",
                    "RABBIT - SHUTDOWN - Flush thread terminated gracefully!".cyan()
                );
            }
        })
            .await
        {
            self.monitor_handle.lock().await.as_mut().map(|f| f.abort());
            self.flush_handle.lock().await.as_mut().map(|f| f.abort());
            println!(
                "{}",
                "RABBIT - SHUTDOWN - Some tasks failed to terminate on time!".yellow()
            );
        }

        if let Some(rabbit) = self.rabbit.write().await.as_mut() {
            let _ = rabbit.channel.close(200, "Goodbye").await;
            println!(
                "{}",
                "RABBIT - SHUTDOWN - Channel closed gracefully!".cyan()
            );
            let _ = rabbit.connection.close(200, "Goodbye").await;
            println!(
                "{}",
                "RABBIT - SHUTDOWN - Connection closed gracefully!".cyan()
            );
        }

        println!("{}", "RABBIT - SHUTDOWN - Goodbye!".cyan())
    }
}
