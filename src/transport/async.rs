//! Asynchronous transport implementation

mod io;
pub(crate) use io::{AsyncStream, AsyncTcpSocket};

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, error, info, warn};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, Notify, RwLock};
use tokio::task;
use tokio::time::Duration;
use tokio_stream::wrappers::BroadcastStream;

use crate::connection::r#async::AsyncConnection;
use crate::messages::{shared_channel_configuration, IncomingMessages, Notice, OutgoingMessages, ResponseMessage};
use crate::Error;

use super::common::{log_orphan, ErrorRouteOwners, IdOrigin, IdOwnerToken, OrderChannelToken, OwnedIdClaim};
use super::routing::{
    determine_routing, is_warning_error, order_routing_strategy, terminal_order_id, DecodedError, OrderRoutingStrategy, RoutingDecision,
    UNSPECIFIED_REQUEST_ID,
};
use super::RoutedItem;

/// Default capacity for broadcast channels
/// This should be large enough to handle bursts of messages without lagging
pub(crate) const BROADCAST_CHANNEL_CAPACITY: usize = 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Cleanup signal for removing channels when subscriptions are dropped
#[derive(Debug, Clone)]
pub(crate) enum CleanupSignal {
    Request(IdOwnerToken),
    Order(OrderChannelToken),
    Shared(OutgoingMessages),
    OrderUpdateStream,
    #[cfg(test)]
    Barrier(Arc<Notify>),
}

/// Asynchronous message bus trait
#[async_trait]
pub trait AsyncMessageBus: Send + Sync {
    async fn send_request(&self, request_id: i32, message: Vec<u8>) -> Result<AsyncInternalSubscription, Error>;

    async fn send_order_request(&self, order_id: i32, message: Vec<u8>) -> Result<AsyncInternalSubscription, Error>;

    async fn send_order_message(&self, order_id: i32, message: Vec<u8>) -> Result<(), Error>;

    async fn send_shared_request(&self, message_type: OutgoingMessages, message: Vec<u8>) -> Result<AsyncInternalSubscription, Error>;

    async fn send_message(&self, message: Vec<u8>) -> Result<(), Error>;

    #[allow(dead_code)]
    async fn cancel_subscription(&self, request_id: i32, message: Vec<u8>) -> Result<(), Error>;
    #[allow(dead_code)]
    async fn cancel_order_subscription(&self, order_id: i32, message: Vec<u8>) -> Result<(), Error>;

    async fn create_order_update_subscription(&self) -> Result<AsyncInternalSubscription, Error>;

    fn notice_subscribe(&self) -> crate::subscriptions::notice_stream::async_impl::NoticeStream;

    async fn ensure_shutdown(&self);

    fn request_shutdown_sync(&self);

    fn is_connected(&self) -> bool;
}

/// Internal subscription for async implementation.
///
/// Holds a `BroadcastStream<RoutedItem>` for poll-based consumption plus a
/// `template_receiver` kept solely so `Clone` can `resubscribe()` to produce an
/// independent stream. We cannot store the `Sender` instead — that would keep
/// the channel alive past the external sender's drop, breaking the
/// "channel closes when senders drop" termination contract.
pub struct AsyncInternalSubscription {
    /// Held only for `Clone` via `resubscribe()`. Never polled directly.
    template_receiver: broadcast::Receiver<RoutedItem>,
    pub(crate) stream: BroadcastStream<RoutedItem>,
    cleanup_sender: Option<mpsc::UnboundedSender<CleanupSignal>>,
    cleanup_signal: Option<CleanupSignal>,
    cleanup_sent: bool,
}

impl Clone for AsyncInternalSubscription {
    fn clone(&self) -> Self {
        // For clones, both template and stream start at the current tail of
        // the broadcast channel — clones see future messages, not the
        // original's history.
        let new_template = self.template_receiver.resubscribe();
        let new_polling = self.template_receiver.resubscribe();
        Self {
            template_receiver: new_template,
            stream: BroadcastStream::new(new_polling),
            cleanup_sender: self.cleanup_sender.clone(),
            cleanup_signal: self.cleanup_signal.clone(),
            cleanup_sent: false, // Each clone should handle its own cleanup
        }
    }
}

impl AsyncInternalSubscription {
    /// Construct an internal subscription wrapping a broadcast receiver.
    ///
    /// **Receiver positioning matters.** The receiver you pass is what feeds
    /// the stream — its position in the broadcast channel determines what
    /// the subscription sees. Pass the receiver paired with the messages
    /// you want consumed (typically the `rx` returned alongside the `tx`).
    /// **Do not** pass `rx.resubscribe()` unless you specifically want to
    /// skip messages already queued in `rx`; `resubscribe()` positions a
    /// fresh receiver at the channel's *current tail*, which silently
    /// hides already-queued items and causes "test ran to completion but
    /// the assertion never fired" failures.
    #[cfg(test)]
    pub(crate) fn new(receiver: broadcast::Receiver<RoutedItem>) -> Self {
        let template = receiver.resubscribe();
        Self {
            template_receiver: template,
            stream: BroadcastStream::new(receiver),
            cleanup_sender: None,
            cleanup_signal: None,
            cleanup_sent: false,
        }
    }

    pub(crate) fn with_cleanup(
        receiver: broadcast::Receiver<RoutedItem>,
        cleanup_sender: mpsc::UnboundedSender<CleanupSignal>,
        cleanup_signal: CleanupSignal,
    ) -> Self {
        let template = receiver.resubscribe();
        Self {
            template_receiver: template,
            stream: BroadcastStream::new(receiver),
            cleanup_sender: Some(cleanup_sender),
            cleanup_signal: Some(cleanup_signal),
            cleanup_sent: false,
        }
    }

    pub async fn next(&mut self) -> Option<Result<ResponseMessage, Error>> {
        loop {
            match self.stream.next().await? {
                Ok(item) => {
                    if let Some(legacy) = item.into_legacy() {
                        return Some(legacy);
                    }
                }
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => return Some(Err(Error::SubscriptionLagged(n))),
            }
        }
    }

    /// Receive the next typed envelope (Response / Notice / Error) without
    /// the legacy projection. Kept because `src/transport/async_tests.rs`
    /// stub fixtures drive the channel directly via this helper.
    #[cfg(test)]
    pub(crate) async fn next_routed(&mut self) -> Option<RoutedItem> {
        loop {
            match self.stream.next().await? {
                Ok(item) => return Some(item),
                Err(_lagged) => continue,
            }
        }
    }

    /// Non-blocking poll for "is anything immediately available?". Returns
    /// `None` if the stream is pending or closed. Used by test fixtures that
    /// assert no cross-talk between subscriptions.
    #[cfg(test)]
    pub(crate) fn try_next_routed(&mut self) -> Option<RoutedItem> {
        use futures::FutureExt;
        loop {
            match self.stream.next().now_or_never()? {
                Some(Ok(item)) => return Some(item),
                Some(Err(_lagged)) => continue,
                None => return None,
            }
        }
    }

    /// Manually send cleanup signal
    fn send_cleanup_signal(&mut self) {
        if !self.cleanup_sent {
            if let (Some(sender), Some(signal)) = (&self.cleanup_sender, &self.cleanup_signal) {
                let _ = sender.send(signal.clone());
                self.cleanup_sent = true;
            }
        }
    }
}

/// Send cleanup signal when subscription is dropped
impl Drop for AsyncInternalSubscription {
    fn drop(&mut self) {
        self.send_cleanup_signal();
    }
}

type BroadcastSender = broadcast::Sender<RoutedItem>;

#[derive(Debug)]
struct ChannelEntry {
    generation: u64,
    sender: BroadcastSender,
}

impl Deref for ChannelEntry {
    type Target = BroadcastSender;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

type ChannelMap = Arc<RwLock<HashMap<i32, ChannelEntry>>>;

enum WriteCommand {
    Message {
        message: Vec<u8>,
        epoch: u64,
        order_claim: Option<OwnedIdClaim>,
        result: oneshot::Sender<Result<(), Error>>,
    },
    Reconnect {
        result: oneshot::Sender<Result<(), Error>>,
        resume: oneshot::Receiver<()>,
    },
}

async fn remove_channel_generation(channels: &RwLock<HashMap<i32, ChannelEntry>>, id: i32, generation: u64) {
    let mut channels = channels.write().await;
    if channels.get(&id).is_some_and(|entry| entry.generation == generation) {
        channels.remove(&id);
    }
}

async fn restore_channel_generation(channels: &RwLock<HashMap<i32, ChannelEntry>>, id: i32, failed_generation: u64, previous: Option<ChannelEntry>) {
    let mut channels = channels.write().await;
    if channels.get(&id).is_some_and(|entry| entry.generation == failed_generation) {
        if let Some(previous) = previous {
            channels.insert(id, previous);
        } else {
            channels.remove(&id);
        }
    }
}

/// Asynchronous TCP message bus implementation
pub struct AsyncTcpMessageBus<S: AsyncStream = AsyncTcpSocket> {
    connection: Arc<AsyncConnection<S>>,
    /// Maps request IDs to their response channels
    request_channels: ChannelMap,
    /// Maps IncomingMessages to broadcast senders (like sync does)
    shared_channel_senders: Arc<RwLock<HashMap<IncomingMessages, Vec<BroadcastSender>>>>,
    /// Maps OutgoingMessages to receivers for client subscription
    shared_channel_receivers: Arc<RwLock<HashMap<OutgoingMessages, broadcast::Receiver<RoutedItem>>>>,
    /// Maps order IDs to their response channels
    order_channels: ChannelMap,
    next_order_channel_generation: AtomicU64,
    /// Rejects explicit cross-kind reuse of the ID shared by TWS request and
    /// order error frames.
    id_origins: Arc<ErrorRouteOwners>,
    /// Maps execution IDs to their response channels (for commission reports)
    execution_channels: Arc<RwLock<HashMap<String, BroadcastSender>>>,
    /// Optional channel for order update stream
    order_update_stream: Arc<RwLock<Option<BroadcastSender>>>,
    /// Channel for cleanup signals
    cleanup_sender: mpsc::UnboundedSender<CleanupSignal>,
    /// Cancellation-safe serialized writer. Once a command is queued, dropping
    /// the caller cannot cancel a partially emitted TWS frame.
    write_sender: mpsc::UnboundedSender<WriteCommand>,
    write_task: Mutex<Option<task::JoinHandle<()>>>,
    connection_epoch: Arc<AtomicU64>,
    /// Handle to the message processing task
    process_task: Arc<RwLock<Option<task::JoinHandle<()>>>>,
    /// Shutdown flag
    shutdown_requested: Arc<AtomicBool>,
    /// Notification to wake the message loop on shutdown
    shutdown_notify: Arc<Notify>,
    connected: Arc<AtomicBool>,
}

impl<S: AsyncStream> Drop for AsyncTcpMessageBus<S> {
    fn drop(&mut self) {
        debug!("dropping async tcp message bus");
        // Set the shutdown flag and notify the message loop to exit
        self.shutdown_requested.store(true, Ordering::Relaxed);
        self.shutdown_notify.notify_one();
        if let Some(task) = self.write_task.get_mut().take() {
            task.abort();
        }
    }
}

impl<S: AsyncStream> AsyncTcpMessageBus<S> {
    /// Create a new async TCP message bus
    pub fn new(connection: AsyncConnection<S>) -> Result<Self, Error> {
        let (cleanup_sender, cleanup_receiver) = mpsc::unbounded_channel();
        let (write_sender, mut write_receiver) = mpsc::unbounded_channel();

        // Pre-create broadcast channels for all shared channels (like sync does)
        let mut shared_channel_senders = HashMap::new();
        let mut shared_channel_receivers = HashMap::new();

        for mapping in shared_channel_configuration::CHANNEL_MAPPINGS {
            let (sender, receiver) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
            shared_channel_receivers.insert(mapping.request, receiver);

            // Map each response type to the sender (multiple response types can share same sender)
            for response_type in mapping.responses {
                shared_channel_senders.entry(*response_type).or_insert_with(Vec::new).push(sender.clone());
            }
        }

        let connection = Arc::new(connection);
        let writer_connection = connection.clone();
        let connection_epoch = Arc::new(AtomicU64::new(0));
        let writer_epoch = connection_epoch.clone();
        let connected = Arc::new(AtomicBool::new(true));
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(Notify::new());
        let writer_connected = connected.clone();
        let writer_shutdown = shutdown_requested.clone();
        let writer_notify = shutdown_notify.clone();
        let write_task = task::spawn(async move {
            while let Some(command) = write_receiver.recv().await {
                match command {
                    WriteCommand::Message {
                        message,
                        epoch,
                        mut order_claim,
                        result,
                    } => {
                        let stale = epoch != writer_epoch.load(Ordering::Acquire);
                        let write_result = if !stale {
                            if let Some(claim) = order_claim.take() {
                                claim.commit();
                            }
                            // A partial frame cannot be retried on this connection. A
                            // stalled write permanently fences this writer and its queue.
                            match tokio::time::timeout(WRITE_TIMEOUT, writer_connection.write_message(&message)).await {
                                Ok(result) => result,
                                Err(_) => Err(Error::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "IB socket write timed out"))),
                            }
                        } else {
                            drop(order_claim.take());
                            Err(Error::ConnectionReset)
                        };
                        let failed = !stale && write_result.is_err();
                        if failed {
                            writer_connected.store(false, Ordering::Release);
                            writer_epoch.fetch_add(1, Ordering::AcqRel);
                            writer_shutdown.store(true, Ordering::Release);
                            writer_notify.notify_one();
                        }
                        let _ = result.send(write_result);
                        if failed {
                            // Dropping queued commands fails their waiters and rolls back
                            // only uncommitted ID claims. Nothing else may reach this socket.
                            break;
                        }
                    }
                    WriteCommand::Reconnect { result, resume } => {
                        let reconnect_result = tokio::time::timeout(WRITE_TIMEOUT, writer_connection.reconnect())
                            .await
                            .unwrap_or(Err(Error::ConnectionFailed));
                        let reconnected = reconnect_result.is_ok();
                        let _ = result.send(reconnect_result);
                        if reconnected {
                            let _ = resume.await;
                        }
                    }
                }
            }
        });

        let message_bus = Self {
            connection,
            request_channels: Arc::new(RwLock::new(HashMap::new())),
            shared_channel_senders: Arc::new(RwLock::new(shared_channel_senders)),
            shared_channel_receivers: Arc::new(RwLock::new(shared_channel_receivers)),
            order_channels: Arc::new(RwLock::new(HashMap::new())),
            next_order_channel_generation: AtomicU64::new(0),
            id_origins: Arc::new(ErrorRouteOwners::default()),
            execution_channels: Arc::new(RwLock::new(HashMap::new())),
            order_update_stream: Arc::new(RwLock::new(None)),
            cleanup_sender,
            write_sender,
            write_task: Mutex::new(Some(write_task)),
            connection_epoch,
            process_task: Arc::new(RwLock::new(None)),
            shutdown_requested,
            shutdown_notify,
            connected,
        };

        // Start cleanup task
        let request_channels = message_bus.request_channels.clone();
        let order_channels = message_bus.order_channels.clone();
        let id_origins = message_bus.id_origins.clone();
        let order_update_stream = message_bus.order_update_stream.clone();

        task::spawn(async move {
            let mut receiver = cleanup_receiver;
            while let Some(signal) = receiver.recv().await {
                match signal {
                    CleanupSignal::Request(request_owner) => {
                        let request_id = request_owner.id;
                        remove_channel_generation(&request_channels, request_id, request_owner.generation).await;
                        id_origins.release_request(request_owner);
                        debug!("Cleaned up request channel for ID: {request_id}");
                    }
                    CleanupSignal::Order(order_channel) => {
                        let order_id = order_channel.id;
                        remove_channel_generation(&order_channels, order_id, order_channel.generation).await;
                        debug!("Cleaned up order channel for ID: {order_id}");
                    }
                    CleanupSignal::Shared(message_type) => {
                        // Shared channels are persistent and should not be removed
                        // They are created at initialization and reused across multiple requests
                        debug!("Subscription for shared channel {:?} ended (channel remains active)", message_type);
                    }
                    CleanupSignal::OrderUpdateStream => {
                        let mut stream = order_update_stream.write().await;
                        *stream = None;
                        debug!("Cleaned up order update stream ownership");
                    }
                    #[cfg(test)]
                    CleanupSignal::Barrier(acknowledge) => acknowledge.notify_one(),
                }
            }
        });

        Ok(message_bus)
    }

    fn begin_write(&self) -> Result<u64, Error> {
        if self.shutdown_requested.load(Ordering::Relaxed) {
            return Err(Error::Shutdown);
        }
        let epoch = self.connection_epoch.load(Ordering::Acquire);
        if !self.connected.load(Ordering::Acquire) {
            return Err(Error::ConnectionReset);
        }
        if self.connection_epoch.load(Ordering::Acquire) != epoch {
            return Err(Error::ConnectionReset);
        }
        Ok(epoch)
    }

    fn enqueue_write_message(&self, message: Vec<u8>, epoch: u64) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        self.enqueue_write_command(message, epoch, None)
    }

    fn enqueue_order_write_message(
        &self,
        message: Vec<u8>,
        epoch: u64,
        order_claim: OwnedIdClaim,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        self.enqueue_write_command(message, epoch, Some(order_claim))
    }

    fn enqueue_write_command(
        &self,
        message: Vec<u8>,
        epoch: u64,
        order_claim: Option<OwnedIdClaim>,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        if self.shutdown_requested.load(Ordering::Relaxed) {
            return Err(Error::Shutdown);
        }
        if !self.connected.load(Ordering::Acquire) || self.connection_epoch.load(Ordering::Acquire) != epoch {
            return Err(Error::ConnectionReset);
        }
        let (result, receiver) = oneshot::channel();
        self.write_sender
            .send(WriteCommand::Message {
                message,
                epoch,
                order_claim,
                result,
            })
            .map_err(|_| Error::ConnectionFailed)?;
        Ok(receiver)
    }

    async fn finish_write_message(write: oneshot::Receiver<Result<(), Error>>) -> Result<(), Error> {
        write
            .await
            .map_err(|_| Error::Simple("async writer stopped before reporting a result".into()))?
    }

    async fn shutdown_writer(&self) {
        self.connection_epoch.fetch_add(1, Ordering::AcqRel);
        let mut task = self.write_task.lock().await;
        if let Some(task) = task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    async fn reconnect_and_reset(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::Release);
        self.connection_epoch.fetch_add(1, Ordering::AcqRel);

        let (result, reconnect_result) = oneshot::channel();
        let (resume, resume_writer) = oneshot::channel();
        self.write_sender
            .send(WriteCommand::Reconnect {
                result,
                resume: resume_writer,
            })
            .map_err(|_| Error::ConnectionFailed)?;

        reconnect_result
            .await
            .map_err(|_| Error::Simple("async writer stopped before reporting reconnect result".into()))??;

        self.reset_channels().await;
        resume.send(()).map_err(|_| Error::ConnectionFailed)?;
        self.connected.store(true, Ordering::Release);
        Ok(())
    }

    /// Start processing messages from TWS
    pub fn process_messages(self: Arc<Self>, _server_version: i32, _reconnect_delay: Duration) -> Result<(), Error> {
        let message_bus = self.clone();
        let shutdown_notify = self.shutdown_notify.clone();

        let handle = task::spawn(async move {
            loop {
                // Use select with shutdown notification instead of a polling sleep.
                // This prevents cancelling read_and_route_message mid-read, which
                // would corrupt the TCP stream (read_exact is not cancellation-safe).
                tokio::select! {
                    _ = shutdown_notify.notified() => {
                        debug!("Shutdown notification received, stopping message processing");
                        message_bus.request_shutdown().await;
                        break;
                    }
                    result = message_bus.read_and_route_message() => {
                        use crate::client::error_handler::{is_connection_error, is_timeout_error};

                        match result {
                            Ok(_) => continue,
                            Err(ref err) if is_timeout_error(err) => {
                                if message_bus.shutdown_requested.load(Ordering::Relaxed) {
                                    debug!("dispatcher task exiting");
                                    break;
                                }
                                continue;
                            }
                            Err(ref err) if is_connection_error(err) => {
                                error!("Connection error detected, attempting to reconnect: {err:?}");
                                match message_bus.reconnect_and_reset().await {
                                    Ok(_) => {
                                        info!("Successfully reconnected to TWS/Gateway");
                                    }
                                    Err(e) => {
                                        error!("Failed to reconnect to TWS/Gateway: {e:?}");
                                        message_bus.request_shutdown().await;
                                        break;
                                    }
                                }
                                continue;
                            }
                            Err(Error::Shutdown) => {
                                error!("Received shutdown signal, stopping message processing.");
                                break;
                            }
                            Err(err) => {
                                error!("Error processing message (shutting down): {err:?}");
                                message_bus.request_shutdown().await;
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Store the task handle
        let process_task = self.process_task.clone();
        tokio::spawn(async move {
            let mut task_guard = process_task.write().await;
            *task_guard = Some(handle);
        });

        Ok(())
    }

    /// Read a message and route it to the appropriate channel
    pub(crate) async fn read_and_route_message(&self) -> Result<(), Error> {
        let message = self.connection.read_message().await?;

        // Use common routing logic
        match determine_routing(&message) {
            RoutingDecision::ByRequestId(request_id) => self.route_to_request_channel(request_id, message).await,
            RoutingDecision::ByOrderId(order_id) => self.route_to_order_channel(order_id, message).await,
            RoutingDecision::ByMessageType(message_type) => self.route_to_shared_channel(message_type, message).await,
            RoutingDecision::SharedMessage(message_type) => self.route_to_shared_channel(message_type, message).await,
            RoutingDecision::Error(payload) => self.route_error_message(message, payload).await,
            RoutingDecision::Shutdown => {
                debug!("Received shutdown message, calling request_shutdown");
                self.request_shutdown().await;
                Err(Error::Shutdown)
            }
        }
    }

    /// Reset all channels after reconnection
    async fn reset_channels(&self) {
        debug!("resetting message bus channels");

        {
            let channels = self.request_channels.read().await;
            for (_, sender) in channels.iter() {
                let _ = sender.send(Error::ConnectionReset.into());
            }
        }

        {
            let channels = self.order_channels.read().await;
            for (_, sender) in channels.iter() {
                let _ = sender.send(Error::ConnectionReset.into());
            }
        }

        {
            let mut channels = self.request_channels.write().await;
            channels.clear();
        }

        {
            let mut channels = self.order_channels.write().await;
            channels.clear();
        }

        {
            let mut channels = self.execution_channels.write().await;
            channels.clear();
        }

        self.id_origins.clear_requests();
    }

    /// Notify all waiting subscriptions about shutdown
    async fn request_shutdown(&self) {
        debug!("shutdown requested");

        // Set the shutdown flag and mark as disconnected
        self.connected.store(false, Ordering::Relaxed);
        self.shutdown_requested.store(true, Ordering::Relaxed);
        self.shutdown_notify.notify_one();
        self.shutdown_writer().await;

        // Clear all channels - dropping the senders will close the channels
        // and cause all receivers to get RecvError::Closed
        {
            let mut channels = self.request_channels.write().await;
            channels.clear();
        }

        {
            let mut channels = self.order_channels.write().await;
            channels.clear();
        }

        self.id_origins.clear();

        {
            let mut channels = self.shared_channel_senders.write().await;
            channels.clear();
        }

        {
            let mut channels = self.shared_channel_receivers.write().await;
            channels.clear();
        }

        {
            let mut order_update_stream = self.order_update_stream.write().await;
            *order_update_stream = None;
        }
    }

    /// Route error message using routing decision
    async fn route_error_message(&self, _message: ResponseMessage, payload: DecodedError) -> Result<(), Error> {
        let request_id = payload.request_id;
        let is_warning = is_warning_error(payload.error_code);
        let notice = Notice::from(payload);

        if request_id == UNSPECIFIED_REQUEST_ID {
            super::common::log_unrouted_notice(&notice);
            let _ = self.connection.notice_sender.send(notice);
        } else {
            let item = if is_warning {
                RoutedItem::Notice(notice.clone())
            } else {
                RoutedItem::Error(Error::Notice(notice.clone()))
            };
            match self.id_origins.origin(request_id) {
                Some(IdOrigin::Request) => {
                    let channels = self.request_channels.read().await;
                    if let Some(sender) = channels.get(&request_id) {
                        let _ = sender.send(item);
                    } else {
                        log_orphan(request_id, &item);
                    }
                }
                Some(IdOrigin::Order) => {
                    // A multiplexed stream must remain alive after one order is
                    // rejected, so its copy is a nonterminal Notice while an
                    // order-specific stream retains the terminal Error.
                    let sent_to_update_stream = self.send_order_update_item(RoutedItem::Notice(notice)).await;
                    let channels = self.order_channels.read().await;
                    if let Some(sender) = channels.get(&request_id) {
                        let _ = sender.send(item);
                    } else if !sent_to_update_stream {
                        log_orphan(request_id, &item);
                    }
                }
                None => log_orphan(request_id, &item),
            }
        }

        Ok(())
    }

    /// Route message to request-specific channel
    async fn route_to_request_channel(&self, request_id: i32, message: ResponseMessage) -> Result<(), Error> {
        let channels = self.request_channels.read().await;
        if let Some(sender) = channels.get(&request_id) {
            let _ = sender.send(message.into());
        }
        Ok(())
    }

    /// Route message to order-specific channel
    async fn route_to_order_channel(&self, order_id: i32, message: ResponseMessage) -> Result<(), Error> {
        if let Some(terminal_id) = terminal_order_id(&message) {
            self.id_origins.mark_order_terminal(terminal_id);
        }
        let routed = self.send_order_update(&message).await;
        let strategy = order_routing_strategy(message.message_type());

        match strategy {
            OrderRoutingStrategy::ExecutionData => {
                // Try order_id channel first, then request_id, storing execution_id mapping
                if let Some(actual_order_id) = message.order_id() {
                    let channels = self.order_channels.read().await;
                    if let Some(sender) = channels.get(&actual_order_id) {
                        self.store_execution_mapping(&message, sender).await;
                        let _ = sender.send(message.into());
                        return Ok(());
                    }
                }
                if let Some(req_id) = message.request_id() {
                    let channels = self.request_channels.read().await;
                    if let Some(sender) = channels.get(&req_id) {
                        self.store_execution_mapping(&message, sender).await;
                        let _ = sender.send(message.into());
                        return Ok(());
                    }
                }
                if !routed {
                    warn!("could not route ExecutionData message {:?}", message);
                }
            }
            OrderRoutingStrategy::ExecutionDataEnd => {
                if let Some(actual_order_id) = message.order_id() {
                    let channels = self.order_channels.read().await;
                    if let Some(sender) = channels.get(&actual_order_id) {
                        let _ = sender.send(message.into());
                        return Ok(());
                    }
                }
                if let Some(req_id) = message.request_id() {
                    let channels = self.request_channels.read().await;
                    if let Some(sender) = channels.get(&req_id) {
                        let _ = sender.send(message.into());
                        return Ok(());
                    }
                }
                warn!("could not route ExecutionDataEnd message {:?}", message);
            }
            OrderRoutingStrategy::OrderOrShared => {
                if let Some(actual_order_id) = message.order_id() {
                    let channels = self.order_channels.read().await;
                    if let Some(sender) = channels.get(&actual_order_id) {
                        let _ = sender.send(message.into());
                        return Ok(());
                    }
                    drop(channels);

                    let shared_channels = self.shared_channel_senders.read().await;
                    if let Some(senders) = shared_channels.get(&message.message_type()) {
                        for sender in senders {
                            let _ = sender.send(message.clone().into());
                        }
                        return Ok(());
                    }
                }
                if !routed {
                    warn!("could not route message {:?}", message);
                }
            }
            OrderRoutingStrategy::ByExecutionId => {
                if let Some(execution_id) = message.execution_id() {
                    let exec_channels = self.execution_channels.read().await;
                    if let Some(sender) = exec_channels.get(&execution_id) {
                        let _ = sender.send(message.into());
                        return Ok(());
                    }
                }
            }
            OrderRoutingStrategy::SharedOnly => {
                let shared_channels = self.shared_channel_senders.read().await;
                if let Some(senders) = shared_channels.get(&message.message_type()) {
                    for sender in senders {
                        let _ = sender.send(message.clone().into());
                    }
                    return Ok(());
                }
                if !routed {
                    warn!("could not route message {:?}", message);
                }
            }
            OrderRoutingStrategy::ByOrderId => {
                if order_id >= 0 {
                    let channels = self.order_channels.read().await;
                    if let Some(sender) = channels.get(&order_id) {
                        let _ = sender.send(message.into());
                        return Ok(());
                    }
                }
                if !routed {
                    warn!("could not route message {:?}", message);
                }
            }
        }

        Ok(())
    }

    /// Store execution_id -> sender mapping for commission report routing
    async fn store_execution_mapping(&self, message: &ResponseMessage, sender: &BroadcastSender) {
        if let Some(execution_id) = message.execution_id() {
            let mut exec_channels = self.execution_channels.write().await;
            exec_channels.insert(execution_id, sender.clone());
        }
    }

    /// Route message to shared channel
    async fn route_to_shared_channel(&self, message_type: IncomingMessages, message: ResponseMessage) -> Result<(), Error> {
        // Send order-related messages to order update stream
        match message_type {
            IncomingMessages::OpenOrder
            | IncomingMessages::OrderStatus
            | IncomingMessages::ExecutionData
            | IncomingMessages::CommissionsReport
            | IncomingMessages::CompletedOrder => {
                self.send_order_update(&message).await;
            }
            _ => {}
        }

        // Route to all senders for this message type (like sync does)
        let channels = self.shared_channel_senders.read().await;
        if let Some(senders) = channels.get(&message_type) {
            // Broadcast to all subscribers
            for sender in senders {
                if let Err(e) = sender.send(message.clone().into()) {
                    warn!("error sending to shared channel for {message_type:?}: {e}");
                }
            }
        }

        Ok(())
    }

    /// Send message to order update stream if it exists
    async fn send_order_update(&self, message: &ResponseMessage) -> bool {
        self.send_order_update_item(message.clone().into()).await
    }

    /// Send a pre-classified item to the order update stream if it exists.
    async fn send_order_update_item(&self, item: RoutedItem) -> bool {
        let order_update_stream = self.order_update_stream.read().await;
        if let Some(sender) = order_update_stream.as_ref() {
            if let Err(e) = sender.send(item) {
                warn!("error sending to order update stream: {e}");
                return false;
            }
            return true;
        }
        false
    }
}

#[async_trait]
impl<S: AsyncStream> AsyncMessageBus for AsyncTcpMessageBus<S> {
    async fn send_request(&self, request_id: i32, message: Vec<u8>) -> Result<AsyncInternalSubscription, Error> {
        let epoch = self.begin_write()?;
        let claim = self.id_origins.claim_request(request_id)?;
        let request_owner = claim.token();
        let (sender, receiver) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);

        let previous_channel = {
            let mut channels = self.request_channels.write().await;
            channels.insert(
                request_id,
                ChannelEntry {
                    generation: request_owner.generation,
                    sender,
                },
            )
        };

        let write = match self.enqueue_write_message(message, epoch) {
            Ok(write) => write,
            Err(error) => {
                restore_channel_generation(&self.request_channels, request_id, request_owner.generation, previous_channel).await;
                return Err(error);
            }
        };
        claim.commit();
        let subscription = AsyncInternalSubscription::with_cleanup(receiver, self.cleanup_sender.clone(), CleanupSignal::Request(request_owner));

        if let Err(error) = Self::finish_write_message(write).await {
            restore_channel_generation(&self.request_channels, request_id, request_owner.generation, previous_channel).await;
            self.id_origins.release_request(request_owner);
            return Err(error);
        }

        Ok(subscription)
    }

    async fn send_order_request(&self, order_id: i32, message: Vec<u8>) -> Result<AsyncInternalSubscription, Error> {
        let epoch = self.begin_write()?;
        let claim = self.id_origins.claim_order(order_id)?.into_owned(self.id_origins.clone());
        let order_channel = OrderChannelToken {
            id: order_id,
            generation: self.next_order_channel_generation.fetch_add(1, Ordering::Relaxed),
        };
        let (sender, receiver) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);

        let previous_channel = {
            let mut channels = self.order_channels.write().await;
            channels.insert(
                order_id,
                ChannelEntry {
                    generation: order_channel.generation,
                    sender,
                },
            )
        };

        let write = match self.enqueue_order_write_message(message, epoch, claim) {
            Ok(write) => write,
            Err(error) => {
                restore_channel_generation(&self.order_channels, order_id, order_channel.generation, previous_channel).await;
                return Err(error);
            }
        };
        let subscription = AsyncInternalSubscription::with_cleanup(receiver, self.cleanup_sender.clone(), CleanupSignal::Order(order_channel));

        if let Err(error) = Self::finish_write_message(write).await {
            restore_channel_generation(&self.order_channels, order_id, order_channel.generation, previous_channel).await;
            return Err(error);
        }

        Ok(subscription)
    }

    async fn send_order_message(&self, order_id: i32, message: Vec<u8>) -> Result<(), Error> {
        let epoch = self.begin_write()?;
        let claim = self.id_origins.claim_order(order_id)?.into_owned(self.id_origins.clone());
        let write = self.enqueue_order_write_message(message, epoch, claim)?;
        Self::finish_write_message(write).await
    }

    async fn send_shared_request(&self, message_type: OutgoingMessages, message: Vec<u8>) -> Result<AsyncInternalSubscription, Error> {
        let epoch = self.begin_write()?;
        let receiver = {
            let channels = self.shared_channel_receivers.read().await;
            if let Some(receiver) = channels.get(&message_type) {
                receiver.resubscribe()
            } else {
                return Err(Error::InvalidArgument(format!(
                    "No shared channel configured for message type: {:?}",
                    message_type
                )));
            }
        };

        let write = self.enqueue_write_message(message, epoch)?;
        Self::finish_write_message(write).await?;

        Ok(AsyncInternalSubscription::with_cleanup(
            receiver,
            self.cleanup_sender.clone(),
            CleanupSignal::Shared(message_type),
        ))
    }

    async fn send_message(&self, message: Vec<u8>) -> Result<(), Error> {
        let epoch = self.begin_write()?;
        let write = self.enqueue_write_message(message, epoch)?;
        Self::finish_write_message(write).await
    }

    async fn cancel_subscription(&self, request_id: i32, message: Vec<u8>) -> Result<(), Error> {
        let epoch = self.begin_write()?;
        let write = self.enqueue_write_message(message, epoch)?;
        Self::finish_write_message(write).await?;

        // Single write lock: the previous version held a read guard while
        // awaiting the write upgrade and self-deadlocked on the same task.
        let mut channels = self.request_channels.write().await;
        if let Some(sender) = channels.get(&request_id) {
            let _ = sender.send(Error::Cancelled.into());
        }
        channels.remove(&request_id);
        drop(channels);
        self.id_origins.release_request_id(request_id);

        Ok(())
    }

    async fn cancel_order_subscription(&self, order_id: i32, message: Vec<u8>) -> Result<(), Error> {
        let epoch = self.begin_write()?;
        let write = self.enqueue_write_message(message, epoch)?;
        Self::finish_write_message(write).await?;

        let mut channels = self.order_channels.write().await;
        if let Some(sender) = channels.get(&order_id) {
            let _ = sender.send(Error::Cancelled.into());
        }
        channels.remove(&order_id);

        Ok(())
    }

    async fn create_order_update_subscription(&self) -> Result<AsyncInternalSubscription, Error> {
        let mut order_update_stream = self.order_update_stream.write().await;

        if order_update_stream.is_some() {
            return Err(Error::AlreadySubscribed);
        }

        let (sender, receiver) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);

        *order_update_stream = Some(sender);

        Ok(AsyncInternalSubscription::with_cleanup(
            receiver,
            self.cleanup_sender.clone(),
            CleanupSignal::OrderUpdateStream,
        ))
    }

    fn notice_subscribe(&self) -> crate::subscriptions::notice_stream::async_impl::NoticeStream {
        crate::subscriptions::notice_stream::async_impl::NoticeStream::new(self.connection.notice_sender.subscribe())
    }

    async fn ensure_shutdown(&self) {
        debug!("ensure_shutdown called");

        // Request shutdown
        self.request_shutdown().await;

        // Wait for the processing task to finish
        let task_handle = {
            let mut task_guard = self.process_task.write().await;
            task_guard.take()
        };

        if let Some(handle) = task_handle {
            debug!("Waiting for processing task to finish");
            if let Err(e) = handle.await {
                warn!("Error joining processing task: {e}");
            }
            debug!("Processing task finished");
        }
    }

    fn request_shutdown_sync(&self) {
        debug!("sync shutdown requested");
        self.connected.store(false, Ordering::Relaxed);
        self.shutdown_requested.store(true, Ordering::Relaxed);
        self.shutdown_notify.notify_one();
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed) && !self.shutdown_requested.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod memory;
#[cfg(test)]
pub(crate) use memory::MemoryStream;

#[cfg(test)]
pub(crate) mod test_listener;

#[cfg(test)]
#[path = "async_tests.rs"]
mod tests;
