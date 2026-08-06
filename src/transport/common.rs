//! Common utilities shared between sync and async transport implementations

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use log::{error, info, warn};

use crate::messages::{ConnectivityStatus, Notice};
use crate::subscriptions::common::RoutedItem;
use crate::Error;

const ORDER_ERROR_OWNER_GRACE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdOrigin {
    Request,
    Order,
}

#[derive(Debug, Clone, Copy)]
struct OwnerEntry {
    origin: IdOrigin,
    generation: u64,
    terminal_revision: u64,
    pending_claims: usize,
    committed: bool,
    expires_at: Option<Instant>,
}

#[derive(Debug, Default)]
struct OwnerRegistryState {
    entries: HashMap<i32, OwnerEntry>,
    expirations: BinaryHeap<Reverse<(Instant, i32, u64)>>,
}

/// Owns the otherwise untyped ID carried by TWS Error frames.
///
/// TWS reports request and order failures through one integer field. Automatic
/// IDs share one allocator, while callers may still provide explicit IDs.
/// Claims reject cross-kind reuse before a packet is written and keep
/// fire-and-forget order ownership after any per-order subscription is dropped.
#[derive(Debug, Default)]
pub(crate) struct ErrorRouteOwners {
    state: Mutex<OwnerRegistryState>,
    next_generation: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IdOwnerToken {
    pub(crate) id: i32,
    pub(crate) generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrderChannelToken {
    pub(crate) id: i32,
    pub(crate) generation: u64,
}

pub(crate) struct IdClaim<'a> {
    owners: &'a ErrorRouteOwners,
    id: i32,
    origin: IdOrigin,
    generation: u64,
    terminal_revision: u64,
    finished: bool,
}

pub(crate) struct OwnedIdClaim {
    owners: Arc<ErrorRouteOwners>,
    id: i32,
    origin: IdOrigin,
    generation: u64,
    terminal_revision: u64,
    finished: bool,
}

impl ErrorRouteOwners {
    pub(crate) fn claim_request(&self, id: i32) -> Result<IdClaim<'_>, Error> {
        self.claim(id, IdOrigin::Request)
    }

    pub(crate) fn claim_order(&self, id: i32) -> Result<IdClaim<'_>, Error> {
        self.claim(id, IdOrigin::Order)
    }

    fn claim(&self, id: i32, origin: IdOrigin) -> Result<IdClaim<'_>, Error> {
        let mut state = self.state.lock().unwrap();
        Self::prune_expired(&mut state);
        let generation = match state.entries.get_mut(&id) {
            None => {
                let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                state.entries.insert(
                    id,
                    OwnerEntry {
                        origin,
                        generation,
                        terminal_revision: 0,
                        pending_claims: 1,
                        committed: false,
                        expires_at: None,
                    },
                );
                generation
            }
            Some(entry) if entry.origin == IdOrigin::Order && origin == IdOrigin::Order => {
                entry.pending_claims = entry.pending_claims.saturating_add(1);
                entry.generation
            }
            Some(entry) => {
                return Err(Error::InvalidArgument(format!(
                    "TWS id {id} is already registered as {:?}; cannot reuse it as {origin:?}",
                    entry.origin
                )));
            }
        };
        let terminal_revision = state
            .entries
            .get(&id)
            .expect("a successful claim must retain its owner entry")
            .terminal_revision;
        Ok(IdClaim {
            owners: self,
            id,
            origin,
            generation,
            terminal_revision,
            finished: false,
        })
    }

    pub(crate) fn origin(&self, id: i32) -> Option<IdOrigin> {
        let mut state = self.state.lock().unwrap();
        Self::prune_expired(&mut state);
        state.entries.get(&id).map(|entry| entry.origin)
    }

    pub(crate) fn release_request(&self, token: IdOwnerToken) {
        let mut state = self.state.lock().unwrap();
        Self::prune_expired(&mut state);
        if state
            .entries
            .get(&token.id)
            .is_some_and(|entry| entry.origin == IdOrigin::Request && entry.generation == token.generation)
        {
            state.entries.remove(&token.id);
        }
    }

    pub(crate) fn release_request_id(&self, id: i32) {
        let mut state = self.state.lock().unwrap();
        Self::prune_expired(&mut state);
        if state.entries.get(&id).is_some_and(|entry| entry.origin == IdOrigin::Request) {
            state.entries.remove(&id);
        }
    }

    pub(crate) fn mark_order_terminal(&self, id: i32) {
        self.mark_order_terminal_with_grace(id, ORDER_ERROR_OWNER_GRACE);
    }

    #[cfg(test)]
    pub(crate) fn order_expiration(&self, id: i32) -> Option<Instant> {
        self.state.lock().unwrap().entries.get(&id).and_then(|entry| entry.expires_at)
    }

    fn mark_order_terminal_with_grace(&self, id: i32, grace: Duration) {
        let mut state = self.state.lock().unwrap();
        Self::prune_expired(&mut state);
        let expires_at = Instant::now() + grace;
        let generation = if let Some(entry) = state.entries.get_mut(&id).filter(|entry| entry.origin == IdOrigin::Order) {
            entry.expires_at = Some(expires_at);
            entry.terminal_revision = entry.terminal_revision.wrapping_add(1);
            Some(entry.generation)
        } else {
            None
        };
        if let Some(generation) = generation {
            state.expirations.push(Reverse((expires_at, id, generation)));
        }
    }

    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.entries.clear();
        state.expirations.clear();
    }

    pub(crate) fn clear_requests(&self) {
        let mut state = self.state.lock().unwrap();
        Self::prune_expired(&mut state);
        state.entries.retain(|_, entry| entry.origin == IdOrigin::Order);
    }

    fn commit_claim(&self, id: i32, origin: IdOrigin, generation: u64, terminal_revision: u64) {
        let mut state = self.state.lock().unwrap();
        Self::prune_expired(&mut state);
        if let Some(entry) = state
            .entries
            .get_mut(&id)
            .filter(|entry| entry.origin == origin && entry.generation == generation)
        {
            entry.pending_claims = entry.pending_claims.saturating_sub(1);
            entry.committed = true;
            if entry.terminal_revision == terminal_revision {
                entry.expires_at = None;
            }
        }
    }

    fn rollback_claim(&self, id: i32, origin: IdOrigin, generation: u64) {
        let mut state = self.state.lock().unwrap();
        Self::prune_expired(&mut state);
        let remove = if let Some(entry) = state
            .entries
            .get_mut(&id)
            .filter(|entry| entry.origin == origin && entry.generation == generation)
        {
            entry.pending_claims = entry.pending_claims.saturating_sub(1);
            let expiry_elapsed = entry.expires_at.is_some_and(|expires_at| expires_at <= Instant::now());
            entry.pending_claims == 0 && (!entry.committed || expiry_elapsed)
        } else {
            false
        };
        if remove {
            state.entries.remove(&id);
        }
    }

    fn prune_expired(state: &mut OwnerRegistryState) {
        let now = Instant::now();
        let mut blocked = Vec::new();
        while let Some(Reverse((expires_at, id, generation))) = state.expirations.peek().copied() {
            if expires_at > now {
                break;
            }
            state.expirations.pop();
            match state.entries.get(&id) {
                Some(entry) if entry.generation == generation && entry.expires_at == Some(expires_at) && entry.pending_claims == 0 => {
                    state.entries.remove(&id);
                }
                Some(entry) if entry.generation == generation && entry.expires_at == Some(expires_at) => {
                    blocked.push(Reverse((expires_at, id, generation)));
                }
                _ => {}
            }
        }
        state.expirations.extend(blocked);
    }
}

impl IdClaim<'_> {
    pub(crate) fn token(&self) -> IdOwnerToken {
        IdOwnerToken {
            id: self.id,
            generation: self.generation,
        }
    }

    pub(crate) fn commit(mut self) -> IdOwnerToken {
        self.owners.commit_claim(self.id, self.origin, self.generation, self.terminal_revision);
        self.finished = true;
        self.token()
    }

    pub(crate) fn into_owned(mut self, owners: Arc<ErrorRouteOwners>) -> OwnedIdClaim {
        debug_assert!(std::ptr::eq(self.owners, owners.as_ref()));
        self.finished = true;
        OwnedIdClaim {
            owners,
            id: self.id,
            origin: self.origin,
            generation: self.generation,
            terminal_revision: self.terminal_revision,
            finished: false,
        }
    }
}

impl Drop for IdClaim<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.owners.rollback_claim(self.id, self.origin, self.generation);
        }
    }
}

impl OwnedIdClaim {
    pub(crate) fn commit(mut self) -> IdOwnerToken {
        self.owners.commit_claim(self.id, self.origin, self.generation, self.terminal_revision);
        self.finished = true;
        IdOwnerToken {
            id: self.id,
            generation: self.generation,
        }
    }
}

impl Drop for OwnedIdClaim {
    fn drop(&mut self) {
        if !self.finished {
            self.owners.rollback_claim(self.id, self.origin, self.generation);
        }
    }
}

/// A notice reports *healthy* data-farm connectivity ("…connection is OK")
/// rather than a problem. IB's message-codes reference classifies these as
/// System Notifications, not warnings, so they're logged at info instead of
/// warn. Only [`ConnectivityStatus::Ok`] is benign — `Broken`/`Inactive`/
/// `Connecting` stay at warn via [`Notice::is_warning`].
fn is_benign_connectivity_notice(notice: &Notice) -> bool {
    notice.connectivity_status() == Some(ConnectivityStatus::Ok)
}

/// Log an unrouted notice (no subscription owner) at the appropriate severity.
pub(crate) fn log_unrouted_notice(notice: &Notice) {
    if is_benign_connectivity_notice(notice) {
        info!("connectivity: {notice}");
    } else if notice.is_warning() {
        warn!("warning: {notice}");
    } else {
        error!("error: {notice}");
    }
}

/// Log a routed notice/error that arrived bound to an id with no matching
/// request or order channel. The dispatcher only constructs `Notice` and
/// `Error` variants for this path; `Response` is unreachable here.
pub(crate) fn log_orphan(request_id: i32, item: &RoutedItem) {
    match item {
        RoutedItem::Notice(n) => info!("no recipient for notice (id={request_id}): {n}"),
        RoutedItem::Error(e) => info!("no recipient for error (id={request_id}): {e}"),
        RoutedItem::Response(_) => {}
    }
}

/// Maximum number of reconnection attempts
pub(crate) const MAX_RECONNECT_ATTEMPTS: i32 = 20;

/// Fibonacci backoff for reconnection attempts
pub(crate) struct FibonacciBackoff {
    previous: u64,
    current: u64,
    max: u64,
}

impl FibonacciBackoff {
    pub(crate) fn new(max: u64) -> Self {
        FibonacciBackoff {
            previous: 0,
            current: 1,
            max,
        }
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        let next = self.previous + self.current;
        self.previous = self.current;
        self.current = next;

        if next > self.max {
            Duration::from_secs(self.max)
        } else {
            Duration::from_secs(next)
        }
    }
}

#[cfg(test)]
#[path = "common_tests.rs"]
mod tests;
