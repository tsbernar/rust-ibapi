//! ID generation for requests and orders
//!
//! This module provides thread-safe ID generation for request IDs and order IDs.
//! Request IDs are used to track API requests, while order IDs are used for order placement.

use std::sync::atomic::{AtomicI32, Ordering};

/// Starting value for request IDs
const INITIAL_REQUEST_ID: i32 = 9000;

/// Thread-safe ID generator using atomic operations
#[derive(Debug)]
pub(crate) struct IdGenerator {
    next_id: AtomicI32,
}

impl IdGenerator {
    /// Creates a new ID generator with the specified starting value
    pub(crate) fn new(start: i32) -> Self {
        Self {
            next_id: AtomicI32::new(start),
        }
    }

    /// Gets the next ID, incrementing the internal counter
    pub(crate) fn next(&self) -> i32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Gets the current ID without incrementing
    #[allow(dead_code)]
    pub(crate) fn current(&self) -> i32 {
        self.next_id.load(Ordering::Relaxed)
    }

    /// Sets the next ID value (useful for order ID updates from server)
    pub(crate) fn set(&self, value: i32) {
        self.next_id.store(value, Ordering::Relaxed);
    }

    /// Advances to at least `value` without reusing an ID already handed out.
    #[cfg(test)]
    pub(crate) fn advance_to(&self, value: i32) {
        self.next_id.fetch_max(value, Ordering::Relaxed);
    }

    /// Atomically reserves and returns an ID at or above `floor`.
    pub(crate) fn reserve_at_least(&self, floor: i32) -> i32 {
        self.try_reserve_at_least(floor).expect("invalid or exhausted IB API ID sequence")
    }

    pub(crate) fn try_reserve_at_least(&self, floor: i32) -> Option<i32> {
        if floor < 0 {
            return None;
        }
        let mut observed = self.next_id.load(Ordering::Relaxed);
        loop {
            if observed < 0 {
                return None;
            }
            let reserved = observed.max(floor);
            let next = reserved.checked_add(1)?;
            match self.next_id.compare_exchange_weak(observed, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return Some(reserved),
                Err(actual) => observed = actual,
            }
        }
    }

    /// Resets the generator to a new starting value
    #[allow(dead_code)]
    pub(crate) fn reset(&self, start: i32) {
        self.set(start);
    }
}

impl Default for IdGenerator {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Manages one collision-free request/order ID namespace for a client.
///
/// TWS Error frames carry only an integer ID, without identifying whether it
/// came from a request or an order. Sharing the allocator prevents automatic
/// IDs from becoming ambiguous as the server-provided order sequence grows.
#[derive(Debug)]
pub(crate) struct ClientIdManager {
    ids: IdGenerator,
}

impl ClientIdManager {
    /// Starts at the larger of the local request floor and the server's next
    /// valid order ID.
    pub(crate) fn new(initial_order_id: i32) -> Self {
        Self {
            ids: IdGenerator::new(INITIAL_REQUEST_ID.max(initial_order_id)),
        }
    }

    /// Gets the next request ID
    pub(crate) fn next_request_id(&self) -> i32 {
        self.ids.next()
    }

    /// Gets the next order ID
    pub(crate) fn next_order_id(&self) -> i32 {
        self.ids.next()
    }

    /// Updates the order ID (e.g., from server's next valid ID response)
    #[cfg(test)]
    pub(crate) fn set_order_id(&self, order_id: i32) {
        self.ids.advance_to(order_id);
    }

    /// Atomically reserves an order ID at or above the server-provided floor.
    pub(crate) fn reserve_order_id_at_least(&self, order_id: i32) -> i32 {
        self.ids.reserve_at_least(order_id)
    }

    pub(crate) fn try_reserve_order_id_at_least(&self, order_id: i32) -> Option<i32> {
        self.ids.try_reserve_at_least(order_id)
    }

    /// Gets the current order ID without incrementing
    #[allow(dead_code)]
    pub(crate) fn current_order_id(&self) -> i32 {
        self.ids.current()
    }

    /// Gets the current request ID without incrementing
    #[allow(dead_code)]
    pub(crate) fn current_request_id(&self) -> i32 {
        self.ids.current()
    }
}

#[cfg(test)]
#[path = "id_generator_tests.rs"]
mod tests;
