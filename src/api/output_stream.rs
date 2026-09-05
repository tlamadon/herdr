//! In-process handoff of raw output subscriptions from the app thread to the
//! blocking `pane.stream_output` socket handler.
//!
//! The API response channel carries JSON strings, so the app-side open handler
//! stashes the live `PaneOutputSubscription` here under a server-generated
//! token and returns a plain success response. The socket handler claims the
//! stash after the dispatch completes. Entries are evicted by age so a handler
//! that dies between dispatch and claim cannot leak a subscription.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::pane::PaneOutputSubscription;

const STASH_MAX_AGE: Duration = Duration::from_secs(60);

static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);

pub(crate) struct PendingOutputStream {
    pub subscription: PaneOutputSubscription,
    /// App-runtime handle so the blocking handler can await the broadcast
    /// receiver with a timeout instead of spin-polling.
    pub runtime: tokio::runtime::Handle,
    stashed_at: Instant,
}

fn pending() -> &'static Mutex<HashMap<u64, PendingOutputStream>> {
    static PENDING: OnceLock<Mutex<HashMap<u64, PendingOutputStream>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn next_token() -> u64 {
    NEXT_TOKEN.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn stash(token: u64, subscription: PaneOutputSubscription) {
    let entry = PendingOutputStream {
        subscription,
        runtime: tokio::runtime::Handle::current(),
        stashed_at: Instant::now(),
    };
    let mut pending = match pending().lock() {
        Ok(pending) => pending,
        Err(poisoned) => poisoned.into_inner(),
    };
    let now = Instant::now();
    pending.retain(|_, entry| now.duration_since(entry.stashed_at) < STASH_MAX_AGE);
    pending.insert(token, entry);
}

pub(crate) fn take(token: u64) -> Option<PendingOutputStream> {
    let mut pending = match pending().lock() {
        Ok(pending) => pending,
        Err(poisoned) => poisoned.into_inner(),
    };
    pending.remove(&token)
}
