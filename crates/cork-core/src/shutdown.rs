use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownMode {
    Cancel,
    Drain,
}

#[derive(Debug, Clone)]
pub struct ShutdownPolicy {
    pub mode: ShutdownMode,
    pub drain_timeout: Duration,
}

impl Default for ShutdownPolicy {
    fn default() -> Self {
        Self {
            mode: ShutdownMode::Drain,
            drain_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default)]
pub struct ShutdownState {
    is_shutting_down: AtomicBool,
    notify: Notify,
}

impl ShutdownState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn begin(&self) -> bool {
        if self
            .is_shutting_down
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.notify.notify_waiters();
            true
        } else {
            false
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::SeqCst)
    }

    pub async fn wait(&self) {
        if self.is_shutting_down() {
            return;
        }
        self.notify.notified().await;
    }
}
