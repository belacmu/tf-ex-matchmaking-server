use std::{net::SocketAddr, sync::Arc};

use tokio::sync::{Mutex, MutexGuard};

use super::quickplay::QuickplayQueued;

pub trait QueueEntry {
    fn addr(&self) -> SocketAddr;
    fn is_wide(&self) -> bool;
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct QueueCounts {
    pub standard: u32,
    pub wide: u32,
}

impl QueueCounts {
    pub fn of<T: QueueEntry>(entries: &[T]) -> Self {
        let wide = entries.iter().filter(|entry| entry.is_wide()).count() as u32;

        Self {
            standard: entries.len() as u32 - wide,
            wide,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Queue<T>(Arc<Mutex<Vec<T>>>);

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(vec![])))
    }
}

impl<T: QueueEntry> Queue<T> {
    pub async fn lock(&self) -> MutexGuard<'_, Vec<T>> {
        self.0.lock().await
    }

    pub async fn remove(&self, addr: SocketAddr) {
        self.0.lock().await.retain(|entry| entry.addr() != addr);
    }

    pub async fn counts(&self) -> QueueCounts {
        QueueCounts::of(&self.0.lock().await)
    }

    pub async fn take_all(&self) -> Vec<T> {
        self.0.lock().await.drain(..).collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Queues {
    pub quick_play: Queue<QuickplayQueued>,
}
