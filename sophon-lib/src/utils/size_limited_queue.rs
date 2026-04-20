use std::{sync::atomic::AtomicU64, time::Duration};

use crossbeam_channel::{Receiver, SendTimeoutError, Sender};

#[allow(clippy::type_complexity)]
pub fn new_size_limited<'a, L, T>(
    (sender, receiver): (Sender<(L, T)>, Receiver<(L, T)>),
    memory_limit: Option<(u64, &'a AtomicU64)>,
) -> (
    SizeLimitedQueueSender<'a, L, T>,
    SizeLimitedQueueReceiver<'a, L, T>,
) {
    (
        SizeLimitedQueueSender {
            sender,
            memory_limit,
        },
        SizeLimitedQueueReceiver {
            receiver,
            memory_limit,
        },
    )
}

/// Custom [Sender] wrapper that tracks how big the total size of enqueued chunks is
#[derive(Debug)]
pub struct SizeLimitedQueueSender<'a, L, T> {
    pub(crate) sender: Sender<(L, T)>,
    pub(crate) memory_limit: Option<(u64, &'a AtomicU64)>,
}

impl<L, T> Clone for SizeLimitedQueueSender<'_, L, T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            memory_limit: self.memory_limit,
        }
    }
}

impl<L: SizeLimitedQueueLocation, T: SizeLimitedQueuePayload> SizeLimitedQueueSender<'_, L, T> {
    /// The timeout is only used for the [Sender::send_timeout] call, ignored while waiting for
    /// "space" in queue
    pub fn send_timeout(
        &self,
        (loc, payload): (L, T),
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<(L, T)>> {
        let chunk_size = if self.memory_limit.is_some() {
            Some(loc.size().unwrap_or_else(|_| {
                // fails only in case of filesystem-backed chunk. Just try to read from chunk
                // info.
                payload.raw_size()
            }))
        } else {
            None
        };
        while !self.has_space(&loc, &payload) {
            std::thread::sleep(Duration::from_millis(50));
        }
        self.sender.send_timeout((loc, payload), timeout)?;
        if let Some((_, counter)) = self.memory_limit {
            counter.fetch_add(
                chunk_size.expect("self.memory_limit is Some, as checked earlier"),
                std::sync::atomic::Ordering::Release,
            );
        }
        Ok(())
    }

    fn has_space(&self, loc: &L, payload: &T) -> bool {
        // Prevent the queue choking on a chunk that is bigger than the limit.
        if self.sender.is_empty() {
            return true;
        }
        let Some((limit, counter)) = self.memory_limit else {
            return true;
        };
        let chunk_size = loc.size().unwrap_or_else(|_| {
            // fails only in case of filesystem-backed chunk. Just try to read from chunk
            // info.
            payload.raw_size()
        });
        let queue_size = counter.load(std::sync::atomic::Ordering::Acquire);
        queue_size + chunk_size <= limit
    }
}

/// Custom [Receiver] wrapper that tracks how big the total size of enqueued chunks is
#[derive(Debug)]
pub struct SizeLimitedQueueReceiver<'a, L, T> {
    pub(crate) receiver: Receiver<(L, T)>,
    pub(crate) memory_limit: Option<(u64, &'a AtomicU64)>,
}

impl<L, T> Clone for SizeLimitedQueueReceiver<'_, L, T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
            memory_limit: self.memory_limit,
        }
    }
}

impl<L: SizeLimitedQueueLocation, T: SizeLimitedQueuePayload> SizeLimitedQueueReceiver<'_, L, T> {
    pub fn recv(&self) -> Result<(L, T), crossbeam_channel::RecvError> {
        self.receiver.recv().map(|(loc, chunk_info)| {
            if let Some((_, counter)) = self.memory_limit {
                let chunk_size = loc.size().unwrap_or_else(|_| {
                    // fails only in case of filesystem-backed chunk. Just try to read from chunk
                    // info.
                    chunk_info.raw_size()
                });
                counter.fetch_sub(chunk_size, std::sync::atomic::Ordering::Release);
            }
            (loc, chunk_info)
        })
    }
}

pub trait SizeLimitedQueueLocation {
    fn size(&self) -> std::io::Result<u64>;
}

pub trait SizeLimitedQueuePayload {
    fn raw_size(&self) -> u64;
}
