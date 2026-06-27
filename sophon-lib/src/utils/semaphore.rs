//! A semaphore implementation to avoid pulling in an external dep just for that

use std::{
    num::NonZeroUsize,
    sync::{Condvar, Mutex},
};

pub struct Semaphore {
    state: Mutex<usize>,
    cond: Condvar,
}

impl Semaphore {
    pub fn new(permits: NonZeroUsize) -> Self {
        Self {
            state: Mutex::new(permits.get()),
            cond: Condvar::new(),
        }
    }

    pub fn acquire(&self, amount: usize) -> Permit<'_> {
        let mut count = self.state.lock().unwrap();
        count = self
            .cond
            .wait_while(count, |count| *count < amount)
            .unwrap();
        *count -= amount;
        Permit {
            semaphore: self,
            amount,
        }
    }

    fn release(&self, amount: usize) {
        let mut count = self.state.lock().unwrap();
        *count += amount;
        self.cond.notify_one();
    }
}

pub struct Permit<'a> {
    semaphore: &'a Semaphore,
    amount: usize,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.semaphore.release(self.amount);
    }
}
