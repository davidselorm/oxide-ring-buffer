use std::sync::atomic::{AtomicUsize, Ordering};

pub struct RingBuffer<T> {
    buffer: Vec<Option<T>>,
    capacity: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(None);
        }
        Self { buffer, capacity, head: AtomicUsize::new(0), tail: AtomicUsize::new(0) }
    }
    pub fn len(&self) -> usize {
        self.tail.load(Ordering::Relaxed) - self.head.load(Ordering::Relaxed)
    }
}
