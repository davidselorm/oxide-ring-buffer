use std::sync::atomic::{AtomicUsize, Ordering};

pub struct RingBuffer<T: Clone> {
    buffer: Vec<Option<T>>,
    capacity: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl<T: Clone> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(None);
        }
        Self { buffer, capacity, head: AtomicUsize::new(0), tail: AtomicUsize::new(0) }
    }
    pub fn try_push(&mut self, item: T) -> Result<(), &'static str> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if tail - head >= self.capacity {
            return Err("Buffer is full");
        }
        self.buffer[tail % self.capacity] = Some(item);
        self.tail.store(tail + 1, Ordering::Release);
        Ok(())
    }
}
