use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

/// 64-byte cache-line aligned cache padding to prevent false sharing on SMP architectures.
#[repr(align(64))]
struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    pub const fn new(value: T) -> Self {
        Self { value }
    }
}

/// Single-Producer Single-Consumer (SPSC) lock-free ring buffer.
pub struct SpscRingBuffer<T, const CAP: usize> {
    buffer: [UnsafeCell<MaybeUninit<T>>; CAP],
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send, const CAP: usize> Sync for SpscRingBuffer<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Send for SpscRingBuffer<T, CAP> {}

impl<T, const CAP: usize> SpscRingBuffer<T, CAP> {
    pub fn new() -> Self {
        assert!(CAP > 0 && (CAP & (CAP - 1)) == 0, "Capacity must be a power of two");
        // Initialize uninitialized array
        let buffer = unsafe {
            let mut arr: [UnsafeCell<MaybeUninit<T>>; CAP] = MaybeUninit::uninit().assume_init();
            for item in &mut arr {
                std::ptr::write(item, UnsafeCell::new(MaybeUninit::uninit()));
            }
            arr
        };

        Self {
            buffer,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub fn push(&self, item: T) -> Result<(), T> {
        let head = self.head.value.load(Ordering::Relaxed);
        let tail = self.tail.value.load(Ordering::Acquire);

        if head.wrapping_sub(tail) >= CAP {
            return Err(item); // Buffer full
        }

        let slot = &self.buffer[head & (CAP - 1)];
        unsafe {
            (*slot.get()).write(item);
        }

        self.head.value.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        let tail = self.tail.value.load(Ordering::Relaxed);
        let head = self.head.value.load(Ordering::Acquire);

        if tail == head {
            return None; // Buffer empty
        }

        let slot = &self.buffer[tail & (CAP - 1)];
        let item = unsafe {
            (*slot.get()).assume_init_read()
        };

        self.tail.value.store(tail.wrapping_add(1), Ordering::Release);
        Some(item)
    }

    pub fn len(&self) -> usize {
        let head = self.head.value.load(Ordering::Relaxed);
        let tail = self.tail.value.load(Ordering::Relaxed);
        head.wrapping_sub(tail)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() >= CAP
    }
}

/// Multi-Producer Multi-Consumer (MPMC) lock-free bounded queue.
pub struct MpmcQueue<T> {
    buffer: Box<[Cell<T>]>,
    mask: usize,
    enqueue_pos: CachePadded<AtomicUsize>,
    dequeue_pos: CachePadded<AtomicUsize>,
}

struct Cell<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Sync for MpmcQueue<T> {}
unsafe impl<T: Send> Send for MpmcQueue<T> {}

impl<T> MpmcQueue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 1 && (capacity & (capacity - 1)) == 0, "Capacity must be power of two");
        let mut buffer = Vec::with_capacity(capacity);
        for i in 0..capacity {
            buffer.push(Cell {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        Self {
            buffer: buffer.into_boxed_slice(),
            mask: capacity - 1,
            enqueue_pos: CachePadded::new(AtomicUsize::new(0)),
            dequeue_pos: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    pub fn push(&self, data: T) -> Result<(), T> {
        let mut pos = self.enqueue_pos.value.load(Ordering::Relaxed);
        loop {
            let cell = &self.buffer[pos & self.mask];
            let seq = cell.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos as isize;

            if diff == 0 {
                if self.enqueue_pos.value.compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                    unsafe { (*cell.value.get()).write(data); }
                    cell.sequence.store(pos + 1, Ordering::Release);
                    return Ok(());
                }
            } else if diff < 0 {
                return Err(data); // Queue full
            } else {
                pos = self.enqueue_pos.value.load(Ordering::Relaxed);
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        let mut pos = self.dequeue_pos.value.load(Ordering::Relaxed);
        loop {
            let cell = &self.buffer[pos & self.mask];
            let seq = cell.sequence.load(Ordering::Acquire);
            let diff = seq as isize - (pos + 1) as isize;

            if diff == 0 {
                if self.dequeue_pos.value.compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                    let val = unsafe { (*cell.value.get()).assume_init_read() };
                    cell.sequence.store(pos + self.mask + 1, Ordering::Release);
                    return Some(val);
                }
            } else if diff < 0 {
                return None; // Queue empty
            } else {
                pos = self.dequeue_pos.value.load(Ordering::Relaxed);
            }
        }
    }
}
