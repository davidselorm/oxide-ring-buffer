# oxide-ring-buffer

Ultra-low-latency, zero-allocation lock-free ring buffer and concurrent queue library in Rust.

## Architecture
- **SpscRingBuffer<T, CAP>**: Bounded single-producer single-consumer circular buffer with 64-byte `#[repr(align(64))]` false-sharing prevention.
- **MpmcQueue<T>**: Bounded multi-producer multi-consumer queue using atomic sequence tracking and memory barriers.
- **Zero Heap Allocations in Hot Paths**: Ring slots use static array buffers or flat contiguous slices.

## Usage
```rust
use oxide_ring_buffer::SpscRingBuffer;

let ring: SpscRingBuffer<u64, 1024> = SpscRingBuffer::new();
ring.push(42).expect("Buffer full");
assert_eq!(ring.pop(), Some(42));
```
