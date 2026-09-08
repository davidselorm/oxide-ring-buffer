#[cfg(test)]
mod tests {
    use oxide_ring_buffer::{SpscRingBuffer, MpmcQueue};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_spsc_fifo_ordering() {
        let rb: SpscRingBuffer<i32, 8> = SpscRingBuffer::new();
        assert!(rb.push(10).is_ok());
        assert!(rb.push(20).is_ok());
        assert!(rb.push(30).is_ok());

        assert_eq!(rb.pop(), Some(10));
        assert_eq!(rb.pop(), Some(20));
        assert_eq!(rb.pop(), Some(30));
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn test_mpmc_concurrent_throughput() {
        let queue = Arc::new(MpmcQueue::new(64));
        let q_clone = Arc::clone(&queue);

        let producer = thread::spawn(move || {
            for i in 0..1000 {
                while q_clone.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut sum = 0i64;
            for _ in 0..1000 {
                loop {
                    if let Some(val) = queue.pop() {
                        sum += val as i64;
                        break;
                    }
                    thread::yield_now();
                }
            }
            sum
        });

        producer.join().unwrap();
        let total = consumer.join().unwrap();
        assert_eq!(total, (0..1000).sum::<i64>());
    }
}
