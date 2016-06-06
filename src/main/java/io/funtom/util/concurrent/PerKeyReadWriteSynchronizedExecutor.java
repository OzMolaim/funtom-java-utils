package io.funtom.util.concurrent;

import java.util.function.Supplier;

public final class PerKeyReadWriteSynchronizedExecutor<KEY_TYPE> {

    private static final int CONCURRENCY_LEVEL = 32;

    private final ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor>[] concurrencySegments;

    @SuppressWarnings({"unchecked"})
    public PerKeyReadWriteSynchronizedExecutor() {
        concurrencySegments = (ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor>[])new ConcurrencySegment[CONCURRENCY_LEVEL];
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            concurrencySegments[i] = new ConcurrencySegment<>(ReadWriteSynchronizedExecutor::new);
        }
    }

    public void readExecute(KEY_TYPE key, Runnable task) {
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = concurrencySegments[segmentIndex(key)];
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            executor.readExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public <R> R readExecute(KEY_TYPE key, Supplier<R> task) throws Exception {
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = concurrencySegments[segmentIndex(key)];
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            return executor.readExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public void writeExecute(KEY_TYPE key, Runnable task) {
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = concurrencySegments[segmentIndex(key)];
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            executor.writeExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public <R> R writeExecute(KEY_TYPE key, Supplier<R> task) {
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = concurrencySegments[segmentIndex(key)];
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            return executor.writeExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    private int segmentIndex(KEY_TYPE key) {
        int h = key.hashCode();

        // Protection against poor hash functions.
        // Used by java.util.concurrent.ConcurrentHashMap
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        h ^= (h >>> 16);

        return Math.abs(h % CONCURRENCY_LEVEL);
    }
}
