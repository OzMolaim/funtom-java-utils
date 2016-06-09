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
        int segmentIndex = HashUtil.boundedHash(key, CONCURRENCY_LEVEL);
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = concurrencySegments[segmentIndex];
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            executor.readExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public <R> R readExecute(KEY_TYPE key, Supplier<R> task) throws Exception {
        int segmentIndex = HashUtil.boundedHash(key, CONCURRENCY_LEVEL);
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = concurrencySegments[segmentIndex];
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            return executor.readExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public void writeExecute(KEY_TYPE key, Runnable task) {
        int segmentIndex = HashUtil.boundedHash(key, CONCURRENCY_LEVEL);
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = concurrencySegments[segmentIndex];
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            executor.writeExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public <R> R writeExecute(KEY_TYPE key, Supplier<R> task) {
        int segmentIndex = HashUtil.boundedHash(key, CONCURRENCY_LEVEL);
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = concurrencySegments[segmentIndex];
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            return executor.writeExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }
}
