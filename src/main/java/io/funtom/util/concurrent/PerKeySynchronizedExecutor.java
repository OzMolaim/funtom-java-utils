package io.funtom.util.concurrent;

import java.util.function.Supplier;

public final class PerKeySynchronizedExecutor<KEY_TYPE> {

    private static final int CONCURRENCY_LEVEL = 32;

    private final ConcurrencySegment<KEY_TYPE, SynchronizedExecutor>[] segments;

    @SuppressWarnings({"unchecked"})
    public PerKeySynchronizedExecutor() {
        segments = (ConcurrencySegment<KEY_TYPE, SynchronizedExecutor>[]) new ConcurrencySegment[CONCURRENCY_LEVEL];
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            segments[i] = new ConcurrencySegment<>(SynchronizedExecutor::new);
        }
    }

    public void execute(KEY_TYPE key, Runnable task) {
        int segmentIndex = HashUtil.boundedHash(key, CONCURRENCY_LEVEL);
        ConcurrencySegment<KEY_TYPE, SynchronizedExecutor> s = segments[segmentIndex];
        SynchronizedExecutor executor = s.getValue(key);
        try {
            executor.execute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public <R> R execute(KEY_TYPE key, Supplier<R> task) {
        int segmentIndex = HashUtil.boundedHash(key, CONCURRENCY_LEVEL);
        ConcurrencySegment<KEY_TYPE, SynchronizedExecutor> s = segments[segmentIndex];
        SynchronizedExecutor executor = s.getValue(key);
        try {
            return executor.execute(task);
        } finally {
            s.releaseKey(key);
        }
    }
}
