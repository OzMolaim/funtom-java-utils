package io.funtom.util.concurrent;

import java.util.function.Supplier;

public final class PerKeySynchronizedExecutor<KEY_TYPE> {

    private static final int CONCURRENCY_LEVEL = 32;

    private final ConcurrencySegment<KEY_TYPE, SynchronizedExecutor>[] concurrencySegments;

    @SuppressWarnings("unchecked")
    public PerKeySynchronizedExecutor() {
        concurrencySegments = (ConcurrencySegment<KEY_TYPE, SynchronizedExecutor>[])new ConcurrencySegment[CONCURRENCY_LEVEL];
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            concurrencySegments[i] = new ConcurrencySegment<>(SynchronizedExecutor::new);
        }
    }

	public void execute(KEY_TYPE key, Runnable task) {
        ConcurrencySegment<KEY_TYPE, SynchronizedExecutor> s = concurrencySegments[segmentIndex(key)];
		SynchronizedExecutor executor = s.getValue(key);
		try {
			executor.execute(task);
		} finally {
			s.releaseKey(key);
		}
	}

	public <R> R execute(KEY_TYPE key, Supplier<R> task) {
        ConcurrencySegment<KEY_TYPE, SynchronizedExecutor> s = concurrencySegments[segmentIndex(key)];
        SynchronizedExecutor executor = s.getValue(key);
		try {
			return executor.execute(task);
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
