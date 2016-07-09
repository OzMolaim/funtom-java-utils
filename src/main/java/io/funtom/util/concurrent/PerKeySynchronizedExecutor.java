package io.funtom.util.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * An Executor which executes tasks on the caller thread.
 * The tasks will be executed synchronously on a <b>per-key basis</b>.
 * By saying <b>per-key</b>, we mean that thread safety is guaranteed for threads calling it with equals keys.
 * When two threads calling the executor with equals keys, the executions will never overlap each other.
 * On the other hand, the executor is implemented so calls from different threads, with keys that are not equals, will be executed concurrently with minimal contention between the calls.
 * Calling threads might be suspended.
 * Calling execute from different thread with equals keys has the same memory semantics as locking and releasing a java.util.concurrent.locks.{@link Lock}.
 */
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
