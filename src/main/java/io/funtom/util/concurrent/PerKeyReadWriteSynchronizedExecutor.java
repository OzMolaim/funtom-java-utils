package io.funtom.util.concurrent;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

/**
 * An Executor which executes tasks on the caller thread.
 * The tasks will be executed synchronously on a <b>per-key basis</b>.
 * By saying <b>per-key</b>, we mean that thread safety is guaranteed for threads calling it with equals keys.
 * For different threads calling the executor with equals keys,
 * <p>
 * Calls to readExecute(...) methods:
 * <ul><li>Never lock each other</li>
 * <li>Have the same memory semantics as locking and unlocking the <b>read</b> lock of a java.util.concurrent.lock.{@link ReadWriteLock}</li></ul>
 * <p>
 * Calls to writeExecute(...) methods:
 * <ul><li>Never overlaps with any other calls to execute methods</li>
 * <li>Have the same memory semantics as locking and unlocking the <b>write</b> lock of a java.util.concurrent.lock.{@link ReadWriteLock}</li></ul>
 * <p>
 * On the other hand, the executor is implemented so calls from different threads, with keys that are not equals, will be executed concurrently with minimal contention between the calls.
 * Calling threads might be suspended.
 */
public final class PerKeyReadWriteSynchronizedExecutor<KEY_TYPE> {

    private static final int CONCURRENCY_LEVEL = 32;

    private final ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor>[] concurrencySegments;

    @SuppressWarnings({"unchecked"})
    public PerKeyReadWriteSynchronizedExecutor() {
        concurrencySegments = (ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor>[]) new ConcurrencySegment[CONCURRENCY_LEVEL];
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            concurrencySegments[i] = new ConcurrencySegment<>(ReadWriteSynchronizedExecutor::new);
        }
    }

    public void readExecute(KEY_TYPE key, Runnable task) {
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = getSegment(key);
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            executor.readExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public <R> R readExecute(KEY_TYPE key, Supplier<R> task) {
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = getSegment(key);
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            return executor.readExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public void writeExecute(KEY_TYPE key, Runnable task) {
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = getSegment(key);
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            executor.writeExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    public <R> R writeExecute(KEY_TYPE key, Supplier<R> task) {
        ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> s = getSegment(key);
        ReadWriteSynchronizedExecutor executor = s.getValue(key);
        try {
            return executor.writeExecute(task);
        } finally {
            s.releaseKey(key);
        }
    }

    private ConcurrencySegment<KEY_TYPE, ReadWriteSynchronizedExecutor> getSegment(KEY_TYPE key) {
        int segmentIndex = HashUtil.boundedHash(key, CONCURRENCY_LEVEL);
        return concurrencySegments[segmentIndex];
    }
}
