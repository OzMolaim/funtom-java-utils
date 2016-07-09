package io.funtom.util.concurrent;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * An Executor which executes tasks on the caller thread.
 * <p>
 * Calls to readExecute(...) methods:
 * <ul><li>Never lock each other</li>
 * <li>Have the same memory semantics as locking and unlocking the <b>read</b> lock of a java.util.concurrent.lock.{@link ReadWriteLock}</li></ul>
 * <p>
 * Calls to writeExecute(...) methods:
 * <ul><li>Never overlaps with any other calls to execute methods</li>
 * <li>Have the same memory semantics as locking and unlocking the <b>write</b> lock of a java.util.concurrent.lock.{@link ReadWriteLock}</li></ul>
 * <p>
 * Calling threads might be suspended.
 */
public final class ReadWriteSynchronizedExecutor {

    private final SynchronizedExecutor readExecutor;
    private final SynchronizedExecutor writeExecutor;

    public ReadWriteSynchronizedExecutor() {
        ReadWriteLock lock = new ReentrantReadWriteLock();
        readExecutor = new SynchronizedExecutor(lock.readLock());
        writeExecutor = new SynchronizedExecutor(lock.writeLock());
    }

    public void readExecute(Runnable task) {
        readExecutor.execute(task);
    }

    public <R> R readExecute(Supplier<R> task) {
        return readExecutor.execute(task);
    }

    public void writeExecute(Runnable task) {
        writeExecutor.execute(task);
    }

    public <R> R writeExecute(Supplier<R> task) {
        return writeExecutor.execute(task);
    }
}
