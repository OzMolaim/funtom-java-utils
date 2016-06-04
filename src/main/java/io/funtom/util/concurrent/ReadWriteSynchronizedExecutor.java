package io.funtom.util.concurrent;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public final class ReadWriteSynchronizedExecutor {

    private final SynchronizedExecutor readExecutor;
    private final SynchronizedExecutor writeExecutor;

    public ReadWriteSynchronizedExecutor() {
        ReadWriteLock lock = new ReentrantReadWriteLock();
        readExecutor = new SynchronizedExecutor(lock.readLock());
        writeExecutor = new SynchronizedExecutor(lock.writeLock());
    }

    public ReadWriteSynchronizedExecutor(ReadWriteLock lock) {
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
