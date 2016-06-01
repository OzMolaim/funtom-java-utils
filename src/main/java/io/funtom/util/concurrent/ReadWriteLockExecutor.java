package io.funtom.util.concurrent;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public final class ReadWriteLockExecutor {

    private final LockExecutor readExecutor;
    private final LockExecutor writeExecutor;

    public ReadWriteLockExecutor() {
        ReadWriteLock lock = new ReentrantReadWriteLock();
        readExecutor = new LockExecutor(lock.readLock());
        writeExecutor = new LockExecutor(lock.writeLock());
    }

    public ReadWriteLockExecutor(ReadWriteLock lock) {
        readExecutor = new LockExecutor(lock.readLock());
        writeExecutor = new LockExecutor(lock.writeLock());
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
