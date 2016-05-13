package io.funtom.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLockExecutor {

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

    public <R> R readSubmit(Callable<R> task) throws Exception {
        return readExecutor.submit(task);
    }

    public <R> R readSubmitUnchecked(Callable<R> task) {
        return readExecutor.submitUnchecked(task);
    }

    public void writeExecute(Runnable task) {
        writeExecutor.execute(task);
    }

    public <R> R writeSubmit(Callable<R> task) throws Exception {
        return writeExecutor.submit(task);
    }

    public <R> R writeSubmitUnchecked(Callable<R> task) {
        return writeExecutor.submitUnchecked(task);
    }
}
