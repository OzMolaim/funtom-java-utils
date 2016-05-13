package io.funtom.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockExecutor {

    private final Lock lock;

    public LockExecutor() {
        this.lock = new ReentrantLock();
    }

    public LockExecutor(Lock lock) {
        this.lock = lock;
    }

    public void execute(Runnable task) {
        lock.lock();
        try {
            task.run();
        } finally {
            lock.unlock();
        }
    }

    public <R> R submit(Callable<R> task) throws Exception {
        lock.lock();
        try {
            return task.call();
        } finally {
            lock.unlock();
        }
    }

    public <R> R submitUnchecked(Callable<R> task) {
        try {
            return submit(task);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }
}
