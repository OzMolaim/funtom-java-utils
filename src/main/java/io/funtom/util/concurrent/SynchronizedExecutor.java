package io.funtom.util.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * An Executor which executes tasks on the caller thread.
 * The tasks will be executed synchronously, so no overlapping between two tasks running on different threads will ever occur.
 * Calling threads might be suspended.
 * Executing a task has the same memory semantics as locking and releasing a java.util.concurrent.locks.{@link Lock}.
 */
public final class SynchronizedExecutor {

    private final Lock lock;

    public SynchronizedExecutor() {
        this.lock = new ReentrantLock();
    }

    SynchronizedExecutor(Lock lock) {
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

    public <R> R execute(Supplier<R> task) {
        lock.lock();
        try {
            return task.get();
        } finally {
            lock.unlock();
        }
    }
}
