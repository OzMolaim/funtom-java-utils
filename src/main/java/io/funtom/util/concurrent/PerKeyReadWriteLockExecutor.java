package io.funtom.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class PerKeyReadWriteLockExecutor<KEY_TYPE> {

    private final Map<KEY_TYPE, ReadWriteLockExecutor> executors = new HashMap<>();
    private final Map<KEY_TYPE, Integer> keyUsersCount = new HashMap<>();

    public void readExecute(KEY_TYPE key, Runnable task) {
        ReadWriteLockExecutor executor = getExecutorForKey(key);
        try {
            executor.readExecute(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    public <R> R readSubmit(KEY_TYPE key, Callable<R> task) throws Exception {
        ReadWriteLockExecutor executor = getExecutorForKey(key);
        try {
            return executor.readSubmit(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    public <R> R readSubmitUnchecked(KEY_TYPE key, Callable<R> task) {
        ReadWriteLockExecutor executor = getExecutorForKey(key);
        try {
            return executor.readSubmitUnchecked(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    public void writeExecute(KEY_TYPE key, Runnable task) {
        ReadWriteLockExecutor executor = getExecutorForKey(key);
        try {
            executor.writeExecute(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    public <R> R writeSubmit(KEY_TYPE key, Callable<R> task) throws Exception {
        ReadWriteLockExecutor executor = getExecutorForKey(key);
        try {
            return executor.writeSubmit(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    public <R> R writeSubmitUnchecked(KEY_TYPE key, Callable<R> task) {
        ReadWriteLockExecutor executor = getExecutorForKey(key);
        try {
            return executor.writeSubmitUnchecked(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    private synchronized ReadWriteLockExecutor getExecutorForKey(KEY_TYPE key) {
        ReadWriteLockExecutor result;
        Integer currentUsers = keyUsersCount.get(key);
        if (currentUsers == null) {
            keyUsersCount.put(key, 1);
            result = new ReadWriteLockExecutor(new ReentrantReadWriteLock());
            executors.put(key, result);
        } else {
            keyUsersCount.put(key, currentUsers + 1);
            result = executors.get(key);
        }
        return result;
    }

    private synchronized void freeExecutorForKey(KEY_TYPE key) {
        int currentUsers = keyUsersCount.get(key);
        if (currentUsers == 1) {
            keyUsersCount.remove(key);
            executors.remove(key);
        } else {
            keyUsersCount.put(key, currentUsers - 1);
        }
    }
}
