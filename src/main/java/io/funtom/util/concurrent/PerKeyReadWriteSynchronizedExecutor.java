package io.funtom.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public final class PerKeyReadWriteSynchronizedExecutor<KEY_TYPE> {

    private final Map<KEY_TYPE, ReadWriteSynchronizedExecutor> executors = new HashMap<>();
    private final Map<KEY_TYPE, Integer> keyUsersCount = new HashMap<>();

    public void readExecute(KEY_TYPE key, Runnable task) {
        ReadWriteSynchronizedExecutor executor = getExecutorForKey(key);
        try {
            executor.readExecute(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    public <R> R readExecute(KEY_TYPE key, Supplier<R> task) throws Exception {
        ReadWriteSynchronizedExecutor executor = getExecutorForKey(key);
        try {
            return executor.readExecute(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    public void writeExecute(KEY_TYPE key, Runnable task) {
        ReadWriteSynchronizedExecutor executor = getExecutorForKey(key);
        try {
            executor.writeExecute(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    public <R> R writeExecute(KEY_TYPE key, Supplier<R> task) {
        ReadWriteSynchronizedExecutor executor = getExecutorForKey(key);
        try {
            return executor.writeExecute(task);
        } finally {
            freeExecutorForKey(key);
        }
    }

    private synchronized ReadWriteSynchronizedExecutor getExecutorForKey(KEY_TYPE key) {
        ReadWriteSynchronizedExecutor result;
        Integer currentUsers = keyUsersCount.get(key);
        if (currentUsers == null) {
            keyUsersCount.put(key, 1);
            result = new ReadWriteSynchronizedExecutor();
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
