package io.funtom.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

public final class SynchronizedPerKeyExecutor<K> {

	private final Map<K, LockExecutor> executors = new HashMap<>();
	private final Map<K, Integer> keyUsersCount = new HashMap<>();

	public void execute(K key, Runnable task) {
		LockExecutor executor = getExecutorForKey(key);
		try {
			executor.execute(task);
		} finally {
			freeExecutorForKey(key);
		}
	}

	public <R> R submit(K key, Callable<R> task) throws Exception {
		LockExecutor executor = getExecutorForKey(key);
		try {
			return executor.submit(task);
		} finally {
			freeExecutorForKey(key);
		}
	}

	public <R> R submitUnchecked(K key, Callable<R> task) {
		LockExecutor executor = getExecutorForKey(key);
		try {
			return executor.submitUnchecked(task);
		} finally {
			freeExecutorForKey(key);
		}
	}

	private synchronized LockExecutor getExecutorForKey(K key) {
		LockExecutor result;
		Integer currentUsers = keyUsersCount.get(key);
		if (currentUsers == null) {
			keyUsersCount.put(key, 1);
			result = new LockExecutor(new ReentrantLock());
			executors.put(key, result);
		} else {
			keyUsersCount.put(key, currentUsers + 1);
			result = executors.get(key);
		}
		return result;
	}

	private synchronized void freeExecutorForKey(K key) {
		int currentUsers = keyUsersCount.get(key);
		if (currentUsers == 1) {
			keyUsersCount.remove(key);
			executors.remove(key);
		} else {
			keyUsersCount.put(key, currentUsers - 1);
		}
	}
}
