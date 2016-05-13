package io.funtom.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

public final class PerKeyLockExecutor<KEY_TYPE> {

	private final Map<KEY_TYPE, LockExecutor> executors = new HashMap<>();
	private final Map<KEY_TYPE, Integer> keyUsersCount = new HashMap<>();

	public void execute(KEY_TYPE key, Runnable task) {
		LockExecutor executor = getExecutorForKey(key);
		try {
			executor.execute(task);
		} finally {
			freeExecutorForKey(key);
		}
	}

	public <R> R submit(KEY_TYPE key, Callable<R> task) throws Exception {
		LockExecutor executor = getExecutorForKey(key);
		try {
			return executor.submit(task);
		} finally {
			freeExecutorForKey(key);
		}
	}

	public <R> R submitUnchecked(KEY_TYPE key, Callable<R> task) {
		LockExecutor executor = getExecutorForKey(key);
		try {
			return executor.submitUnchecked(task);
		} finally {
			freeExecutorForKey(key);
		}
	}

	private synchronized LockExecutor getExecutorForKey(KEY_TYPE key) {
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
