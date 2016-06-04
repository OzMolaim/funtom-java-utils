package io.funtom.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public final class PerKeySynchronizedExecutor<KEY_TYPE> {

	private final Map<KEY_TYPE, SynchronizedExecutor> executors = new HashMap<>();
	private final Map<KEY_TYPE, Integer> keyUsersCount = new HashMap<>();

	public void execute(KEY_TYPE key, Runnable task) {
		SynchronizedExecutor executor = getExecutorForKey(key);
		try {
			executor.execute(task);
		} finally {
			freeExecutorForKey(key);
		}
	}

	public <R> R execute(KEY_TYPE key, Supplier<R> task) {
		SynchronizedExecutor executor = getExecutorForKey(key);
		try {
			return executor.execute(task);
		} finally {
			freeExecutorForKey(key);
		}
	}

	private synchronized SynchronizedExecutor getExecutorForKey(KEY_TYPE key) {
		SynchronizedExecutor result;
		Integer currentUsers = keyUsersCount.get(key);
		if (currentUsers == null) {
			keyUsersCount.put(key, 1);
			result = new SynchronizedExecutor();
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
