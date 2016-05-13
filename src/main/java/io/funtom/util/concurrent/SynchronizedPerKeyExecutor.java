package io.funtom.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class SynchronizedPerKeyExecutor<K> {

	private final Map<K, Lock> locks = new HashMap<>();
	private final Map<K, Integer> keyUsersCount = new HashMap<>();

	public void execute(K key, Runnable task) {
		Lock lock = getLockForKey(key);
		lock.lock();
		try {
			task.run();
		} finally {
			lock.unlock();
			freeLockForKey(key);
		}
	}

	public <R> R submit(K key, Callable<R> task) throws Exception {
		Lock lock = getLockForKey(key);
		lock.lock();
		try {
			return task.call();
		} finally {
			lock.unlock();
			freeLockForKey(key);
		}
	}

	public <R> R submitUnchecked(K key, Callable<R> task) {
		try {
			return submit(key, task);
		} catch (Exception e) {
			throw new UncheckedExecutionException(e);
		}
	}

	private synchronized Lock getLockForKey(K key) {
		Lock result;
		Integer currentUsers = keyUsersCount.get(key);
		if (currentUsers == null) {
			keyUsersCount.put(key, 1);
			result = new ReentrantLock();
			locks.put(key, result);
		} else {
			keyUsersCount.put(key, currentUsers + 1);
			result = locks.get(key);
		}
		return result;
	}

	private synchronized void freeLockForKey(K key) {
		int currentUsers = keyUsersCount.get(key);
		if (currentUsers == 1) {
			keyUsersCount.remove(key);
			locks.remove(key);
		} else {
			keyUsersCount.put(key, currentUsers - 1);
		}
	}

	public static class UncheckedExecutionException extends RuntimeException {

		private static final long serialVersionUID = -9113509948641626834L;

		UncheckedExecutionException(Throwable cause) {
			super("Exception during task execution", cause);
		}
	}
}
