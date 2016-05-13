package io.funtom.util.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class SynchronizedPerKeyExecutorTest {

	@Test
	public void executeSimpleTask() {
		SynchronizedPerKeyExecutor<Integer> underTest = new SynchronizedPerKeyExecutor<>();
		final AtomicBoolean bool = new AtomicBoolean(false);
		underTest.execute(1, () -> bool.set(true));
		assertTrue(bool.get());
	}
	
	@Test
	public void submitSimpleTask() throws Exception {
		SynchronizedPerKeyExecutor<Integer> underTest = new SynchronizedPerKeyExecutor<>();
		boolean result = underTest.submit(1, () -> true);
		assertTrue(result);
	}
	
	@Test
	public void submitUncheckedSimpleTask() throws Exception {
		SynchronizedPerKeyExecutor<Integer> underTest = new SynchronizedPerKeyExecutor<>();
		boolean result = underTest.submitUnchecked(1, () -> true);
		assertTrue(result);
	}

	@Test(expected = UncheckedExecutionException.class)
	public void submitUncheckedThrowsOnException() throws Exception {
		SynchronizedPerKeyExecutor<Integer> underTest = new SynchronizedPerKeyExecutor<>();
		boolean result = underTest.submitUnchecked(1, () -> {throw new Exception();});
		assertTrue(result);
	}
	
	@Test
	public void executeManyTasksForSameKey() throws InterruptedException {
		final SynchronizedPerKeyExecutor<String> underTest = new SynchronizedPerKeyExecutor<>();
		final int N = 10000;
		final CountDownLatch signal = new CountDownLatch(N);
		final List<Integer> actual = new ArrayList<>();
		final AtomicInteger seq = new AtomicInteger();

		final Runnable unsafeTask = () -> {
			actual.add(seq.incrementAndGet());
			signal.countDown();
		};
		
		final Callable<Integer> unsafeTaskCallable = () -> {
			actual.add(seq.incrementAndGet());
			signal.countDown();
			return 1;
		};

		ExecutorService pool = Executors.newFixedThreadPool(50);
		for (int i = 0; i < N; i++) {
			if (i % 3 == 0) {
				pool.execute(() -> underTest.execute("KEY", unsafeTask));
			} else if (i % 3 == 1){
				pool.submit((() -> underTest.submit("KEY", unsafeTaskCallable)));
			}  else {
				pool.submit((() -> underTest.submitUnchecked("KEY", unsafeTaskCallable)));
			}
		}

		shutdownAndWait(signal, pool);
		
		List<Integer> expected = new ArrayList<>();
		for (int i = 1; i <= N; i++) {
			expected.add(i);
		}

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void executeAndSubmitMultipleTasksForMultipleKeys() throws InterruptedException {
		final SynchronizedPerKeyExecutor<String> underTest = new SynchronizedPerKeyExecutor<>();

		final int N = 10000;
		final CountDownLatch signal = new CountDownLatch(N * 2);
		final List<Long> actual1 = new ArrayList<>();
		final List<Long> actual2 = new ArrayList<>();

		final Runnable unsafeTask1 = new Runnable() {
			@Override
			public void run() {
				actual1.add(System.currentTimeMillis());
				signal.countDown();
			}
		};
		
		final Callable<Long> unsafeTask2 = () -> {
			try {
				long res = System.currentTimeMillis();
				actual2.add(res);
				return res;
			} finally {
				signal.countDown();
			}
		};

		ExecutorService pool = Executors.newFixedThreadPool(50);
		for (int i = 0; i < N; i++) {
			pool.execute(() -> underTest.execute("KEY1", unsafeTask1));
			pool.submit(() -> underTest.submit("KEY2", unsafeTask2));
		}

		shutdownAndWait(signal, pool);
		
		Assert.assertEquals(N, actual1.size());
		Assert.assertEquals(N, actual2.size());
		assertTaskExecutedConcurrentlyForDiffrentKeys(actual1, actual2);
	}

	private void assertTaskExecutedConcurrentlyForDiffrentKeys(final List<Long> actual1, final List<Long> actual2) {
		assertAscendingOrder(actual1);
		assertAscendingOrder(actual2);
		Assert.assertTrue(Collections.min(actual2) < Collections.max(actual1));
		Assert.assertTrue(Collections.min(actual1) < Collections.max(actual2));
	}

	private void assertAscendingOrder(List<Long> actual1) {
		for (int i = 1; i < actual1.size(); i++) {
			String msg = "Element: " + i;
			assertTrue(msg, actual1.get(i-1) <= actual1.get(i));
		}
	}

	private void shutdownAndWait(CountDownLatch signal, ExecutorService pool) throws InterruptedException {
		signal.await(30, TimeUnit.SECONDS);
		pool.shutdownNow();
		pool.awaitTermination(30, TimeUnit.SECONDS);
	}
}

