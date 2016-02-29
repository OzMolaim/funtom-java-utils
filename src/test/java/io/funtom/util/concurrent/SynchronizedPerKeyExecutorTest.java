package io.funtom.util.concurrent;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

public class SynchronizedPerKeyExecutorTest {

	@Test
	public void executeSimpleTask() {
		SynchronizedPerKeyExecutor<Integer> underTest = new SynchronizedPerKeyExecutor<>();

		final AtomicBoolean bool = new AtomicBoolean(false);
		underTest.execute(1, new Runnable() {
			@Override
			public void run() {
				bool.set(true);
			}
		});

		assertTrue(bool.get());
	}
	
	@Test
	public void submitSimpleTask() throws Exception {
		SynchronizedPerKeyExecutor<Integer> underTest = new SynchronizedPerKeyExecutor<>();

		boolean result = underTest.submit(1, new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				return true;
			}
		});
		
		assertTrue(result);
	}
	
	@Test
	public void submitUncheckedSimpleTask() throws Exception {
		SynchronizedPerKeyExecutor<Integer> underTest = new SynchronizedPerKeyExecutor<>();

		boolean result = underTest.submitUnchecked(1, new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				return true;
			}
		});
		
		assertTrue(result);
	}

	@Test(expected = SynchronizedPerKeyExecutor.UncheckedExecutionException.class)
	public void submitUncheckedThrowsOnException() throws Exception {
		SynchronizedPerKeyExecutor<Integer> underTest = new SynchronizedPerKeyExecutor<>();

		boolean result = underTest.submitUnchecked(1, new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				throw new Exception();
			}
		});
		
		assertTrue(result);
	}
	
	@Test
	public void executeManyTasksForSameKey() throws InterruptedException {
		final SynchronizedPerKeyExecutor<String> underTest = new SynchronizedPerKeyExecutor<>();

		final int N = 10000;
		final CountDownLatch signal = new CountDownLatch(N);
		final List<Integer> actual = new ArrayList<>();
		final AtomicInteger seq = new AtomicInteger();

		final Runnable unsafeTask = new Runnable() {
			@Override
			public void run() {
				actual.add(seq.incrementAndGet());
				signal.countDown();
			}
		};
		
		final Callable<Integer> unsafeTaskCallable = new Callable<Integer>() {
			@Override
			public Integer call() {
				actual.add(seq.incrementAndGet());
				signal.countDown();
				return 1;
			}
		};

		ExecutorService pool = Executors.newFixedThreadPool(50);
		for (int i = 0; i < N; i++) {
			if (i % 3 == 0) {
				pool.execute(new Runnable() {
					@Override
					public void run() {
						underTest.execute(new String("KEY"), unsafeTask);
					}
				});
			} else if (i % 3 == 1){
				pool.submit((new Callable<Integer>() {
					@Override
					public Integer call() throws Exception {
						return underTest.submit(new String("KEY"), unsafeTaskCallable);
					}
				}));
			}  else {
				pool.submit((new Callable<Integer>() {
					@Override
					public Integer call() throws Exception {
						return underTest.submitUnchecked(new String("KEY"), unsafeTaskCallable);
					}
				}));
			}
		}

		signal.await(30, TimeUnit.SECONDS);
		pool.shutdownNow();
		pool.awaitTermination(30, TimeUnit.SECONDS);
		
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
		
		final Callable<Long> unsafeTask2 = new Callable<Long>() {

			@Override
			public Long call() throws Exception {
				try {
					long res = System.currentTimeMillis();
					actual2.add(res);
					return res;
				} finally {
					signal.countDown();
				}
			}
		};

		ExecutorService pool = Executors.newFixedThreadPool(50);
		for (int i = 0; i < N; i++) {
			pool.execute(new Runnable() {
				@Override
				public void run() {
					underTest.execute(new String("KEY1"), unsafeTask1);
				}
			});
			pool.submit(new Callable<Long>() {
				@Override
				public Long call() throws Exception {
					return underTest.submit(new String("KEY2"), unsafeTask2);
				}
			});
		}

		signal.await(30, TimeUnit.SECONDS);
		pool.shutdownNow();
		pool.awaitTermination(30, TimeUnit.SECONDS);
		
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
}

