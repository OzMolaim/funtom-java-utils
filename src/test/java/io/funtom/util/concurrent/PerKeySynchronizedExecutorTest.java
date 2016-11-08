package io.funtom.util.concurrent;

import io.funtom.util.concurrent.helper.MutableClass;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertTrue;

public class PerKeySynchronizedExecutorTest {

    @Test
    public void executeSimpleRunable() {
        PerKeySynchronizedExecutor<Integer> underTest = new PerKeySynchronizedExecutor<>();
        final AtomicBoolean bool = new AtomicBoolean(false);
        underTest.execute(1, () -> bool.set(true));
        assertTrue(bool.get());
    }

    @Test
    public void executeSimpleSupplier() {
        PerKeySynchronizedExecutor<Integer> underTest = new PerKeySynchronizedExecutor<>();
        boolean result = underTest.execute(1, () -> true);
        assertTrue(result);
    }

    @Test
    public void executeManyTasksForSameKey() throws InterruptedException {
        final PerKeySynchronizedExecutor<String> underTest = new PerKeySynchronizedExecutor<>();
        final int N = 10000;
        final CountDownLatch signal = new CountDownLatch(N);
        final List<Integer> actual = new ArrayList<>();
        final AtomicInteger seq = new AtomicInteger();

        final Runnable unsafeRunnable = () -> {
            actual.add(seq.incrementAndGet());
            signal.countDown();
        };

        final Supplier<Integer> unsafeSupplier = () -> {
            actual.add(seq.incrementAndGet());
            signal.countDown();
            return 1;
        };

        ExecutorService pool = Executors.newFixedThreadPool(50);
        for (int i = 0; i < N; i++) {
            if (i % 2 == 0) {
                pool.execute(() -> underTest.execute("KEY", unsafeRunnable));
            } else {
                pool.submit((() -> underTest.execute("KEY", unsafeSupplier)));
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
    public void executeMultipleTasksForMultipleKeys() throws InterruptedException {
        final PerKeySynchronizedExecutor<String> underTest = new PerKeySynchronizedExecutor<>();

        final int N = 10000;
        final CountDownLatch signal = new CountDownLatch(N * 2);
        final List<Long> actual1 = new ArrayList<>();
        final List<Long> actual2 = new ArrayList<>();

        final Runnable unsafeTask1 = () -> {
            actual1.add(System.currentTimeMillis());
            signal.countDown();
        };

        final Supplier<Long> unsafeTask2 = () -> {
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
            pool.submit(() -> underTest.execute("KEY2", unsafeTask2));
        }

        shutdownAndWait(signal, pool);

        Assert.assertEquals(N, actual1.size());
        Assert.assertEquals(N, actual2.size());
        assertTaskExecutedConcurrentlyForDiffrentKeys(actual1, actual2);
    }


    @Test
    public void stressTheExecutor() throws InterruptedException, BrokenBarrierException, ExecutionException {
        try (ExecutorStressTester tester = new ExecutorStressTester()) {
            tester.test();
        }
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
            assertTrue(msg, actual1.get(i - 1) <= actual1.get(i));
        }
    }

    private void shutdownAndWait(CountDownLatch signal, ExecutorService pool) throws InterruptedException {
        signal.await(30, TimeUnit.SECONDS);
        pool.shutdownNow();
        pool.awaitTermination(30, TimeUnit.SECONDS);
    }

    private class ExecutorStressTester implements AutoCloseable {

        private static final int NUMBER_OF_KEYS = 10;
        private static final int NUMBER_OF_THREADS = 400;
        private static final int EXECUTIONS_PER_THREAD = 1000;
        private static final int EXPECTED_MUTATIONS_PER_INSTANCE = NUMBER_OF_THREADS / NUMBER_OF_KEYS * EXECUTIONS_PER_THREAD;

        private final PerKeySynchronizedExecutor<Integer> underTest = new PerKeySynchronizedExecutor<>();
        private final MutableClass[] mutableInstances;

        private final CyclicBarrier startSignal;
        private final ExecutorService pool;

        ExecutorStressTester() {
            mutableInstances = new MutableClass[NUMBER_OF_KEYS];
            for (int i = 0; i < NUMBER_OF_KEYS; i++) {
                mutableInstances[i] = new MutableClass();
            }

            startSignal = new CyclicBarrier(NUMBER_OF_THREADS);
            this.pool = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        }

        void test() throws ExecutionException, InterruptedException, BrokenBarrierException {
            for (int i = 0; i < NUMBER_OF_THREADS; i++) {
                int key = i % NUMBER_OF_KEYS;
                if (i % 2 == 0) {
                    pool.execute(() -> executeRunnable(key));
                } else {
                    pool.execute(() -> executeSupplier(key));
                }
            }
        }

        @Override
        public void close() throws InterruptedException {
            if (pool != null) {
                pool.shutdown();
                if (!pool.awaitTermination(1, TimeUnit.MINUTES)) {
                    pool.shutdownNow();
                }
            }

            for (MutableClass instance : mutableInstances) {
                instance.assertConsistency();
                instance.assertNumberOfMutations(EXPECTED_MUTATIONS_PER_INSTANCE);
            }
        }

        void executeRunnable(final int key) {

            try {
                startSignal.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }

            for (int i = 0; i < EXECUTIONS_PER_THREAD; i++) {
                underTest.execute(key, () -> {
                    MutableClass mutableInstance = mutableInstances[key];
                    mutableInstance.assertConsistency();
                    mutableInstance.executeNonAtomicMutation();
                    mutableInstance.assertConsistency();
                });
            }
            ;
        }

        void executeSupplier(final int key) {
            try {
                startSignal.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }

            for (int i = 0; i < EXECUTIONS_PER_THREAD; i++) {
                underTest.execute(key, () -> {
                    MutableClass mutableInstance = mutableInstances[key];
                    mutableInstance.assertConsistency();
                    mutableInstance.executeNonAtomicMutation();
                    mutableInstance.assertConsistency();
                    return key;
                });
            }
            ;
        }
    }
}

