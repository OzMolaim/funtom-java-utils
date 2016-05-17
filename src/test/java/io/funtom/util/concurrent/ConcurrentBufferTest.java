package io.funtom.util.concurrent;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConcurrentBufferTest {

    @Test
    public void bufferOneElement() {
        ConcurrentBuffer<Integer> underTest = new ConcurrentBuffer<>();
        underTest.add(1);
        assertBufferContains(underTest, 1);
        assertBufferContains(underTest);
    }

    @Test
    public void bufferCanBeResetAndKeepWorking() {
        ConcurrentBuffer<Integer> underTest = new ConcurrentBuffer<>();
        underTest.add(1);
        assertBufferContains(underTest, 1);
        assertBufferContains(underTest);
        underTest.addAll(Arrays.asList(3, 2, 1));
        underTest.add(0);
        assertBufferContains(underTest, 3, 2, 1, 0);
        assertBufferContains(underTest);
    }

    @Test
    public void stressTheBuffer() throws InterruptedException {
        try (BufferStressTester stress = new BufferStressTester(4, 26)) {
            stress.test();
        }
    }

    private void assertBufferContains(ConcurrentBuffer<Integer> underTest, Integer... ints) {
        assertBufferContains(underTest, Arrays.asList(ints));
    }

    private void assertBufferContains(ConcurrentBuffer<Integer> underTest, List<Integer> expected) {
        Assert.assertThat(expected, Matchers.equalTo(underTest.getAndRemoveAll()));
    }

    private class BufferStressTester implements AutoCloseable {

        final ConcurrentBuffer<Integer> underTest = new ConcurrentBuffer<>();
        final List<Integer> actualReadFromBuffer = new Vector<>();

        final long writesPerWriter = 250000;
        final int numberOfReaders;
        final int numberOfWriters;

        final ExecutorService pool;
        final CountDownLatch taskStartSignal;
        final CountDownLatch taskStopSignal;

        BufferStressTester(int readers, int writers) {
            this.numberOfReaders = readers;
            this.numberOfWriters = writers;
            this.taskStartSignal = new CountDownLatch(readers + writers);
            this.taskStopSignal = new CountDownLatch(readers + writers);
            this.pool = Executors.newFixedThreadPool(readers + writers);
        }

        void test() {

            for (int i = 0; i < numberOfWriters; i++) {
                final int elementToWrite = i;
                pool.execute(() -> writerTask(elementToWrite));
            }

            for (int i = 0; i < numberOfReaders; i++) {
                pool.submit(this::readerTask);
            }

            try {
                taskStopSignal.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            actualReadFromBuffer.addAll(underTest.getAndRemoveAll());
            assertAllWritesWhereReadFromBuffer();
        }

        void assertAllWritesWhereReadFromBuffer() {
            Map<Integer, Long> elementToOccurences = actualReadFromBuffer.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            Assert.assertEquals(IntStream.range(0, numberOfWriters).boxed().collect(Collectors.toSet()), elementToOccurences.keySet());
            Assert.assertEquals(Arrays.asList(writesPerWriter), elementToOccurences.values().stream().distinct().collect(Collectors.toList()));
        }

        void writerTask(final int elementToWrite) {
            try {

                taskStartSignal.countDown();
                taskStartSignal.await();

                for (int i = 0; i < writesPerWriter; i++) {
                    underTest.add(elementToWrite);
                }

            } catch (Exception e) {
                throw new RuntimeException("Writer task failed", e);
            } finally {
                taskStopSignal.countDown();
            }
        }

        void readerTask() {
            try {
                taskStartSignal.countDown();
                taskStartSignal.await();
                actualReadFromBuffer.addAll(underTest.getAndRemoveAll());
            } catch (Exception e) {
                throw new RuntimeException("Reader task failed", e);
            } finally {
                taskStopSignal.countDown();
            }
        }

        @Override
        public void close() throws InterruptedException {
            if (pool != null) {
                pool.shutdown();
                if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            }
        }
    }
}
