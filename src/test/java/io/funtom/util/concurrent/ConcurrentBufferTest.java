package io.funtom.util.concurrent;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
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
    public void getAndRemove() {
        ConcurrentBuffer<Integer> underTest = new ConcurrentBuffer<>();
        underTest.add(1);
        underTest.add(2);
        underTest.add(3);
        underTest.add(4);
        Assert.assertThat(Arrays.asList(1, 2), Matchers.equalTo(underTest.getAndRemove(2)));
        underTest.add(5);
        Assert.assertThat(underTest.getAndRemove(0), Matchers.equalTo(Collections.emptyList()));
        Assert.assertThat(underTest.getAndRemove(1), Matchers.equalTo(Collections.singletonList(3)));
        Assert.assertThat(underTest.getAndRemoveAll(), Matchers.equalTo(Arrays.asList(4, 5)));
    }

    @Test
    public void stressTheBuffer() throws InterruptedException, ExecutionException, BrokenBarrierException {
        try (BufferStressTester stress = new BufferStressTester(5, 30)) {
            stress.test();
        }
    }

    private void assertBufferContains(ConcurrentBuffer<Integer> underTest, Integer... ints) {
        assertBufferContains(underTest, Arrays.asList(ints));
    }

    private void assertBufferContains(ConcurrentBuffer<Integer> underTest, List<Integer> expected) {
        Assert.assertThat(underTest.getAndRemoveAll(), Matchers.equalTo(expected));
    }

    private class BufferStressTester implements AutoCloseable {

        final ConcurrentBuffer<Integer> underTest = new ConcurrentBuffer<>();
        final List<Integer> actualReadFromBuffer = new ArrayList<>();

        final int writesPerWriter = 5000;
        final int numberOfReaders;
        final int numberOfWriters;

        final ExecutorService pool;
        final CyclicBarrier startStopTaskBarrier;

        BufferStressTester(int readers, int writers) {
            this.numberOfReaders = readers;
            this.numberOfWriters = writers;
            this.startStopTaskBarrier = new CyclicBarrier(readers + writers);
            this.pool = Executors.newFixedThreadPool(readers + writers);
        }

        void test() throws ExecutionException, InterruptedException, BrokenBarrierException {

            for (int i = 0; i < numberOfWriters; i++) {
                final int elementToWrite = i;
                CompletableFuture.runAsync(() -> writerTask(elementToWrite), pool).exceptionally(
                        err -> {
                            throw new RuntimeException("Failure during writer task - " + elementToWrite, err);
                        }
                );
            }

            List<CompletableFuture<List<Integer>>> readers = new ArrayList<>();
            for (int i = 0; i < numberOfReaders; i++) {
                CompletableFuture<List<Integer>> r = CompletableFuture.supplyAsync(this::readerTask, pool).exceptionally(
                        err -> {
                            throw new RuntimeException("Failure during reader task", err);
                        }
                );
                readers.add(r);
            }

            for (CompletableFuture<List<Integer>> f : readers) {
                actualReadFromBuffer.addAll(f.get());
            }

            assertAllWritesWhereReadFromBuffer();
        }

        void assertAllWritesWhereReadFromBuffer() {
            Map<Integer, Long> elementToOccurrences = actualReadFromBuffer.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            Assert.assertEquals(IntStream.range(0, numberOfWriters).boxed().collect(Collectors.toSet()), elementToOccurrences.keySet());
            Assert.assertEquals(Arrays.asList((long) writesPerWriter), elementToOccurrences.values().stream().distinct().collect(Collectors.toList()));
        }

        void writerTask(final int elementToWrite) {
            try {
                tryWriterTask(elementToWrite);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        void tryWriterTask(final int elementToWrite) throws BrokenBarrierException, InterruptedException {
            try {
                startStopTaskBarrier.await();
                for (int i = 0; i < writesPerWriter; i++) {
                    underTest.add(elementToWrite);
                }
            } finally {
                startStopTaskBarrier.await();
            }
        }

        List<Integer> readerTask() {
            try {
                return tryReaderTask();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        List<Integer> tryReaderTask() throws BrokenBarrierException, InterruptedException {
            try {
                startStopTaskBarrier.await();
                List<Integer> result = new ArrayList<>();
                result.addAll(underTest.getAndRemove(writesPerWriter % 2));
                result.addAll(underTest.getAndRemoveAll());
                return result;
            } finally {
                startStopTaskBarrier.await();
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
