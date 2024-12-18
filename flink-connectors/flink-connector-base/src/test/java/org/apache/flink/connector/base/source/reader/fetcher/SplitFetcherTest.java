/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;
import org.apache.flink.connector.base.source.reader.mocks.TestingSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.TestingSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.util.ExceptionUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.State.WAITING;
import static org.apache.flink.test.util.TestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link SplitFetcher}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class SplitFetcherTest {

    @Test
    void testNewFetcherIsIdle() {
        final SplitFetcher<Object, TestingSourceSplit> fetcher =
                createFetcher(new TestingSplitReader<>());
        assertThat(fetcher.isIdle()).isTrue();
    }

    @Test
    void testFetcherNotIdleAfterSplitAdded() {
        final SplitFetcher<Object, TestingSourceSplit> fetcher =
                createFetcher(new TestingSplitReader<>());
        final TestingSourceSplit split = new TestingSourceSplit("test-split");

        fetcher.addSplits(Collections.singletonList(split));

        assertThat(fetcher.isIdle()).isFalse();

        // need to loop here because the internal wakeup flag handling means we need multiple loops
        while (fetcher.assignedSplits().isEmpty()) {
            fetcher.runOnce();
            assertThat(fetcher.isIdle()).isFalse();
        }
    }

    @Test
    void testIdleAfterFinishedSplitsEnqueued() {
        final SplitFetcher<Object, TestingSourceSplit> fetcher =
                createFetcherWithSplit(
                        "test-split", new TestingSplitReader<>(finishedSplitFetch("test-split")));

        fetcher.runOnce();

        assertThat(fetcher.assignedSplits()).isEmpty();
        assertThat(fetcher.isIdle()).isTrue();
    }

    @Test
    void testNotifiesWhenGoingIdle() {
        final FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue =
                new FutureCompletingBlockingQueue<>();
        final SplitFetcher<Object, TestingSourceSplit> fetcher =
                createFetcherWithSplit(
                        "test-split",
                        queue,
                        new TestingSplitReader<>(finishedSplitFetch("test-split")));

        fetcher.runOnce();

        assertThat(fetcher.assignedSplits()).isEmpty();
        assertThat(fetcher.isIdle()).isTrue();
        assertThat(queue.getAvailabilityFuture().isDone()).isTrue();
    }

    @Test
    void testNotifiesOlderFutureWhenGoingIdle() {
        final FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue =
                new FutureCompletingBlockingQueue<>();
        final SplitFetcher<Object, TestingSourceSplit> fetcher =
                createFetcherWithSplit(
                        "test-split",
                        queue,
                        new TestingSplitReader<>(finishedSplitFetch("test-split")));

        final CompletableFuture<?> future = queue.getAvailabilityFuture();

        fetcher.runOnce();

        assertThat(fetcher.assignedSplits()).isEmpty();
        assertThat(fetcher.isIdle()).isTrue();
        assertThat(future.isDone()).isTrue();
    }

    @Test
    void testNotifiesWhenGoingIdleConcurrent() throws Exception {
        final FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue =
                new FutureCompletingBlockingQueue<>();
        final SplitFetcher<Object, TestingSourceSplit> fetcher =
                createFetcherWithSplit(
                        "test-split",
                        queue,
                        new TestingSplitReader<>(finishedSplitFetch("test-split")));

        final QueueDrainerThread queueDrainer = new QueueDrainerThread(queue, fetcher, 1);
        queueDrainer.start();

        fetcher.runOnce();

        queueDrainer.sync();

        // either we got the notification that the fetcher went idle after the queue was drained
        // (thread finished)
        // or the fetcher was already idle when the thread drained the queue (then we need no
        // additional notification)
        assertThat(queue.getAvailabilityFuture().isDone() || queueDrainer.wasIdleWhenFinished())
                .isTrue();
    }

    @Test
    void testNotifiesOlderFutureWhenGoingIdleConcurrent() throws Exception {
        final FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue =
                new FutureCompletingBlockingQueue<>();
        final SplitFetcher<Object, TestingSourceSplit> fetcher =
                createFetcherWithSplit(
                        "test-split",
                        queue,
                        new TestingSplitReader<>(finishedSplitFetch("test-split")));

        final QueueDrainerThread queueDrainer = new QueueDrainerThread(queue, fetcher, 1);
        queueDrainer.start();

        final CompletableFuture<?> future = queue.getAvailabilityFuture();

        fetcher.runOnce();
        assertThat(future.isDone()).isTrue();

        queueDrainer.sync();
    }

    @Test
    void testWakeup() throws InterruptedException {
        final int numSplits = 3;
        final int numRecordsPerSplit = 10_000;
        final int wakeupRecordsInterval = 10;
        final int numTotalRecords = numRecordsPerSplit * numSplits;

        FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementQueue =
                new FutureCompletingBlockingQueue<>(1);
        SplitFetcher<int[], MockSourceSplit> fetcher =
                new SplitFetcher<>(
                        0,
                        elementQueue,
                        MockSplitReader.newBuilder()
                                .setNumRecordsPerSplitPerFetch(2)
                                .setBlockingFetch(true)
                                .build(),
                        ExceptionUtils::rethrow,
                        () -> {},
                        (ignore) -> {},
                        false);

        // Prepare the splits.
        List<MockSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(new MockSourceSplit(i, 0, numRecordsPerSplit));
            int base = i * numRecordsPerSplit;
            for (int j = base; j < base + numRecordsPerSplit; j++) {
                splits.get(splits.size() - 1).addRecord(j);
            }
        }
        // Add splits to the fetcher.
        fetcher.addSplits(splits);

        // A thread drives the fetcher.
        Thread fetcherThread = new Thread(fetcher, "FetcherThread");

        SortedSet<Integer> recordsRead = Collections.synchronizedSortedSet(new TreeSet<>());

        // A thread waking up the split fetcher frequently.
        AtomicInteger wakeupTimes = new AtomicInteger(0);
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread wakeUpCaller =
                new Thread("Wakeup Caller") {
                    @Override
                    public void run() {
                        int lastWakeup = 0;
                        while (recordsRead.size() < numTotalRecords && !stop.get()) {
                            int numRecordsRead = recordsRead.size();
                            if (numRecordsRead >= lastWakeup + wakeupRecordsInterval) {
                                fetcher.wakeUp(false);
                                wakeupTimes.incrementAndGet();
                                lastWakeup = numRecordsRead;
                            }
                        }
                    }
                };

        try {
            fetcherThread.start();
            wakeUpCaller.start();

            while (recordsRead.size() < numSplits * numRecordsPerSplit) {
                final RecordsWithSplitIds<int[]> nextBatch = elementQueue.take();
                while (nextBatch.nextSplit() != null) {
                    int[] arr;
                    while ((arr = nextBatch.nextRecordFromSplit()) != null) {
                        assertThat(recordsRead.add(arr[0])).isTrue();
                    }
                }
            }

            assertThat(recordsRead).hasSize(numTotalRecords);
            assertThat(recordsRead.first()).isEqualTo(0);
            assertThat(recordsRead.last()).isEqualTo(numTotalRecords - 1);
            assertThat(wakeupTimes.get()).isGreaterThan(0);
        } finally {
            stop.set(true);
            fetcher.shutdown();
            fetcherThread.join();
            wakeUpCaller.join();
        }
    }

    @Test
    void testClose() {
        TestingSplitReader<Object, TestingSourceSplit> splitReader = new TestingSplitReader<>();
        final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcher(splitReader);
        fetcher.shutdown();
        fetcher.run();
        assertThat(splitReader.isClosed()).isTrue();
    }

    @Test
    void testCloseAfterPause() throws InterruptedException {
        final FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue =
                new FutureCompletingBlockingQueue<>();
        final SplitFetcher<Object, TestingSourceSplit> fetcher =
                createFetcherWithSplit(
                        "test-split",
                        queue,
                        new TestingSplitReader<>(finishedSplitFetch("test-split")));

        fetcher.pause();

        Thread fetcherThread = new Thread(fetcher::shutdown);
        fetcherThread.start();
        fetcherThread.join();

        assertThat(fetcher.runOnce()).isFalse();
    }

    @Test
    void testShutdownWaitingForRecordsProcessing() throws Exception {
        TestingSplitReader<Object, TestingSourceSplit> splitReader = new TestingSplitReader<>();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue =
                new FutureCompletingBlockingQueue<>();
        final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcher(splitReader, queue);
        fetcher.shutdown(true);

        // Spawn a new fetcher thread to go through the shutdown sequence.
        CheckedThread fetcherThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        fetcher.run();
                        assertThat(splitReader.isClosed()).isTrue();
                    }
                };
        fetcherThread.start();

        // Wait until the fetcher thread to block on the shutdown latch.
        waitUntil(
                () -> fetcherThread.getState() == WAITING,
                Duration.ofSeconds(1),
                "The fetcher thread should be waiting for the shutdown latch");
        assertThat(splitReader.isClosed())
                .as("The split reader should have not been closed.")
                .isFalse();

        queue.getAvailabilityFuture().thenRun(() -> queue.poll().recycle());
        // Now pull the latch.
        fetcherThread.sync();
    }

    // ------------------------------------------------------------------------
    //  testing utils
    // ------------------------------------------------------------------------

    private static <E> RecordsBySplits<E> finishedSplitFetch(String splitId) {
        return new RecordsBySplits<>(Collections.emptyMap(), Collections.singleton(splitId));
    }

    private static <E> SplitFetcher<E, TestingSourceSplit> createFetcher(
            final SplitReader<E, TestingSourceSplit> reader) {
        return createFetcher(reader, new FutureCompletingBlockingQueue<>());
    }

    private static <E> SplitFetcher<E, TestingSourceSplit> createFetcher(
            final SplitReader<E, TestingSourceSplit> reader,
            final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> queue) {
        return new SplitFetcher<>(
                0, queue, reader, ExceptionUtils::rethrow, () -> {}, (ignore) -> {}, false);
    }

    private static <E> SplitFetcher<E, TestingSourceSplit> createFetcherWithSplit(
            final String splitId, final SplitReader<E, TestingSourceSplit> reader) {
        return createFetcherWithSplit(splitId, new FutureCompletingBlockingQueue<>(), reader);
    }

    private static <E> SplitFetcher<E, TestingSourceSplit> createFetcherWithSplit(
            final String splitId,
            final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> queue,
            final SplitReader<E, TestingSourceSplit> reader) {

        final SplitFetcher<E, TestingSourceSplit> fetcher = createFetcher(reader, queue);

        fetcher.addSplits(Collections.singletonList(new TestingSourceSplit(splitId)));
        while (fetcher.assignedSplits().isEmpty()) {
            fetcher.runOnce();
        }
        return fetcher;
    }

    // ------------------------------------------------------------------------

    private static final class QueueDrainerThread extends CheckedThread {

        private final FutureCompletingBlockingQueue<?> queue;
        private final SplitFetcher<?, ?> fetcher;
        private final int numFetchesToTake;

        private volatile boolean wasIdleWhenFinished;

        QueueDrainerThread(
                FutureCompletingBlockingQueue<?> queue,
                SplitFetcher<?, ?> fetcher,
                int numFetchesToTake) {
            super("Queue Drainer");
            setPriority(Thread.MAX_PRIORITY);
            this.queue = queue;
            this.fetcher = fetcher;
            this.numFetchesToTake = numFetchesToTake;
        }

        @Override
        public void go() throws Exception {
            int remaining = numFetchesToTake;
            while (remaining > 0) {
                remaining--;
                queue.take();
            }

            wasIdleWhenFinished = fetcher.isIdle();
        }

        public boolean wasIdleWhenFinished() {
            return wasIdleWhenFinished;
        }
    }
}
