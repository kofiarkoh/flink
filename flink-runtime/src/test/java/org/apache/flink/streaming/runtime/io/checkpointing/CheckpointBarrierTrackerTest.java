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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.operators.testutils.DummyCheckpointInvokable;
import org.apache.flink.streaming.runtime.io.MockInputGate;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.clock.SystemClock;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.streaming.runtime.io.checkpointing.UnalignedCheckpointsTest.addSequence;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the behavior of the barrier tracker. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CheckpointBarrierTrackerTest {

    private static final int PAGE_SIZE = 512;

    private CheckpointedInputGate inputGate;

    @AfterEach
    void ensureEmpty() throws Exception {
        assertThat(inputGate.pollNext()).isNotPresent();
        assertThat(inputGate.isFinished()).isTrue();
    }

    @Test
    void testSingleChannelNoBarriers() throws Exception {
        BufferOrEvent[] sequence = {createBuffer(0), createBuffer(0), createBuffer(0)};
        inputGate = createCheckpointedInputGate(1, sequence);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    @Test
    void testMultiChannelNoBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(2),
            createBuffer(2),
            createBuffer(0),
            createBuffer(1),
            createBuffer(0),
            createBuffer(3),
            createBuffer(1),
            createBuffer(1),
            createBuffer(2)
        };
        inputGate = createCheckpointedInputGate(4, sequence);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    @Test
    void testSingleChannelWithBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(0),
            createBuffer(0),
            createBuffer(0),
            createBarrier(1, 0),
            createBuffer(0),
            createBuffer(0),
            createBuffer(0),
            createBuffer(0),
            createBarrier(2, 0),
            createBarrier(3, 0),
            createBuffer(0),
            createBuffer(0),
            createBarrier(4, 0),
            createBarrier(5, 0),
            createBarrier(6, 0),
            createBuffer(0)
        };
        CheckpointSequenceValidator validator = new CheckpointSequenceValidator(1, 2, 3, 4, 5, 6);
        inputGate = createCheckpointedInputGate(1, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    @Test
    void testSingleChannelWithSkippedBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(0),
            createBarrier(1, 0),
            createBuffer(0),
            createBuffer(0),
            createBarrier(3, 0),
            createBuffer(0),
            createBarrier(4, 0),
            createBarrier(6, 0),
            createBuffer(0),
            createBarrier(7, 0),
            createBuffer(0),
            createBarrier(10, 0),
            createBuffer(0)
        };
        CheckpointSequenceValidator validator = new CheckpointSequenceValidator(1, 3, 4, 6, 7, 10);
        inputGate = createCheckpointedInputGate(1, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    @Test
    void testMultiChannelWithBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(0),
            createBuffer(2),
            createBuffer(0),
            createBarrier(1, 1),
            createBarrier(1, 2),
            createBuffer(2),
            createBuffer(1),
            createBarrier(1, 0),
            createBuffer(0),
            createBuffer(0),
            createBuffer(1),
            createBuffer(1),
            createBuffer(2),
            createBarrier(2, 0),
            createBarrier(2, 1),
            createBarrier(2, 2),
            createBuffer(2),
            createBuffer(2),
            createBarrier(3, 2),
            createBuffer(2),
            createBuffer(2),
            createBarrier(3, 0),
            createBarrier(3, 1),
            createBarrier(4, 1),
            createBarrier(4, 2),
            createBarrier(4, 0),
            createBuffer(0)
        };
        CheckpointSequenceValidator validator = new CheckpointSequenceValidator(1, 2, 3, 4);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    @Test
    void testMultiChannelSkippingCheckpoints() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(0),
            createBuffer(2),
            createBuffer(0),
            createBarrier(1, 1),
            createBarrier(1, 2),
            createBuffer(2),
            createBuffer(1),
            createBarrier(1, 0),
            createBuffer(0),
            createBuffer(0),
            createBuffer(1),
            createBuffer(1),
            createBuffer(2),
            createBarrier(2, 0),
            createBarrier(2, 1),
            createBarrier(2, 2),
            createBuffer(2),
            createBuffer(2),
            createBarrier(3, 2),
            createBuffer(2),
            createBuffer(2),

            // jump to checkpoint 4
            createBarrier(4, 0),
            createBuffer(0),
            createBuffer(1),
            createBuffer(2),
            createBarrier(4, 1),
            createBuffer(1),
            createBarrier(4, 2),
            createBuffer(0)
        };
        CheckpointSequenceValidator validator = new CheckpointSequenceValidator(1, 2, 4);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    /**
     * This test validates that the barrier tracker does not immediately discard a pending
     * checkpoint as soon as it sees a barrier from a later checkpoint from some channel.
     *
     * <p>This behavior is crucial, otherwise topologies where different inputs have different
     * latency (and that latency is close to or higher than the checkpoint interval) may skip many
     * checkpoints, or fail to complete a checkpoint all together.
     */
    @Test
    void testCompleteCheckpointsOnLateBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            // checkpoint 2
            createBuffer(1),
            createBuffer(1),
            createBuffer(0),
            createBuffer(2),
            createBarrier(2, 1),
            createBarrier(2, 0),
            createBarrier(2, 2),

            // incomplete checkpoint 3
            createBuffer(1),
            createBuffer(0),
            createBarrier(3, 1),
            createBarrier(3, 2),

            // some barriers from checkpoint 4
            createBuffer(1),
            createBuffer(0),
            createBarrier(4, 2),
            createBarrier(4, 1),
            createBuffer(1),
            createBuffer(2),

            // last barrier from checkpoint 3
            createBarrier(3, 0),

            // complete checkpoint 4
            createBuffer(0),
            createBarrier(4, 0),

            // regular checkpoint 5
            createBuffer(1),
            createBuffer(2),
            createBarrier(5, 1),
            createBuffer(0),
            createBarrier(5, 0),
            createBuffer(1),
            createBarrier(5, 2),

            // checkpoint 6 (incomplete),
            createBuffer(1),
            createBarrier(6, 1),
            createBuffer(0),
            createBarrier(6, 0),

            // checkpoint 7, with early barriers for checkpoints 8 and 9
            createBuffer(1),
            createBarrier(7, 1),
            createBuffer(0),
            createBarrier(7, 2),
            createBuffer(2),
            createBarrier(8, 2),
            createBuffer(0),
            createBarrier(8, 1),
            createBuffer(1),
            createBarrier(9, 1),

            // complete checkpoint 7, first barriers from checkpoint 10
            createBarrier(7, 0),
            createBuffer(0),
            createBarrier(9, 2),
            createBuffer(2),
            createBarrier(10, 2),

            // complete checkpoint 8 and 9
            createBarrier(8, 0),
            createBuffer(1),
            createBuffer(2),
            createBarrier(9, 0),

            // trailing data
            createBuffer(1),
            createBuffer(0),
            createBuffer(2),

            // complete checkpoint 10
            createBarrier(10, 0),
            createBarrier(10, 1),
        };
        CheckpointSequenceValidator validator =
                new CheckpointSequenceValidator(2, 3, 4, 5, 7, 8, 9, 10);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    @Test
    void testNextFirstCheckpointBarrierOvertakesCancellationBarrier() throws Exception {
        BufferOrEvent[] sequence = {
            // start checkpoint 1
            createBarrier(1, 1),
            //  start checkpoint 2(just suppose checkpoint 1 was canceled)
            createBarrier(2, 1),
            // cancellation barrier of checkpoint 1
            createCancellationBarrier(1, 0),
            //  finish the checkpoint 2
            createBarrier(2, 0)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler();
        ManualClock manualClock = new ManualClock();
        inputGate = createCheckpointedInputGate(2, sequence, validator, manualClock);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
            manualClock.advanceTime(Duration.ofSeconds(1));
        }
        assertThatFuture(validator.lastAlignmentDurationNanos)
                .eventuallySucceeds()
                .isEqualTo(Duration.ofSeconds(2).toNanos());
    }

    @Test
    void testSingleChannelAbortCheckpoint() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(0),
            createBarrier(1, 0),
            createBuffer(0),
            createBarrier(2, 0),
            createCancellationBarrier(4, 0),
            createBarrier(5, 0),
            createBuffer(0),
            createCancellationBarrier(6, 0),
            createBuffer(0)
        };
        // negative values mean an expected cancellation call!
        CheckpointSequenceValidator validator = new CheckpointSequenceValidator(1, 2, -4, 5, -6);
        inputGate = createCheckpointedInputGate(1, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    @Test
    void testMultiChannelAbortCheckpoint() throws Exception {
        BufferOrEvent[] sequence = {
            // some buffers and a successful checkpoint
            createBuffer(0),
            createBuffer(2),
            createBuffer(0),
            createBarrier(1, 1),
            createBarrier(1, 2),
            createBuffer(2),
            createBuffer(1),
            createBarrier(1, 0),

            // aborted on last barrier
            createBuffer(0),
            createBuffer(2),
            createBarrier(2, 0),
            createBarrier(2, 2),
            createBuffer(0),
            createBuffer(2),
            createCancellationBarrier(2, 1),

            // successful checkpoint
            createBuffer(2),
            createBuffer(1),
            createBarrier(3, 1),
            createBarrier(3, 2),
            createBarrier(3, 0),

            // abort on first barrier
            createBuffer(0),
            createBuffer(1),
            createCancellationBarrier(4, 1),
            createBarrier(4, 2),
            createBuffer(0),
            createBarrier(4, 0),

            // another successful checkpoint
            createBuffer(0),
            createBuffer(1),
            createBuffer(2),
            createBarrier(5, 2),
            createBarrier(5, 1),
            createBarrier(5, 0),

            // abort multiple cancellations and a barrier after the cancellations
            createBuffer(0),
            createBuffer(1),
            createCancellationBarrier(6, 1),
            createCancellationBarrier(6, 2),
            createBarrier(6, 0),
            createBuffer(0)
        };
        // negative values mean an expected cancellation call!
        CheckpointSequenceValidator validator =
                new CheckpointSequenceValidator(1, -2, 3, -4, 5, -6);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    /**
     * Tests that each checkpoint is only aborted once in case of an interleaved cancellation
     * barrier arrival of two consecutive checkpoints.
     */
    @Test
    void testInterleavedCancellationBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            createBarrier(1L, 0),
            createCancellationBarrier(2L, 0),
            createCancellationBarrier(1L, 1),
            createCancellationBarrier(2L, 1),
            createCancellationBarrier(1L, 2),
            createCancellationBarrier(2L, 2),
            createBuffer(0)
        };
        CheckpointSequenceValidator validator = new CheckpointSequenceValidator(-1, -2);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    @Test
    void testMetrics() throws Exception {
        List<BufferOrEvent> output = new ArrayList<>();
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
        int numberOfChannels = 3;
        inputGate = createCheckpointedInputGate(numberOfChannels, handler);
        int[] sequenceNumbers = new int[numberOfChannels];

        int bufferSize = 100;
        long checkpointId = 1;
        long sleepTime = 10;

        long checkpointBarrierCreation = System.currentTimeMillis();
        long alignmentStartNanos = System.nanoTime();

        Thread.sleep(sleepTime);

        addSequence(
                inputGate,
                output,
                sequenceNumbers,
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createBarrier(checkpointId, 1, checkpointBarrierCreation),
                createBuffer(0, bufferSize),
                createBuffer(2, bufferSize),
                createBarrier(checkpointId, 0),
                createBuffer(2, bufferSize));

        Thread.sleep(sleepTime);

        addSequence(
                inputGate,
                output,
                sequenceNumbers,
                createBarrier(checkpointId, 2),
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createEndOfPartition(0),
                createEndOfPartition(1),
                createEndOfPartition(2));

        long startDelay = System.currentTimeMillis() - checkpointBarrierCreation;
        long alignmentDuration = System.nanoTime() - alignmentStartNanos;

        assertThat(inputGate.getCheckpointStartDelayNanos() / 1_000_000)
                .isBetween(sleepTime, startDelay);

        assertThatFuture(handler.getLastAlignmentDurationNanos())
                .eventuallySucceeds()
                .satisfies(
                        duration ->
                                assertThat(duration / 1_000_000)
                                        .isBetween(sleepTime, alignmentDuration));
        assertThat(handler.getLastAlignmentDurationNanos()).isDone();
        assertThat(handler.getLastAlignmentDurationNanos().get() / 1_000_000)
                .isBetween(sleepTime, alignmentDuration);

        assertThat(handler.getLastBytesProcessedDuringAlignment())
                .isCompletedWithValue(3L * bufferSize);
    }

    @Test
    void testSingleChannelMetrics() throws Exception {
        List<BufferOrEvent> output = new ArrayList<>();
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
        int numberOfChannels = 1;
        inputGate = createCheckpointedInputGate(numberOfChannels, handler);
        int[] sequenceNumbers = new int[numberOfChannels];

        int bufferSize = 100;
        long checkpointId = 1;
        long sleepTime = 10;

        long checkpointBarrierCreation = System.currentTimeMillis();

        Thread.sleep(sleepTime);

        addSequence(
                inputGate,
                output,
                sequenceNumbers,
                createBuffer(0, bufferSize),
                createBarrier(checkpointId, 0, checkpointBarrierCreation),
                createBuffer(0, bufferSize),
                createEndOfPartition(0));

        long startDelay = System.currentTimeMillis() - checkpointBarrierCreation;

        assertThat(inputGate.getCheckpointStartDelayNanos() / 1_000_000)
                .isBetween(sleepTime, startDelay);

        assertThat(handler.getLastAlignmentDurationNanos()).isCompletedWithValue(0L);
        assertThat(handler.getLastBytesProcessedDuringAlignment()).isCompletedWithValue(0L);
    }

    @Test
    void testTriggerCheckpointsWithEndOfPartition() throws Exception {
        BufferOrEvent[] sequence = {
            createBarrier(1, 0),
            createBarrier(2, 0),
            createBarrier(2, 1),
            createBarrier(3, 0),
            createBarrier(4, 0),
            createBarrier(4, 1),
            createBarrier(5, 1),
            createEndOfPartition(2)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(4);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        CheckpointBarrierTracker checkpointBarrierTracker =
                (CheckpointBarrierTracker) inputGate.getCheckpointBarrierHandler();

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }

        // Only checkpoints 4 is triggered and the previous checkpoints are ignored.
        assertThat(validator.triggeredCheckpoints).containsExactly(4L);
        assertThat(validator.getAbortedCheckpointCounter()).isZero();
        assertThat(checkpointBarrierTracker.getPendingCheckpointIds()).containsExactly(5L);
        assertThat(checkpointBarrierTracker.getNumOpenChannels()).isEqualTo(2);
    }

    @Test
    void testDeduplicateChannelsWithBothBarrierAndEndOfPartition() throws Exception {
        BufferOrEvent[] sequence = {
            /* 0 */ createBarrier(2, 0),
            /* 1 */ createBarrier(2, 1),
            /* 2 */ createEndOfPartition(1),
            /* 3 */ createBarrier(2, 2)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(-1);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (int i = 0; i <= 2; ++i) {
            assertThat(inputGate.pollNext()).hasValue(sequence[i]);
        }

        // Here the checkpoint should not be triggered.
        assertThat(validator.getTriggeredCheckpointCounter()).isZero();
        assertThat(validator.getAbortedCheckpointCounter()).isZero();

        // The last barrier aligned the pending checkpoint 2.
        assertThat(inputGate.pollNext()).hasValue(sequence[3]);
        assertThat(validator.triggeredCheckpoints).containsExactly(2L);
    }

    @Test
    void testTriggerCheckpointsAfterReceivedEndOfPartition() throws Exception {
        BufferOrEvent[] sequence = {
            /* 0 */ createEndOfPartition(2),
            /* 1 */ createBarrier(5, 0),
            /* 2 */ createBarrier(6, 0),
            /* 3 */ createBarrier(6, 1),
            /* 4 */ createEndOfPartition(1),
            /* 5 */ createBarrier(7, 0)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(-1);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }

        assertThat(validator.triggeredCheckpoints).containsExactly(6L, 7L);
        assertThat(validator.getAbortedCheckpointCounter()).isZero();
    }

    @Test
    void testNoFastPathWithChannelFinishedDuringCheckpoints() throws Exception {
        BufferOrEvent[] sequence = {
            createBarrier(1, 0), createEndOfPartition(0), createBarrier(1, 1)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(2, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }

        // The last barrier should finish the pending checkpoint instead of trigger a "new" one.
        assertThat(validator.getTriggeredCheckpointCounter()).isOne();
        assertThat(inputGate.getCheckpointBarrierHandler().isCheckpointPending()).isFalse();
    }

    @Test
    void testCompleteAndRemoveAbortedCheckpointWithEndOfPartition() throws Exception {
        BufferOrEvent[] sequence = {
            createCancellationBarrier(4, 0),
            createBarrier(4, 1),
            createBarrier(5, 1),
            createEndOfPartition(2)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(-1);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        CheckpointBarrierTracker checkpointBarrierTracker =
                (CheckpointBarrierTracker) inputGate.getCheckpointBarrierHandler();

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }

        assertThat(validator.getAbortedCheckpointCounter()).isOne();
        assertThat(validator.getLastCanceledCheckpointId()).isEqualTo(4L);
        assertThat(validator.getTriggeredCheckpointCounter()).isZero();
        assertThat(checkpointBarrierTracker.getPendingCheckpointIds()).containsExactly(5L);
        assertThat(checkpointBarrierTracker.getNumOpenChannels()).isEqualTo(2L);
    }

    @Test
    void testAbortCheckpointsAfterEndOfPartitionReceived() throws Exception {
        BufferOrEvent[] sequence = {
            /* 0 */ createEndOfPartition(2),
            /* 1 */ createBarrier(5, 0),
            /* 2 */ createBarrier(6, 0),
            /* 3 */ createCancellationBarrier(6, 1),
            /* 4 */ createEndOfPartition(1),
            /* 5 */ createCancellationBarrier(7, 0)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(-1);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }

        assertThat(validator.abortedCheckpoints).containsExactly(5L, 6L, 7L);
        assertThat(validator.getTriggeredCheckpointCounter()).isZero();
    }

    @Test
    void testNoFastPathWithChannelFinishedDuringCheckpointsCancel() throws Exception {
        BufferOrEvent[] sequence = {
            createBarrier(1, 0, 0), createEndOfPartition(0), createCancellationBarrier(1, 1)
        };

        ValidatingCheckpointHandler checkpointHandler = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(2, sequence, checkpointHandler);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }

        assertThat(checkpointHandler.getLastCanceledCheckpointId()).isOne();
        // If go with the fast path, the pending checkpoint would not be removed normally.
        assertThat(inputGate.getCheckpointBarrierHandler().isCheckpointPending()).isFalse();
    }

    @Test
    void testTwoLastBarriersOneByOne() throws Exception {
        BufferOrEvent[] sequence = {
            // start checkpoint 1
            createBarrier(1, 1),
            // start checkpoint 2
            createBarrier(2, 1),
            // finish the checkpoint 1
            createBarrier(1, 0),
            //  finish the checkpoint 2
            createBarrier(2, 0)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler();
        ManualClock manualClock = new ManualClock();
        inputGate = createCheckpointedInputGate(2, sequence, validator, manualClock);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
            manualClock.advanceTime(Duration.ofSeconds(1));
        }
        assertThatFuture(validator.lastAlignmentDurationNanos)
                .eventuallySucceeds()
                .isEqualTo(Duration.ofSeconds(2).toNanos());
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    private CheckpointedInputGate createCheckpointedInputGate(
            int numberOfChannels, AbstractInvokable toNotify) throws IOException {
        final NettyShuffleEnvironment environment = new NettyShuffleEnvironmentBuilder().build();
        SingleInputGate gate =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(numberOfChannels)
                        .setupBufferPoolFactory(environment)
                        .build();
        gate.setInputChannels(
                IntStream.range(0, numberOfChannels)
                        .mapToObj(
                                channelIndex ->
                                        InputChannelBuilder.newBuilder()
                                                .setChannelIndex(channelIndex)
                                                .setupFromNettyShuffleEnvironment(environment)
                                                .setConnectionManager(
                                                        new TestingConnectionManager())
                                                .buildRemoteChannel(gate))
                        .toArray(RemoteInputChannel[]::new));

        gate.setup();
        gate.requestPartitions();

        return createCheckpointedInputGate(gate, toNotify);
    }

    private static CheckpointedInputGate createCheckpointedInputGate(
            int numberOfChannels, BufferOrEvent[] sequence) {
        return createCheckpointedInputGate(
                numberOfChannels, sequence, new DummyCheckpointInvokable());
    }

    private static CheckpointedInputGate createCheckpointedInputGate(
            int numberOfChannels,
            BufferOrEvent[] sequence,
            @Nullable AbstractInvokable toNotifyOnCheckpoint) {
        MockInputGate gate = new MockInputGate(numberOfChannels, Arrays.asList(sequence));
        return createCheckpointedInputGate(gate, toNotifyOnCheckpoint);
    }

    private static CheckpointedInputGate createCheckpointedInputGate(
            int numberOfChannels,
            BufferOrEvent[] sequence,
            @Nullable AbstractInvokable toNotifyOnCheckpoint,
            Clock clock) {
        MockInputGate gate = new MockInputGate(numberOfChannels, Arrays.asList(sequence));
        return createCheckpointedInputGate(gate, toNotifyOnCheckpoint, clock);
    }

    private static CheckpointedInputGate createCheckpointedInputGate(
            IndexedInputGate inputGate, @Nullable AbstractInvokable toNotifyOnCheckpoint) {
        return createCheckpointedInputGate(
                inputGate, toNotifyOnCheckpoint, SystemClock.getInstance());
    }

    private static CheckpointedInputGate createCheckpointedInputGate(
            IndexedInputGate inputGate,
            @Nullable AbstractInvokable toNotifyOnCheckpoint,
            Clock clock) {
        return new CheckpointedInputGate(
                inputGate,
                new CheckpointBarrierTracker(
                        inputGate.getNumberOfInputChannels(), toNotifyOnCheckpoint, clock, true),
                new SyncMailboxExecutor());
    }

    private static BufferOrEvent createBarrier(long checkpointId, int channel) {
        return createBarrier(checkpointId, channel, System.currentTimeMillis());
    }

    private static BufferOrEvent createBarrier(
            long checkpointId, int channel, long creationTimestamp) {
        return new BufferOrEvent(
                new CheckpointBarrier(
                        checkpointId,
                        creationTimestamp,
                        CheckpointOptions.forCheckpointWithDefaultLocation()),
                new InputChannelInfo(0, channel));
    }

    private static BufferOrEvent createCancellationBarrier(long id, int channel) {
        return new BufferOrEvent(new CancelCheckpointMarker(id), new InputChannelInfo(0, channel));
    }

    private static BufferOrEvent createBuffer(int channel) {
        return new BufferOrEvent(
                new NetworkBuffer(
                        MemorySegmentFactory.wrap(new byte[] {1, 2}),
                        FreeingBufferRecycler.INSTANCE),
                new InputChannelInfo(0, channel));
    }

    private static BufferOrEvent createBuffer(int channel, int size) {
        return new BufferOrEvent(
                TestBufferFactory.createBuffer(size), new InputChannelInfo(0, channel));
    }

    private static BufferOrEvent createEndOfPartition(int channel) {
        return new BufferOrEvent(EndOfPartitionEvent.INSTANCE, new InputChannelInfo(0, channel));
    }

    // ------------------------------------------------------------------------
    //  Testing Mocks
    // ------------------------------------------------------------------------

}
