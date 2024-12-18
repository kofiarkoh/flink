/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.clock.SystemClock;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the behaviors of the {@link CheckpointedInputGate}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class UnalignedCheckpointsTest {

    private static final long DEFAULT_CHECKPOINT_ID = 0L;

    private int sizeCounter = 1;

    private CheckpointedInputGate inputGate;

    private RecordingChannelStateWriter channelStateWriter;

    private int[] sequenceNumbers;

    private List<BufferOrEvent> output;

    @BeforeEach
    void setUp() {
        channelStateWriter = new RecordingChannelStateWriter();
    }

    @AfterEach
    void ensureEmpty() throws Exception {
        if (inputGate != null) {
            assertThat(inputGate.pollNext()).isNotPresent();
            assertThat(inputGate.isFinished()).isTrue();
            inputGate.close();
        }

        if (channelStateWriter != null) {
            channelStateWriter.close();
        }
    }

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    /**
     * Validates that the buffer behaves correctly if no checkpoint barriers come, for a single
     * input channel.
     */
    @Test
    void testSingleChannelNoBarriers() throws Exception {
        inputGate = createInputGate(1, new ValidatingCheckpointHandler(1));
        final BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(0),
                        createEndOfPartition(0));

        assertOutput(sequence);
        assertInflightData();
    }

    /**
     * Validates that the buffer behaves correctly if no checkpoint barriers come, for an input with
     * multiple input channels.
     */
    @Test
    void testMultiChannelNoBarriers() throws Exception {
        inputGate = createInputGate(4, new ValidatingCheckpointHandler(1));
        final BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
                        createBuffer(2),
                        createBuffer(2),
                        createBuffer(0),
                        createBuffer(1),
                        createBuffer(0),
                        createEndOfPartition(0),
                        createBuffer(3),
                        createBuffer(1),
                        createEndOfPartition(3),
                        createBuffer(1),
                        createEndOfPartition(1),
                        createBuffer(2),
                        createEndOfPartition(2));

        assertOutput(sequence);
        assertInflightData();
    }

    /**
     * Validates that the buffer preserved the order of elements for a input with a single input
     * channel, and checkpoint events.
     */
    @Test
    void testSingleChannelWithBarriers() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(1, handler);
        final BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
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
                        createBuffer(0),
                        createEndOfPartition(0));

        assertOutput(sequence);
    }

    /**
     * Validates that the buffer correctly aligns the streams for inputs with multiple input
     * channels, by buffering and blocking certain inputs.
     */
    @Test
    void testMultiChannelWithBarriers() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);

        // checkpoint with in-flight data
        BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(0),
                        createBarrier(1, 1),
                        createBarrier(1, 2),
                        createBuffer(2),
                        createBuffer(1),
                        createBuffer(0), // last buffer = in-flight
                        createBarrier(1, 0));

        // checkpoint 1 triggered unaligned
        assertOutput(sequence1);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isOne();
        assertInflightData(sequence1[7]);

        // checkpoint without in-flight data
        BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(1),
                        createBuffer(1),
                        createBuffer(2),
                        createBarrier(2, 0),
                        createBarrier(2, 1),
                        createBarrier(2, 2));

        assertOutput(sequence2);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(2L);
        assertInflightData();

        // checkpoint with data only from one channel
        BufferOrEvent[] sequence3 =
                addSequence(
                        inputGate,
                        createBuffer(2),
                        createBuffer(2),
                        createBarrier(3, 2),
                        createBuffer(2),
                        createBuffer(2),
                        createBarrier(3, 0),
                        createBarrier(3, 1));

        assertOutput(sequence3);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(3L);
        assertInflightData();

        // empty checkpoint
        addSequence(inputGate, createBarrier(4, 1), createBarrier(4, 2), createBarrier(4, 0));

        assertOutput();
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(4L);
        assertInflightData();

        // checkpoint with in-flight data in mixed order
        BufferOrEvent[] sequence5 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(0),
                        createBarrier(5, 1),
                        createBuffer(2),
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(1),
                        createBarrier(5, 2),
                        createBuffer(1),
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(1),
                        createBarrier(5, 0));

        assertOutput(sequence5);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(5L);
        assertInflightData(sequence5[4], sequence5[5], sequence5[6], sequence5[10]);

        // some trailing data
        BufferOrEvent[] sequence6 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createEndOfPartition(0),
                        createEndOfPartition(1),
                        createEndOfPartition(2));

        assertOutput(sequence6);
        assertInflightData();
    }

    @Test
    void testMetrics() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);
        int bufferSize = 100;
        long checkpointId = 1;
        long sleepTime = 10;

        long checkpointBarrierCreation = System.currentTimeMillis();

        Thread.sleep(sleepTime);

        long alignmentStartNanos = System.nanoTime();

        addSequence(
                inputGate,
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createBarrier(checkpointId, 1, checkpointBarrierCreation),
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createBarrier(checkpointId, 0),
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize));

        long startDelay = System.currentTimeMillis() - checkpointBarrierCreation;
        Thread.sleep(sleepTime);

        addSequence(
                inputGate,
                createBarrier(checkpointId, 2),
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createEndOfPartition(0),
                createEndOfPartition(1),
                createEndOfPartition(2));

        long alignmentDuration = System.nanoTime() - alignmentStartNanos;

        assertThat(inputGate.getCheckpointBarrierHandler().getLatestCheckpointId())
                .isEqualTo(checkpointId);
        assertThat(inputGate.getCheckpointStartDelayNanos() / 1_000_000)
                .isBetween(sleepTime, startDelay);

        assertThat(handler.getLastAlignmentDurationNanos()).isDone();
        assertThat(handler.getLastAlignmentDurationNanos().get() / 1_000_000)
                .isBetween(sleepTime, alignmentDuration);

        assertThat(handler.getLastBytesProcessedDuringAlignment())
                .isCompletedWithValue(6L * bufferSize);
    }

    @Test
    void testMultiChannelTrailingInflightData() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler, false);

        BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(1),
                        createBuffer(2),
                        createBarrier(1, 1),
                        createBarrier(1, 2),
                        createBarrier(1, 0),
                        createBuffer(2),
                        createBuffer(1),
                        createBuffer(0),
                        createBarrier(2, 1),
                        createBuffer(1),
                        createBuffer(1),
                        createEndOfPartition(1),
                        createBuffer(0),
                        createBuffer(2),
                        createBarrier(2, 2),
                        createBuffer(2),
                        createEndOfPartition(2),
                        createBuffer(0),
                        createEndOfPartition(0));

        assertOutput(sequence);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(2L);
        // TODO: treat EndOfPartitionEvent as a special CheckpointBarrier?
        assertInflightData();
    }

    @Test
    void testMissingCancellationBarriers() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(2, handler);
        final BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
                        createBarrier(1L, 0),
                        createCancellationBarrier(2L, 0),
                        createCancellationBarrier(3L, 0),
                        createCancellationBarrier(3L, 1),
                        createBuffer(0),
                        createEndOfPartition(0),
                        createEndOfPartition(1));

        assertOutput(sequence);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isOne();
        assertThat(handler.getLastCanceledCheckpointId()).isEqualTo(3L);
        assertInflightData();
    }

    @Test
    void testEarlyCleanup() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler, false);

        // checkpoint 1
        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(1),
                        createBuffer(2),
                        createBarrier(1, 1),
                        createBarrier(1, 2),
                        createBarrier(1, 0));
        assertOutput(sequence1);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isOne();
        assertInflightData();

        // checkpoint 2
        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBuffer(2),
                        createBuffer(1),
                        createBuffer(0),
                        createBarrier(2, 1),
                        createBuffer(1),
                        createBuffer(1),
                        createEndOfPartition(1),
                        createBuffer(0),
                        createBuffer(2),
                        createBarrier(2, 2),
                        createBuffer(2),
                        createEndOfPartition(2),
                        createBuffer(0),
                        createEndOfPartition(0));
        assertOutput(sequence2);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(2L);
        assertInflightData();
    }

    @Test
    void testStartAlignmentWithClosedChannels() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(2);
        inputGate = createInputGate(4, handler);

        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        // close some channels immediately
                        createEndOfPartition(2),
                        createEndOfPartition(1),

                        // checkpoint without in-flight data
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(3),
                        createBarrier(2, 3),
                        createBarrier(2, 0));
        assertOutput(sequence1);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(2L);
        assertInflightData();

        // checkpoint with in-flight data
        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBuffer(3),
                        createBuffer(0),
                        createBarrier(3, 3),
                        createBuffer(3),
                        createBuffer(0),
                        createBarrier(3, 0));
        assertOutput(sequence2);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(3L);
        assertInflightData(sequence2[4]);

        // empty checkpoint
        final BufferOrEvent[] sequence3 =
                addSequence(inputGate, createBarrier(4, 0), createBarrier(4, 3));
        assertOutput(sequence3);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(4L);
        assertInflightData();

        // some data, one channel closes
        final BufferOrEvent[] sequence4 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(3),
                        createEndOfPartition(0));
        assertOutput(sequence4);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(-1L);
        assertInflightData();

        // checkpoint on last remaining channel
        final BufferOrEvent[] sequence5 =
                addSequence(
                        inputGate,
                        createBuffer(3),
                        createBarrier(5, 3),
                        createBuffer(3),
                        createEndOfPartition(3));
        assertOutput(sequence5);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(5L);
        assertInflightData();
    }

    @Test
    void testEndOfStreamWhileCheckpoint() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);

        // one checkpoint
        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate, createBarrier(1, 0), createBarrier(1, 1), createBarrier(1, 2));
        assertOutput(sequence1);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isOne();
        assertInflightData();

        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        // some buffers
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(2),

                        // start the checkpoint that will be incomplete
                        createBarrier(2, 2),
                        createBarrier(2, 0),
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(1),

                        // close one after the barrier one before the barrier
                        createEndOfPartition(2),
                        createEndOfPartition(1),
                        createBuffer(0),

                        // final end of stream
                        createEndOfPartition(0));
        assertOutput(sequence2);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(2L);
        assertInflightData(sequence2[7]);
    }

    @Test
    public void testNotifyAbortCheckpointBeforeCancellingAsyncCheckpoint() throws Exception {
        ValidateAsyncFutureNotCompleted handler = new ValidateAsyncFutureNotCompleted(1);
        inputGate = createInputGate(2, handler);
        handler.setInputGate(inputGate);
        addSequence(inputGate, createBarrier(1, 0), createCancellationBarrier(1, 1));

        addSequence(inputGate, createEndOfPartition(0), createEndOfPartition(1));
    }

    @Test
    void testSingleChannelAbortCheckpoint() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(1, handler);
        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBarrier(1, 0),
                        createBuffer(0),
                        createBarrier(2, 0),
                        createCancellationBarrier(4, 0));

        assertOutput(sequence1);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(2L);
        assertThat(handler.getLastCanceledCheckpointId()).isEqualTo(4L);
        assertInflightData();

        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBarrier(5, 0),
                        createBuffer(0),
                        createCancellationBarrier(6, 0),
                        createBuffer(0),
                        createEndOfPartition(0));

        assertOutput(sequence2);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(5L);
        assertThat(handler.getLastCanceledCheckpointId()).isEqualTo(6L);
        assertInflightData();
    }

    @Test
    void testMultiChannelAbortCheckpoint() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);
        // some buffers and a successful checkpoint
        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(0),
                        createBarrier(1, 1),
                        createBarrier(1, 2),
                        createBuffer(2),
                        createBuffer(1),
                        createBarrier(1, 0),
                        createBuffer(0),
                        createBuffer(2));

        assertOutput(sequence1);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isOne();
        assertInflightData();

        // canceled checkpoint on last barrier
        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBarrier(2, 0),
                        createBarrier(2, 2),
                        createBuffer(0),
                        createBuffer(2),
                        createCancellationBarrier(2, 1));

        assertOutput(sequence2);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(2L);
        assertThat(handler.getLastCanceledCheckpointId()).isEqualTo(2L);
        assertInflightData();

        // one more successful checkpoint
        final BufferOrEvent[] sequence3 =
                addSequence(
                        inputGate,
                        createBuffer(2),
                        createBuffer(1),
                        createBarrier(3, 1),
                        createBarrier(3, 2),
                        createBarrier(3, 0));

        assertOutput(sequence3);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(3L);
        assertInflightData();

        // this checkpoint gets immediately canceled, don't start a checkpoint at all
        final BufferOrEvent[] sequence4 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(1),
                        createCancellationBarrier(4, 1),
                        createBarrier(4, 2),
                        createBuffer(0),
                        createBarrier(4, 0));

        assertOutput(sequence4);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(-1L);
        assertThat(handler.getLastCanceledCheckpointId()).isEqualTo(4L);
        assertInflightData();

        // a simple successful checkpoint
        // another successful checkpoint
        final BufferOrEvent[] sequence5 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(1),
                        createBuffer(2),
                        createBarrier(5, 2),
                        createBarrier(5, 1),
                        createBarrier(5, 0),
                        createBuffer(0),
                        createBuffer(1));

        assertOutput(sequence5);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(5L);
        assertInflightData();

        // abort multiple cancellations and a barrier after the cancellations, don't start a
        // checkpoint at all
        final BufferOrEvent[] sequence6 =
                addSequence(
                        inputGate,
                        createCancellationBarrier(6, 1),
                        createCancellationBarrier(6, 2),
                        createBarrier(6, 0),
                        createBuffer(0),
                        createEndOfPartition(0),
                        createEndOfPartition(1),
                        createEndOfPartition(2));

        assertOutput(sequence6);
        assertThat(channelStateWriter.getLastStartedCheckpointId()).isEqualTo(-1L);
        assertThat(handler.getLastCanceledCheckpointId()).isEqualTo(6L);
        assertInflightData();
    }

    /**
     * Tests {@link
     * SingleCheckpointBarrierHandler#processCancellationBarrier(CancelCheckpointMarker,
     * InputChannelInfo)} abort the current pending checkpoint triggered by {@link
     * CheckpointBarrierHandler#processBarrier(CheckpointBarrier, InputChannelInfo, boolean)}.
     */
    @Test
    void testProcessCancellationBarrierAfterProcessBarrier() throws Exception {
        final ValidatingCheckpointInvokable invokable = new ValidatingCheckpointInvokable();
        final SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(2)
                        .setChannelFactory(InputChannelBuilder::buildLocalChannel)
                        .build();
        final SingleCheckpointBarrierHandler handler =
                SingleCheckpointBarrierHandler.createUnalignedCheckpointBarrierHandler(
                        TestSubtaskCheckpointCoordinator.INSTANCE,
                        "test",
                        invokable,
                        SystemClock.getInstance(),
                        true,
                        inputGate);

        // should trigger respective checkpoint
        handler.processBarrier(
                buildCheckpointBarrier(DEFAULT_CHECKPOINT_ID), new InputChannelInfo(0, 0), false);

        assertThat(handler.isCheckpointPending()).isTrue();
        assertThat(handler.getLatestCheckpointId()).isEqualTo(DEFAULT_CHECKPOINT_ID);

        testProcessCancellationBarrier(handler, invokable);
    }

    @Test
    void testProcessCancellationBarrierBeforeProcessAndReceiveBarrier() throws Exception {
        final ValidatingCheckpointInvokable invokable = new ValidatingCheckpointInvokable();
        final SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalChannel)
                        .build();
        final SingleCheckpointBarrierHandler handler =
                SingleCheckpointBarrierHandler.createUnalignedCheckpointBarrierHandler(
                        TestSubtaskCheckpointCoordinator.INSTANCE,
                        "test",
                        invokable,
                        SystemClock.getInstance(),
                        true,
                        inputGate);

        handler.processCancellationBarrier(
                new CancelCheckpointMarker(DEFAULT_CHECKPOINT_ID), new InputChannelInfo(0, 0));

        verifyTriggeredCheckpoint(handler, invokable, DEFAULT_CHECKPOINT_ID);

        // it would not trigger checkpoint since the respective cancellation barrier already
        // happened before
        handler.processBarrier(
                buildCheckpointBarrier(DEFAULT_CHECKPOINT_ID), new InputChannelInfo(0, 0), false);

        verifyTriggeredCheckpoint(handler, invokable, DEFAULT_CHECKPOINT_ID);
    }

    private void testProcessCancellationBarrier(
            SingleCheckpointBarrierHandler handler, ValidatingCheckpointInvokable invokable)
            throws Exception {

        final long cancelledCheckpointId =
                new Random().nextBoolean() ? DEFAULT_CHECKPOINT_ID : DEFAULT_CHECKPOINT_ID + 1L;
        // should abort current checkpoint while processing CancelCheckpointMarker
        handler.processCancellationBarrier(
                new CancelCheckpointMarker(cancelledCheckpointId), new InputChannelInfo(0, 0));
        verifyTriggeredCheckpoint(handler, invokable, cancelledCheckpointId);

        final long nextCancelledCheckpointId = cancelledCheckpointId + 1L;
        // should update current checkpoint id and abort notification while processing
        // CancelCheckpointMarker
        handler.processCancellationBarrier(
                new CancelCheckpointMarker(nextCancelledCheckpointId), new InputChannelInfo(0, 0));
        verifyTriggeredCheckpoint(handler, invokable, nextCancelledCheckpointId);
    }

    private void verifyTriggeredCheckpoint(
            SingleCheckpointBarrierHandler handler,
            ValidatingCheckpointInvokable invokable,
            long currentCheckpointId) {

        assertThat(handler.isCheckpointPending()).isFalse();
        assertThat(handler.getLatestCheckpointId()).isEqualTo(currentCheckpointId);
        assertThat(invokable.getAbortedCheckpointId()).isEqualTo(currentCheckpointId);
    }

    @Test
    void testEndOfStreamWithPendingCheckpoint() throws Exception {
        final int numberOfChannels = 2;
        final ValidatingCheckpointInvokable invokable = new ValidatingCheckpointInvokable();
        final SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalChannel)
                        .setNumberOfChannels(numberOfChannels)
                        .build();
        final SingleCheckpointBarrierHandler handler =
                SingleCheckpointBarrierHandler.createUnalignedCheckpointBarrierHandler(
                        TestSubtaskCheckpointCoordinator.INSTANCE,
                        "test",
                        invokable,
                        SystemClock.getInstance(),
                        false,
                        inputGate);

        // should trigger respective checkpoint
        handler.processBarrier(
                buildCheckpointBarrier(DEFAULT_CHECKPOINT_ID), new InputChannelInfo(0, 0), false);

        assertThat(handler.isCheckpointPending()).isTrue();
        assertThat(handler.getLatestCheckpointId()).isEqualTo(DEFAULT_CHECKPOINT_ID);
        assertThat(handler.getNumOpenChannels()).isEqualTo(numberOfChannels);

        // should abort current checkpoint while processing eof
        handler.processEndOfPartition(new InputChannelInfo(0, 0));

        assertThat(handler.isCheckpointPending()).isFalse();
        assertThat(handler.getLatestCheckpointId()).isEqualTo(DEFAULT_CHECKPOINT_ID);
        assertThat(handler.getNumOpenChannels()).isEqualTo(numberOfChannels - 1);
        assertThat(invokable.getAbortedCheckpointId()).isEqualTo(DEFAULT_CHECKPOINT_ID);
    }

    @Test
    void testTriggerCheckpointsWithEndOfPartition() throws Exception {
        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(-1);
        inputGate = createInputGate(3, validator);

        BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
                        /* 0 */ createBarrier(1, 1),
                        /* 1 */ createBuffer(0),
                        /* 2 */ createBarrier(1, 0),
                        /* 3 */ createBuffer(1),
                        /* 4 */ createEndOfPartition(2),
                        /* 5 */ createEndOfPartition(0),
                        /* 6 */ createEndOfPartition(1));

        assertOutput(sequence);
        assertThat(validator.triggeredCheckpoints).containsExactly(1L);
        assertThat(validator.getAbortedCheckpointCounter()).isZero();
        assertInflightData(sequence[1]);
    }

    @Test
    void testTriggerCheckpointsAfterReceivedEndOfPartition() throws Exception {
        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(-1);
        inputGate = createInputGate(3, validator);

        BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        /* 0 */ createEndOfPartition(0),
                        /* 1 */ createBarrier(3, 1),
                        /* 2 */ createBuffer(1),
                        /* 3 */ createBuffer(2),
                        /* 4 */ createEndOfPartition(1),
                        /* 5 */ createBarrier(3, 2));
        assertOutput(sequence1);
        assertInflightData(sequence1[3]);
        assertThat(validator.triggeredCheckpoints).containsExactly(3L);
        assertThat(validator.getAbortedCheckpointCounter()).isZero();

        BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        /* 0 */ createBuffer(2),
                        /* 1 */ createBarrier(4, 2),
                        /* 2 */ createEndOfPartition(2));
        assertOutput(sequence2);
        assertInflightData();
        assertThat(validator.triggeredCheckpoints).containsExactly(3L, 4L);
        assertThat(validator.getAbortedCheckpointCounter()).isZero();
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    private BufferOrEvent createBarrier(long checkpointId, int channel) {
        return createBarrier(checkpointId, channel, System.currentTimeMillis());
    }

    private BufferOrEvent createBarrier(long checkpointId, int channel, long timestamp) {
        sizeCounter++;
        return new BufferOrEvent(
                new CheckpointBarrier(
                        checkpointId,
                        timestamp,
                        CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault())),
                new InputChannelInfo(0, channel));
    }

    private BufferOrEvent createCancellationBarrier(long checkpointId, int channel) {
        sizeCounter++;
        return new BufferOrEvent(
                new CancelCheckpointMarker(checkpointId), new InputChannelInfo(0, channel));
    }

    private BufferOrEvent createBuffer(int channel, int size) {
        return new BufferOrEvent(
                TestBufferFactory.createBuffer(size), new InputChannelInfo(0, channel));
    }

    private BufferOrEvent createBuffer(int channel) {
        final int size = sizeCounter++;
        return new BufferOrEvent(
                TestBufferFactory.createBuffer(size), new InputChannelInfo(0, channel));
    }

    private static BufferOrEvent createEndOfPartition(int channel) {
        return new BufferOrEvent(EndOfPartitionEvent.INSTANCE, new InputChannelInfo(0, channel));
    }

    private CheckpointedInputGate createInputGate(int numberOfChannels, AbstractInvokable toNotify)
            throws IOException {
        return createInputGate(numberOfChannels, toNotify, true);
    }

    private CheckpointedInputGate createInputGate(
            int numberOfChannels,
            AbstractInvokable toNotify,
            boolean enableCheckpointsAfterTasksFinished)
            throws IOException {
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
                                                .setStateWriter(channelStateWriter)
                                                .setupFromNettyShuffleEnvironment(environment)
                                                .setConnectionManager(
                                                        new TestingConnectionManager())
                                                .buildRemoteChannel(gate))
                        .toArray(RemoteInputChannel[]::new));
        sequenceNumbers = new int[numberOfChannels];

        gate.setup();
        gate.requestPartitions();

        return createCheckpointedInputGate(gate, toNotify, enableCheckpointsAfterTasksFinished);
    }

    private BufferOrEvent[] addSequence(CheckpointedInputGate inputGate, BufferOrEvent... sequence)
            throws Exception {
        output = new ArrayList<>();
        addSequence(inputGate, output, sequenceNumbers, sequence);
        sizeCounter = 1;
        return sequence;
    }

    static BufferOrEvent[] addSequence(
            CheckpointedInputGate inputGate,
            List<BufferOrEvent> output,
            int[] sequenceNumbers,
            BufferOrEvent... sequence)
            throws Exception {
        for (BufferOrEvent bufferOrEvent : sequence) {
            if (bufferOrEvent.isEvent()) {
                bufferOrEvent =
                        new BufferOrEvent(
                                EventSerializer.toBuffer(
                                        bufferOrEvent.getEvent(),
                                        bufferOrEvent.getEvent() instanceof CheckpointBarrier),
                                bufferOrEvent.getChannelInfo(),
                                bufferOrEvent.moreAvailable(),
                                bufferOrEvent.morePriorityEvents());
            }
            ((RemoteInputChannel)
                            inputGate.getChannel(
                                    bufferOrEvent.getChannelInfo().getInputChannelIdx()))
                    .onBuffer(
                            bufferOrEvent.getBuffer(),
                            sequenceNumbers[bufferOrEvent.getChannelInfo().getInputChannelIdx()]++,
                            0,
                            0);

            while (inputGate.pollNext().map(output::add).isPresent()) {}
        }
        return sequence;
    }

    private CheckpointedInputGate createCheckpointedInputGate(
            IndexedInputGate gate, AbstractInvokable toNotify) {
        return createCheckpointedInputGate(gate, toNotify, true);
    }

    private CheckpointedInputGate createCheckpointedInputGate(
            IndexedInputGate gate,
            AbstractInvokable toNotify,
            boolean enableCheckpointsAfterTasksFinished) {
        final SingleCheckpointBarrierHandler barrierHandler =
                SingleCheckpointBarrierHandler.createUnalignedCheckpointBarrierHandler(
                        new TestSubtaskCheckpointCoordinator(channelStateWriter),
                        "Test",
                        toNotify,
                        SystemClock.getInstance(),
                        enableCheckpointsAfterTasksFinished,
                        gate);
        return new CheckpointedInputGate(gate, barrierHandler, new SyncMailboxExecutor());
    }

    private void assertInflightData(BufferOrEvent... expected) {
        Collection<BufferOrEvent> andResetInflightData = getAndResetInflightData();
        assertThat(getIds(andResetInflightData))
                .as("Unexpected in-flight sequence: " + andResetInflightData)
                .isEqualTo(getIds(Arrays.asList(expected)));
    }

    private Collection<BufferOrEvent> getAndResetInflightData() {
        final List<BufferOrEvent> inflightData =
                channelStateWriter.getAddedInput().entries().stream()
                        .map(entry -> new BufferOrEvent(entry.getValue(), entry.getKey()))
                        .collect(Collectors.toList());
        channelStateWriter.reset();
        return inflightData;
    }

    private void assertOutput(BufferOrEvent... expectedSequence) {
        assertThat(getIds(output))
                .as("Unexpected output sequence")
                .isEqualTo(getIds(Arrays.asList(expectedSequence)));
    }

    private List<Object> getIds(Collection<BufferOrEvent> buffers) {
        return buffers.stream()
                .filter(
                        boe ->
                                !boe.isEvent()
                                        || !(boe.getEvent() instanceof CheckpointBarrier
                                                || boe.getEvent()
                                                        instanceof CancelCheckpointMarker))
                .map(boe -> boe.isBuffer() ? boe.getSize() - 1 : boe.getEvent())
                .collect(Collectors.toList());
    }

    private CheckpointBarrier buildCheckpointBarrier(long id) {
        return new CheckpointBarrier(
                id, 0, CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault()));
    }

    // ------------------------------------------------------------------------
    //  Testing Mocks
    // ------------------------------------------------------------------------

    /** The invokable handler used for triggering checkpoint and validation. */
    static class ValidatingCheckpointHandler
            extends org.apache.flink.streaming.runtime.io.checkpointing
                    .ValidatingCheckpointHandler {

        public ValidatingCheckpointHandler(long nextExpectedCheckpointId) {
            super(nextExpectedCheckpointId);
        }

        @Override
        public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) {
            super.abortCheckpointOnBarrier(checkpointId, cause);
            nextExpectedCheckpointId = -1;
        }
    }

    static class ValidateAsyncFutureNotCompleted extends ValidatingCheckpointHandler {
        private @Nullable CheckpointedInputGate inputGate;

        public ValidateAsyncFutureNotCompleted(long nextExpectedCheckpointId) {
            super(nextExpectedCheckpointId);
        }

        @Override
        public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) {
            super.abortCheckpointOnBarrier(checkpointId, cause);
            checkState(inputGate != null);
            assertThat(inputGate.getAllBarriersReceivedFuture(checkpointId)).isNotDone();
        }

        public void setInputGate(CheckpointedInputGate inputGate) {
            this.inputGate = inputGate;
        }
    }

    /**
     * Specific {@link AbstractInvokable} implementation to record and validate which checkpoint id
     * is executed and how many checkpoints are executed.
     */
    private static final class ValidatingCheckpointInvokable extends StreamTask {

        private long expectedCheckpointId;

        private int totalNumCheckpoints;

        private long abortedCheckpointId;

        ValidatingCheckpointInvokable() throws Exception {
            super(new DummyEnvironment("test", 1, 0));
        }

        @Override
        public void init() {}

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) {}

        @Override
        public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
                throws IOException {
            abortedCheckpointId = checkpointId;
        }

        public void triggerCheckpointOnBarrier(
                CheckpointMetaData checkpointMetaData,
                CheckpointOptions checkpointOptions,
                CheckpointMetricsBuilder checkpointMetrics) {
            expectedCheckpointId = checkpointMetaData.getCheckpointId();
            totalNumCheckpoints++;
        }

        long getTriggeredCheckpointId() {
            return expectedCheckpointId;
        }

        int getTotalTriggeredCheckpoints() {
            return totalNumCheckpoints;
        }

        long getAbortedCheckpointId() {
            return abortedCheckpointId;
        }
    }
}
