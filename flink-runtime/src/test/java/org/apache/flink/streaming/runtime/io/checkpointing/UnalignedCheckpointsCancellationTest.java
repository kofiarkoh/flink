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
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.clock.SystemClock;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unaligned checkpoints cancellation test. */
@ExtendWith(ParameterizedTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class UnalignedCheckpointsCancellationTest {
    private final List<RuntimeEvent> events;
    private final boolean expectTriggerCheckpoint;
    private final boolean expectAbortCheckpoint;
    private final int numChannels;
    private final int channel;

    UnalignedCheckpointsCancellationTest(
            boolean expectTriggerCheckpoint,
            boolean expectAbortCheckpoint,
            List<RuntimeEvent> events,
            int numChannels,
            int channel) {
        this.events = events;
        this.expectTriggerCheckpoint = expectTriggerCheckpoint;
        this.expectAbortCheckpoint = expectAbortCheckpoint;
        this.numChannels = numChannels;
        this.channel = channel;
    }

    @Parameters(
            name =
                    "expect trigger: {0}, expect abort {1}, numChannels: {3}, chan: {4}, events: {2}")
    private static Object[][] parameters() {
        return new Object[][] {
            new Object[] {false, true, Arrays.asList(cancel(10), cancel(20)), 1, 0},
            new Object[] {false, true, Arrays.asList(cancel(20), cancel(10)), 1, 0},
            new Object[] {false, true, Arrays.asList(cancel(10), checkpoint(10)), 1, 0},
            new Object[] {true, true, Arrays.asList(cancel(10), checkpoint(20)), 1, 0},
            new Object[] {false, true, Arrays.asList(cancel(20), checkpoint(10)), 1, 0},
            new Object[] {true, false, Arrays.asList(checkpoint(10), checkpoint(10)), 1, 0},
            new Object[] {true, false, Arrays.asList(checkpoint(10), checkpoint(20)), 1, 0},
            new Object[] {true, true, Arrays.asList(checkpoint(10), checkpoint(20)), 2, 0},
            new Object[] {true, false, Arrays.asList(checkpoint(20), checkpoint(10)), 1, 0},
            new Object[] {true, false, Arrays.asList(checkpoint(10), cancel(10)), 1, 0},
            new Object[] {true, true, Arrays.asList(checkpoint(10), cancel(10)), 2, 0},
            new Object[] {true, true, Arrays.asList(checkpoint(10), cancel(20)), 1, 0},
            new Object[] {true, false, Arrays.asList(checkpoint(20), cancel(10)), 1, 0},
        };
    }

    @TestTemplate
    void test() throws Exception {
        TestInvokable invokable = new TestInvokable();
        final SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(numChannels)
                        .setChannelFactory(InputChannelBuilder::buildLocalChannel)
                        .build();
        SingleCheckpointBarrierHandler unaligner =
                SingleCheckpointBarrierHandler.createUnalignedCheckpointBarrierHandler(
                        TestSubtaskCheckpointCoordinator.INSTANCE,
                        "test",
                        invokable,
                        SystemClock.getInstance(),
                        true,
                        inputGate);

        for (RuntimeEvent e : events) {
            if (e instanceof CancelCheckpointMarker) {
                unaligner.processCancellationBarrier(
                        (CancelCheckpointMarker) e, new InputChannelInfo(0, channel));
            } else if (e instanceof CheckpointBarrier) {
                unaligner.processBarrier(
                        (CheckpointBarrier) e, new InputChannelInfo(0, channel), false);
            } else {
                throw new IllegalArgumentException("unexpected event type: " + e);
            }
        }

        assertThat(invokable.checkpointAborted).isEqualTo(expectAbortCheckpoint);
        assertThat(invokable.checkpointTriggered).isEqualTo(expectTriggerCheckpoint);
    }

    private static CheckpointBarrier checkpoint(int checkpointId) {
        return new CheckpointBarrier(
                checkpointId,
                1,
                CheckpointOptions.forCheckpointWithDefaultLocation().toUnaligned());
    }

    private static CancelCheckpointMarker cancel(int checkpointId) {
        return new CancelCheckpointMarker(checkpointId);
    }

    private static class TestInvokable extends AbstractInvokable {
        TestInvokable() {
            super(new DummyEnvironment());
        }

        private boolean checkpointAborted;
        private boolean checkpointTriggered;

        @Override
        public void invoke() {}

        @Override
        public void triggerCheckpointOnBarrier(
                CheckpointMetaData checkpointMetaData,
                CheckpointOptions checkpointOptions,
                CheckpointMetricsBuilder checkpointMetrics) {
            checkpointTriggered = true;
        }

        @Override
        public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) {
            checkpointAborted = true;
        }
    }
}
