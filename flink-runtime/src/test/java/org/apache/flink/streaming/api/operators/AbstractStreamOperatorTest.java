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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Preconditions;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/**
 * Tests for the facilities provided by {@link AbstractStreamOperator}. This mostly tests timers and
 * state and whether they are correctly checkpointed/restored with key-group reshuffling.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class AbstractStreamOperatorTest {
    protected KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarness() throws Exception {
        return createTestHarness(1, 1, 0);
    }

    protected KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarness(int maxParalelism, int numSubtasks, int subtaskIndex)
                    throws Exception {
        TestOperator testOperator = new TestOperator();
        return new KeyedOneInputStreamOperatorTestHarness<>(
                testOperator,
                new TestKeySelector(),
                BasicTypeInfo.INT_TYPE_INFO,
                maxParalelism,
                numSubtasks,
                subtaskIndex);
    }

    protected <K, IN, OUT> KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> createTestHarness(
            int maxParalelism,
            int numSubtasks,
            int subtaskIndex,
            OneInputStreamOperator<IN, OUT> testOperator,
            KeySelector<IN, K> keySelector,
            TypeInformation<K> keyTypeInfo)
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                testOperator, keySelector, keyTypeInfo, maxParalelism, numSubtasks, subtaskIndex);
    }

    @Test
    void testStateDoesNotInterfere() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness()) {
            testHarness.open();

            testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);
            testHarness.processElement(new Tuple2<>(1, "SET_STATE:CIAO"), 0);

            testHarness.processElement(new Tuple2<>(1, "EMIT_STATE"), 0);
            testHarness.processElement(new Tuple2<>(0, "EMIT_STATE"), 0);

            assertThat(extractResult(testHarness))
                    .contains("ON_ELEMENT:1:CIAO", "ON_ELEMENT:0:HELLO");
        }
    }

    /**
     * Verify that firing event-time timers see the state of the key that was active when the timer
     * was set.
     */
    @Test
    void testEventTimeTimersDontInterfere() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness()) {
            testHarness.open();

            testHarness.processWatermark(0L);

            testHarness.processElement(new Tuple2<>(1, "SET_EVENT_TIME_TIMER:20"), 0);

            testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);
            testHarness.processElement(new Tuple2<>(1, "SET_STATE:CIAO"), 0);

            testHarness.processElement(new Tuple2<>(0, "SET_EVENT_TIME_TIMER:10"), 0);

            testHarness.processWatermark(10L);

            assertThat(extractResult(testHarness)).contains("ON_EVENT_TIME:HELLO");

            testHarness.processWatermark(20L);

            assertThat(extractResult(testHarness)).contains("ON_EVENT_TIME:CIAO");
        }
    }

    /**
     * Verify that firing processing-time timers see the state of the key that was active when the
     * timer was set.
     */
    @Test
    void testProcessingTimeTimersDontInterfere() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness()) {
            testHarness.open();

            testHarness.setProcessingTime(0L);

            testHarness.processElement(new Tuple2<>(1, "SET_PROC_TIME_TIMER:20"), 0);

            testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);
            testHarness.processElement(new Tuple2<>(1, "SET_STATE:CIAO"), 0);

            testHarness.processElement(new Tuple2<>(0, "SET_PROC_TIME_TIMER:10"), 0);

            testHarness.setProcessingTime(10L);

            assertThat(extractResult(testHarness)).contains("ON_PROC_TIME:HELLO");

            testHarness.setProcessingTime(20L);

            assertThat(extractResult(testHarness)).contains("ON_PROC_TIME:CIAO");
        }
    }

    /** Verify that a low-level timer is set for processing-time timers in case of restore. */
    @Test
    void testEnsureProcessingTimeTimerRegisteredOnRestore() throws Exception {
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness()) {
            testHarness.open();

            testHarness.setProcessingTime(0L);

            testHarness.processElement(new Tuple2<>(1, "SET_PROC_TIME_TIMER:20"), 0);

            testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);
            testHarness.processElement(new Tuple2<>(1, "SET_STATE:CIAO"), 0);

            testHarness.processElement(new Tuple2<>(0, "SET_PROC_TIME_TIMER:10"), 0);

            snapshot = testHarness.snapshot(0, 0);
        }

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness1 = createTestHarness()) {
            testHarness1.setProcessingTime(0L);

            testHarness1.setup();
            testHarness1.initializeState(snapshot);
            testHarness1.open();

            testHarness1.setProcessingTime(10L);

            assertThat(extractResult(testHarness1)).contains("ON_PROC_TIME:HELLO");

            testHarness1.setProcessingTime(20L);

            assertThat(extractResult(testHarness1)).contains("ON_PROC_TIME:CIAO");
        }
    }

    /** Verify that timers for the different time domains don't clash. */
    @Test
    void testProcessingTimeAndEventTimeDontInterfere() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness()) {
            testHarness.open();

            testHarness.setProcessingTime(0L);
            testHarness.processWatermark(0L);

            testHarness.processElement(new Tuple2<>(0, "SET_PROC_TIME_TIMER:10"), 0);
            testHarness.processElement(new Tuple2<>(0, "SET_EVENT_TIME_TIMER:20"), 0);

            testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);

            testHarness.processWatermark(20L);

            assertThat(extractResult(testHarness)).contains("ON_EVENT_TIME:HELLO");

            testHarness.setProcessingTime(10L);

            assertThat(extractResult(testHarness)).contains("ON_PROC_TIME:HELLO");
        }
    }

    /**
     * Verify that state and timers are checkpointed per key group and that they are correctly
     * assigned to operator subtasks when restoring.
     */
    @Test
    void testStateAndTimerStateShufflingScalingUp() throws Exception {
        final int maxParallelism = 10;

        // first get two keys that will fall into different key-group ranges that go
        // to different operator subtasks when we restore

        // get two sub key-ranges so that we can restore two ranges separately
        KeyGroupRange subKeyGroupRange1 = new KeyGroupRange(0, (maxParallelism / 2) - 1);
        KeyGroupRange subKeyGroupRange2 =
                new KeyGroupRange(subKeyGroupRange1.getEndKeyGroup() + 1, maxParallelism - 1);

        // get two different keys, one per sub range
        int key1 = getKeyInKeyGroupRange(subKeyGroupRange1, maxParallelism);
        int key2 = getKeyInKeyGroupRange(subKeyGroupRange2, maxParallelism);

        OperatorSubtaskState snapshot;

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(maxParallelism, 1, 0)) {
            testHarness.open();

            testHarness.processWatermark(0L);
            testHarness.setProcessingTime(0L);

            testHarness.processElement(new Tuple2<>(key1, "SET_EVENT_TIME_TIMER:10"), 0);
            testHarness.processElement(new Tuple2<>(key2, "SET_EVENT_TIME_TIMER:20"), 0);

            testHarness.processElement(new Tuple2<>(key1, "SET_PROC_TIME_TIMER:10"), 0);
            testHarness.processElement(new Tuple2<>(key2, "SET_PROC_TIME_TIMER:20"), 0);

            testHarness.processElement(new Tuple2<>(key1, "SET_STATE:HELLO"), 0);
            testHarness.processElement(new Tuple2<>(key2, "SET_STATE:CIAO"), 0);

            assertThat(extractResult(testHarness)).isEmpty();

            snapshot = testHarness.snapshot(0, 0);
        }

        // now, restore in two operators, first operator 1
        OperatorSubtaskState initState1 =
                AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        snapshot, maxParallelism, 1, 2, 0);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness1 = createTestHarness(maxParallelism, 2, 0)) {
            testHarness1.setup();
            testHarness1.initializeState(initState1);
            testHarness1.open();

            testHarness1.processWatermark(10L);

            assertThat(extractResult(testHarness1)).contains("ON_EVENT_TIME:HELLO");

            assertThat(extractResult(testHarness1)).isEmpty();

            // this should not trigger anything, the trigger for WM=20 should sit in the
            // other operator subtask
            testHarness1.processWatermark(20L);

            assertThat(extractResult(testHarness1)).isEmpty();

            testHarness1.setProcessingTime(10L);

            assertThat(extractResult(testHarness1)).contains("ON_PROC_TIME:HELLO");

            assertThat(extractResult(testHarness1)).isEmpty();

            // this should not trigger anything, the trigger for TIME=20 should sit in the
            // other operator subtask
            testHarness1.setProcessingTime(20L);

            assertThat(extractResult(testHarness1)).isEmpty();
        }

        // now, for the second operator
        OperatorSubtaskState initState2 =
                AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        snapshot, maxParallelism, 1, 2, 1);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness2 = createTestHarness(maxParallelism, 2, 1)) {
            testHarness2.setup();
            testHarness2.initializeState(initState2);
            testHarness2.open();

            testHarness2.processWatermark(10L);

            // nothing should happen because this timer is in the other subtask
            assertThat(extractResult(testHarness2)).isEmpty();

            testHarness2.processWatermark(20L);

            assertThat(extractResult(testHarness2)).contains("ON_EVENT_TIME:CIAO");

            testHarness2.setProcessingTime(10L);

            // nothing should happen because this timer is in the other subtask
            assertThat(extractResult(testHarness2)).isEmpty();

            testHarness2.setProcessingTime(20L);

            assertThat(extractResult(testHarness2)).contains("ON_PROC_TIME:CIAO");

            assertThat(extractResult(testHarness2)).isEmpty();
        }
    }

    @Test
    void testStateAndTimerStateShufflingScalingDown() throws Exception {
        final int maxParallelism = 10;

        // first get two keys that will fall into different key-group ranges that go
        // to different operator subtasks when we restore

        // get two sub key-ranges so that we can restore two ranges separately
        KeyGroupRange subKeyGroupRange1 = new KeyGroupRange(0, (maxParallelism / 2) - 1);
        KeyGroupRange subKeyGroupRange2 =
                new KeyGroupRange(subKeyGroupRange1.getEndKeyGroup() + 1, maxParallelism - 1);

        // get two different keys, one per sub range
        int key1 = getKeyInKeyGroupRange(subKeyGroupRange1, maxParallelism);
        int key2 = getKeyInKeyGroupRange(subKeyGroupRange2, maxParallelism);

        OperatorSubtaskState snapshot1, snapshot2;
        // register some state with both instances and scale down to parallelism 1
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness1 = createTestHarness(maxParallelism, 2, 0)) {

            testHarness1.setup();
            testHarness1.open();

            testHarness1.processWatermark(0L);
            testHarness1.setProcessingTime(0L);

            testHarness1.processElement(new Tuple2<>(key1, "SET_EVENT_TIME_TIMER:30"), 0);
            testHarness1.processElement(new Tuple2<>(key1, "SET_PROC_TIME_TIMER:30"), 0);
            testHarness1.processElement(new Tuple2<>(key1, "SET_STATE:HELLO"), 0);

            snapshot1 = testHarness1.snapshot(0, 0);
        }

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness2 = createTestHarness(maxParallelism, 2, 1)) {
            testHarness2.setup();
            testHarness2.open();

            testHarness2.processWatermark(0L);
            testHarness2.setProcessingTime(0L);

            testHarness2.processElement(new Tuple2<>(key2, "SET_EVENT_TIME_TIMER:40"), 0);
            testHarness2.processElement(new Tuple2<>(key2, "SET_PROC_TIME_TIMER:40"), 0);
            testHarness2.processElement(new Tuple2<>(key2, "SET_STATE:CIAO"), 0);

            snapshot2 = testHarness2.snapshot(0, 0);
        }
        // take a snapshot from each one of the "parallel" instances of the operator
        // and combine them into one so that we can scale down

        OperatorSubtaskState repackagedState =
                AbstractStreamOperatorTestHarness.repackageState(snapshot1, snapshot2);

        OperatorSubtaskState initSubTaskState =
                AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        repackagedState, maxParallelism, 2, 1, 0);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness3 = createTestHarness(maxParallelism, 1, 0)) {
            testHarness3.setup();
            testHarness3.initializeState(initSubTaskState);
            testHarness3.open();

            testHarness3.processWatermark(30L);
            assertThat(extractResult(testHarness3)).contains("ON_EVENT_TIME:HELLO");
            assertThat(extractResult(testHarness3)).isEmpty();

            testHarness3.processWatermark(40L);
            assertThat(extractResult(testHarness3)).contains("ON_EVENT_TIME:CIAO");
            assertThat(extractResult(testHarness3)).isEmpty();

            testHarness3.setProcessingTime(30L);
            assertThat(extractResult(testHarness3)).contains("ON_PROC_TIME:HELLO");
            assertThat(extractResult(testHarness3)).isEmpty();

            testHarness3.setProcessingTime(40L);
            assertThat(extractResult(testHarness3)).contains("ON_PROC_TIME:CIAO");
            assertThat(extractResult(testHarness3)).isEmpty();
        }
    }

    @Test
    void testCustomRawKeyedStateSnapshotAndRestore() throws Exception {
        // setup: 10 key groups, all assigned to single subtask
        final int maxParallelism = 10;
        final int numSubtasks = 1;
        final int subtaskIndex = 0;
        final List<Integer> keyGroupsToWrite = Arrays.asList(2, 3, 8);

        final byte[] testSnapshotData = "TEST".getBytes(StandardCharsets.UTF_8);
        final CustomRawKeyedStateTestOperator testOperator =
                new CustomRawKeyedStateTestOperator(testSnapshotData, keyGroupsToWrite);

        // snapshot and then restore
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness =
                createTestHarness(
                        maxParallelism,
                        numSubtasks,
                        subtaskIndex,
                        testOperator,
                        input -> input,
                        BasicTypeInfo.STRING_TYPE_INFO)) {
            testHarness.setup();
            testHarness.open();
            snapshot = testHarness.snapshot(0, 0);
        }

        try (KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness =
                createTestHarness(
                        maxParallelism,
                        numSubtasks,
                        subtaskIndex,
                        testOperator,
                        input -> input,
                        BasicTypeInfo.STRING_TYPE_INFO)) {
            testHarness.setup();
            testHarness.initializeState(snapshot);
            testHarness.open();
        }

        assertThat(testOperator.restoredRawKeyedState)
                .is(matching(hasRestoredKeyGroupsWith(testSnapshotData, keyGroupsToWrite)));
    }

    @Test
    void testIdleWatermarkHandling() throws Exception {
        final WatermarkTestingOperator testOperator = new WatermarkTestingOperator();

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, Long, Long, Long> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        testOperator,
                        dummyKeySelector,
                        dummyKeySelector,
                        BasicTypeInfo.INT_TYPE_INFO)) {
            testHarness.setup();
            testHarness.open();
            testHarness.processElement1(1L, 1L);
            testHarness.processElement1(3L, 3L);
            testHarness.processElement1(4L, 4L);
            testHarness.processWatermark1(new Watermark(1L));
            assertThat(testHarness.getOutput()).isEmpty();

            testHarness.processWatermarkStatus2(WatermarkStatus.IDLE);
            expectedOutput.add(new StreamRecord<>(1L));
            expectedOutput.add(new Watermark(1L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());

            testHarness.processWatermark1(new Watermark(3L));
            expectedOutput.add(new StreamRecord<>(3L));
            expectedOutput.add(new Watermark(3L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());

            testHarness.processWatermarkStatus2(WatermarkStatus.ACTIVE);
            // the other input is active now, we should not emit the watermark
            testHarness.processWatermark1(new Watermark(4L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testIdlenessForwarding() throws Exception {
        final WatermarkTestingOperator testOperator = new WatermarkTestingOperator();
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, Long, Long, Long> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        testOperator,
                        dummyKeySelector,
                        dummyKeySelector,
                        BasicTypeInfo.INT_TYPE_INFO)) {
            testHarness.setup();
            testHarness.open();

            testHarness.processWatermarkStatus1(WatermarkStatus.IDLE);
            testHarness.processWatermarkStatus2(WatermarkStatus.IDLE);
            expectedOutput.add(WatermarkStatus.IDLE);
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testTwoInputsRecordAttributesForwarding() throws Exception {
        final WatermarkTestingOperator testOperator = new WatermarkTestingOperator();
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, Long, Long, Long> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        testOperator,
                        dummyKeySelector,
                        dummyKeySelector,
                        BasicTypeInfo.INT_TYPE_INFO)) {
            testHarness.setup();
            testHarness.open();

            final RecordAttributes backlogRecordAttributes =
                    new RecordAttributesBuilder(Collections.emptyList()).setBacklog(true).build();
            final RecordAttributes nonBacklogRecordAttributes =
                    new RecordAttributesBuilder(Collections.emptyList()).setBacklog(false).build();

            testHarness.processRecordAttributes1(backlogRecordAttributes);
            testHarness.processRecordAttributes2(backlogRecordAttributes);
            expectedOutput.add(backlogRecordAttributes);
            expectedOutput.add(backlogRecordAttributes);
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
            testHarness.processRecordAttributes1(nonBacklogRecordAttributes);
            testHarness.processRecordAttributes2(nonBacklogRecordAttributes);
            expectedOutput.add(backlogRecordAttributes);
            expectedOutput.add(nonBacklogRecordAttributes);
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testOneInputRecordAttributesForwarding() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness()) {
            testHarness.open();

            final RecordAttributes backlogRecordAttributes =
                    new RecordAttributesBuilder(Collections.emptyList()).setBacklog(true).build();
            final RecordAttributes nonBacklogRecordAttributes =
                    new RecordAttributesBuilder(Collections.emptyList()).setBacklog(false).build();

            testHarness.processRecordAttributes(backlogRecordAttributes);
            testHarness.processRecordAttributes(nonBacklogRecordAttributes);
            expectedOutput.add(backlogRecordAttributes);
            expectedOutput.add(nonBacklogRecordAttributes);

            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    /** Extracts the result values form the test harness and clear the output queue. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T> List<T> extractResult(OneInputStreamOperatorTestHarness<?, T> testHarness) {
        List<StreamRecord<? extends T>> streamRecords = testHarness.extractOutputStreamRecords();
        List<T> result = new ArrayList<>();
        for (Object in : streamRecords) {
            if (in instanceof StreamRecord) {
                result.add((T) ((StreamRecord) in).getValue());
            }
        }
        testHarness.getOutput().clear();
        return result;
    }

    /** {@link KeySelector} for tests. */
    protected static class TestKeySelector
            implements KeySelector<Tuple2<Integer, String>, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(Tuple2<Integer, String> value) throws Exception {
            return value.f0;
        }
    }

    private static @ExtendWith(CTestJUnit5Extension.class) @CTestClass
    class WatermarkTestingOperator extends AbstractStreamOperator<Long>
            implements TwoInputStreamOperator<Long, Long, Long>,
                    Triggerable<Integer, VoidNamespace> {

        private transient InternalTimerService<VoidNamespace> timerService;

        @Override
        public void open() throws Exception {
            super.open();

            this.timerService =
                    getInternalTimerService("test-timers", VoidNamespaceSerializer.INSTANCE, this);
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            output.collect(new StreamRecord<>(timer.getTimestamp()));
        }

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer)
                throws Exception {}

        @Override
        public void processElement1(StreamRecord<Long> element) throws Exception {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, element.getValue());
        }

        @Override
        public void processElement2(StreamRecord<Long> element) throws Exception {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, element.getValue());
        }
    }

    /**
     * Testing operator that can respond to commands by either setting/deleting state, emitting
     * state or setting timers.
     */
    private static class TestOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<Tuple2<Integer, String>, String>,
                    Triggerable<Integer, VoidNamespace> {

        private static final long serialVersionUID = 1L;

        private transient InternalTimerService<VoidNamespace> timerService;

        private final ValueStateDescriptor<String> stateDescriptor =
                new ValueStateDescriptor<>("state", StringSerializer.INSTANCE);

        @Override
        public void open() throws Exception {
            super.open();

            this.timerService =
                    getInternalTimerService("test-timers", VoidNamespaceSerializer.INSTANCE, this);
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
            String[] command = element.getValue().f1.split(":");
            switch (command[0]) {
                case "SET_STATE":
                    getPartitionedState(stateDescriptor).update(command[1]);
                    break;
                case "DELETE_STATE":
                    getPartitionedState(stateDescriptor).clear();
                    break;
                case "SET_EVENT_TIME_TIMER":
                    timerService.registerEventTimeTimer(
                            VoidNamespace.INSTANCE, Long.parseLong(command[1]));
                    break;
                case "SET_PROC_TIME_TIMER":
                    timerService.registerProcessingTimeTimer(
                            VoidNamespace.INSTANCE, Long.parseLong(command[1]));
                    break;
                case "EMIT_STATE":
                    String stateValue = getPartitionedState(stateDescriptor).value();
                    output.collect(
                            new StreamRecord<>(
                                    "ON_ELEMENT:" + element.getValue().f0 + ":" + stateValue));
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            String stateValue = getPartitionedState(stateDescriptor).value();
            output.collect(new StreamRecord<>("ON_EVENT_TIME:" + stateValue));
        }

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            String stateValue = getPartitionedState(stateDescriptor).value();
            output.collect(new StreamRecord<>("ON_PROC_TIME:" + stateValue));
        }
    }

    /** Operator that writes arbitrary bytes to raw keyed state on snapshots. */
    private static @ExtendWith(CTestJUnit5Extension.class) @CTestClass
    class CustomRawKeyedStateTestOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        private static final long serialVersionUID = 1L;

        private final byte[] snapshotBytes;
        private final List<Integer> keyGroupsToWrite;

        private Map<Integer, byte[]> restoredRawKeyedState;

        CustomRawKeyedStateTestOperator(byte[] snapshotBytes, List<Integer> keyGroupsToWrite) {
            this.snapshotBytes = Arrays.copyOf(snapshotBytes, snapshotBytes.length);
            this.keyGroupsToWrite = Preconditions.checkNotNull(keyGroupsToWrite);
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            // do nothing
        }

        @Override
        protected boolean isUsingCustomRawKeyedState() {
            return true;
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            KeyedStateCheckpointOutputStream rawKeyedStateStream =
                    context.getRawKeyedOperatorStateOutput();
            for (int keyGroupId : keyGroupsToWrite) {
                rawKeyedStateStream.startNewKeyGroup(keyGroupId);
                rawKeyedStateStream.write(snapshotBytes);
            }
            rawKeyedStateStream.close();
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            restoredRawKeyedState = new HashMap<>();
            for (KeyGroupStatePartitionStreamProvider streamProvider :
                    context.getRawKeyedStateInputs()) {
                byte[] readBuffer = new byte[snapshotBytes.length];
                int ignored = streamProvider.getStream().read(readBuffer);
                restoredRawKeyedState.put(streamProvider.getKeyGroupId(), readBuffer);
            }
        }
    }

    private static int getKeyInKeyGroupRange(KeyGroupRange range, int maxParallelism) {
        Random rand = new Random(System.currentTimeMillis());
        int result = rand.nextInt();
        while (!range.contains(KeyGroupRangeAssignment.assignToKeyGroup(result, maxParallelism))) {
            result = rand.nextInt();
        }
        return result;
    }

    private static Matcher<Map<Integer, byte[]>> hasRestoredKeyGroupsWith(
            byte[] testSnapshotData, List<Integer> writtenKeyGroups) {
        return new TypeSafeMatcher<Map<Integer, byte[]>>() {
            @Override
            protected boolean matchesSafely(Map<Integer, byte[]> restored) {
                if (restored.size() != writtenKeyGroups.size()) {
                    return false;
                }

                for (int writtenKeyGroupId : writtenKeyGroups) {
                    if (!Arrays.equals(restored.get(writtenKeyGroupId), testSnapshotData)) {
                        return false;
                    }
                }

                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(
                        "Key groups: "
                                + writtenKeyGroups
                                + " with snapshot data "
                                + Arrays.toString(testSnapshotData));
            }
        };
    }
}
