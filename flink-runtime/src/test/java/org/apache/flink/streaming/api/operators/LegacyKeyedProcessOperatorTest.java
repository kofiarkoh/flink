/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link LegacyKeyedProcessOperator}. */
@Deprecated
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class LegacyKeyedProcessOperatorTest {

    @Test
    void testTimestampAndWatermarkQuerying() throws Exception {

        LegacyKeyedProcessOperator<Integer, Integer, String> operator =
                new LegacyKeyedProcessOperator<>(
                        new QueryingFlatMapFunction(TimeDomain.EVENT_TIME));

        OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processWatermark(new Watermark(17));
        testHarness.processElement(new StreamRecord<>(5, 12L));

        testHarness.processWatermark(new Watermark(42));
        testHarness.processElement(new StreamRecord<>(6, 13L));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new Watermark(17L));
        expectedOutput.add(new StreamRecord<>("5TIME:17 TS:12", 12L));
        expectedOutput.add(new Watermark(42L));
        expectedOutput.add(new StreamRecord<>("6TIME:42 TS:13", 13L));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testTimestampAndProcessingTimeQuerying() throws Exception {

        LegacyKeyedProcessOperator<Integer, Integer, String> operator =
                new LegacyKeyedProcessOperator<>(
                        new QueryingFlatMapFunction(TimeDomain.PROCESSING_TIME));

        OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.setProcessingTime(17);
        testHarness.processElement(new StreamRecord<>(5));

        testHarness.setProcessingTime(42);
        testHarness.processElement(new StreamRecord<>(6));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("5TIME:17 TS:null"));
        expectedOutput.add(new StreamRecord<>("6TIME:42 TS:null"));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testEventTimeTimers() throws Exception {

        LegacyKeyedProcessOperator<Integer, Integer, Integer> operator =
                new LegacyKeyedProcessOperator<>(
                        new TriggeringFlatMapFunction(TimeDomain.EVENT_TIME));

        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processWatermark(new Watermark(0));

        testHarness.processElement(new StreamRecord<>(17, 42L));

        testHarness.processWatermark(new Watermark(5));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new Watermark(0L));
        expectedOutput.add(new StreamRecord<>(17, 42L));
        expectedOutput.add(new StreamRecord<>(1777, 5L));
        expectedOutput.add(new Watermark(5L));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testProcessingTimeTimers() throws Exception {

        LegacyKeyedProcessOperator<Integer, Integer, Integer> operator =
                new LegacyKeyedProcessOperator<>(
                        new TriggeringFlatMapFunction(TimeDomain.PROCESSING_TIME));

        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(17));

        testHarness.setProcessingTime(5);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>(17));
        expectedOutput.add(new StreamRecord<>(1777));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    /** Verifies that we don't have leakage between different keys. */
    @Test
    void testEventTimeTimerWithState() throws Exception {

        LegacyKeyedProcessOperator<Integer, Integer, String> operator =
                new LegacyKeyedProcessOperator<>(
                        new TriggeringStatefulFlatMapFunction(TimeDomain.EVENT_TIME));

        OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processWatermark(new Watermark(1));
        testHarness.processElement(new StreamRecord<>(17, 0L)); // should set timer for 6

        testHarness.processWatermark(new Watermark(2));
        testHarness.processElement(new StreamRecord<>(42, 1L)); // should set timer for 7

        testHarness.processWatermark(new Watermark(6));
        testHarness.processWatermark(new Watermark(7));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new Watermark(1L));
        expectedOutput.add(new StreamRecord<>("INPUT:17", 0L));
        expectedOutput.add(new Watermark(2L));
        expectedOutput.add(new StreamRecord<>("INPUT:42", 1L));
        expectedOutput.add(new StreamRecord<>("STATE:17", 6L));
        expectedOutput.add(new Watermark(6L));
        expectedOutput.add(new StreamRecord<>("STATE:42", 7L));
        expectedOutput.add(new Watermark(7L));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    /** Verifies that we don't have leakage between different keys. */
    @Test
    void testProcessingTimeTimerWithState() throws Exception {

        LegacyKeyedProcessOperator<Integer, Integer, String> operator =
                new LegacyKeyedProcessOperator<>(
                        new TriggeringStatefulFlatMapFunction(TimeDomain.PROCESSING_TIME));

        OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.setProcessingTime(1);
        testHarness.processElement(new StreamRecord<>(17)); // should set timer for 6

        testHarness.setProcessingTime(2);
        testHarness.processElement(new StreamRecord<>(42)); // should set timer for 7

        testHarness.setProcessingTime(6);
        testHarness.setProcessingTime(7);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("INPUT:17"));
        expectedOutput.add(new StreamRecord<>("INPUT:42"));
        expectedOutput.add(new StreamRecord<>("STATE:17"));
        expectedOutput.add(new StreamRecord<>("STATE:42"));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testSnapshotAndRestore() throws Exception {

        LegacyKeyedProcessOperator<Integer, Integer, String> operator =
                new LegacyKeyedProcessOperator<>(new BothTriggeringFlatMapFunction());

        OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(5, 12L));

        // snapshot and restore from scratch
        OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

        testHarness.close();

        operator = new LegacyKeyedProcessOperator<>(new BothTriggeringFlatMapFunction());

        testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.setProcessingTime(5);
        testHarness.processWatermark(new Watermark(6));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("PROC:1777"));
        expectedOutput.add(new StreamRecord<>("EVENT:1777", 6L));
        expectedOutput.add(new Watermark(6));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testNullOutputTagRefusal() throws Exception {
        LegacyKeyedProcessOperator<Integer, Integer, String> operator =
                new LegacyKeyedProcessOperator<>(new NullOutputTagEmittingProcessFunction());

        OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.setProcessingTime(17);
        try {
            assertThatThrownBy(() -> testHarness.processElement(new StreamRecord<>(5)))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            testHarness.close();
        }
    }

    /** This also verifies that the timestamps ouf side-emitted records is correct. */
    @Test
    void testSideOutput() throws Exception {
        LegacyKeyedProcessOperator<Integer, Integer, String> operator =
                new LegacyKeyedProcessOperator<>(new SideOutputProcessFunction());

        OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(42, 17L /* timestamp */));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("IN:42", 17L /* timestamp */));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        ConcurrentLinkedQueue<StreamRecord<Integer>> expectedIntSideOutput =
                new ConcurrentLinkedQueue<>();
        expectedIntSideOutput.add(new StreamRecord<>(42, 17L /* timestamp */));
        ConcurrentLinkedQueue<StreamRecord<Integer>> intSideOutput =
                testHarness.getSideOutput(SideOutputProcessFunction.INTEGER_OUTPUT_TAG);
        TestHarnessUtil.assertOutputEquals(
                "Side output was not correct.", expectedIntSideOutput, intSideOutput);

        ConcurrentLinkedQueue<StreamRecord<Long>> expectedLongSideOutput =
                new ConcurrentLinkedQueue<>();
        expectedLongSideOutput.add(new StreamRecord<>(42L, 17L /* timestamp */));
        ConcurrentLinkedQueue<StreamRecord<Long>> longSideOutput =
                testHarness.getSideOutput(SideOutputProcessFunction.LONG_OUTPUT_TAG);
        TestHarnessUtil.assertOutputEquals(
                "Side output was not correct.", expectedLongSideOutput, longSideOutput);

        testHarness.close();
    }

    private static class NullOutputTagEmittingProcessFunction
            extends ProcessFunction<Integer, String> {

        @Override
        public void processElement(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            ctx.output(null, value);
        }
    }

    private static class SideOutputProcessFunction extends ProcessFunction<Integer, String> {

        static final OutputTag<Integer> INTEGER_OUTPUT_TAG = new OutputTag<Integer>("int-out") {};
        static final OutputTag<Long> LONG_OUTPUT_TAG = new OutputTag<Long>("long-out") {};

        @Override
        public void processElement(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect("IN:" + value);

            ctx.output(INTEGER_OUTPUT_TAG, value);
            ctx.output(LONG_OUTPUT_TAG, value.longValue());
        }
    }

    private static class IdentityKeySelector<T> implements KeySelector<T, T> {
        private static final long serialVersionUID = 1L;

        @Override
        public T getKey(T value) throws Exception {
            return value;
        }
    }

    private static class QueryingFlatMapFunction extends ProcessFunction<Integer, String> {

        private static final long serialVersionUID = 1L;

        private final TimeDomain timeDomain;

        public QueryingFlatMapFunction(TimeDomain timeDomain) {
            this.timeDomain = timeDomain;
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
                out.collect(
                        value
                                + "TIME:"
                                + ctx.timerService().currentWatermark()
                                + " TS:"
                                + ctx.timestamp());
            } else {
                out.collect(
                        value
                                + "TIME:"
                                + ctx.timerService().currentProcessingTime()
                                + " TS:"
                                + ctx.timestamp());
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            // Do nothing
        }
    }

    private static class TriggeringFlatMapFunction extends ProcessFunction<Integer, Integer> {

        private static final long serialVersionUID = 1L;

        private final TimeDomain timeDomain;

        public TriggeringFlatMapFunction(TimeDomain timeDomain) {
            this.timeDomain = timeDomain;
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Integer> out)
                throws Exception {
            out.collect(value);
            if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
                ctx.timerService()
                        .registerEventTimeTimer(ctx.timerService().currentWatermark() + 5);
            } else {
                ctx.timerService()
                        .registerProcessingTimeTimer(
                                ctx.timerService().currentProcessingTime() + 5);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out)
                throws Exception {
            assertThat(ctx.timeDomain()).isEqualTo(timeDomain);
            out.collect(1777);
        }
    }

    private static class TriggeringStatefulFlatMapFunction
            extends ProcessFunction<Integer, String> {

        private static final long serialVersionUID = 1L;

        private final ValueStateDescriptor<Integer> state =
                new ValueStateDescriptor<>("seen-element", IntSerializer.INSTANCE);

        private final TimeDomain timeDomain;

        public TriggeringStatefulFlatMapFunction(TimeDomain timeDomain) {
            this.timeDomain = timeDomain;
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect("INPUT:" + value);
            getRuntimeContext().getState(state).update(value);
            if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
                ctx.timerService()
                        .registerEventTimeTimer(ctx.timerService().currentWatermark() + 5);
            } else {
                ctx.timerService()
                        .registerProcessingTimeTimer(
                                ctx.timerService().currentProcessingTime() + 5);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            assertThat(ctx.timeDomain()).isEqualTo(timeDomain);
            out.collect("STATE:" + getRuntimeContext().getState(state).value());
        }
    }

    private static class BothTriggeringFlatMapFunction extends ProcessFunction<Integer, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            ctx.timerService().registerProcessingTimeTimer(5);
            ctx.timerService().registerEventTimeTimer(6);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            if (TimeDomain.EVENT_TIME.equals(ctx.timeDomain())) {
                out.collect("EVENT:1777");
            } else {
                out.collect("PROC:1777");
            }
        }
    }
}
