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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * These tests verify that the api calls on {@link WindowedStream} that use the "time" shortcut
 * instantiate the correct window operator.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TimeWindowTranslationTest {

    /**
     * Verifies that calls to timeWindow() instantiate a regular windowOperator instead of an
     * aligned one.
     */
    @Test
    void testAlignedWindowDeprecation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(x -> x.f0)
                        .window(
                                TumblingProcessingTimeWindows.of(
                                        Duration.ofMillis(1000), Duration.ofMillis(100)))
                        .reduce(reducer);

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 =
                transform1.getOperator();
        assertThat(operator1).isInstanceOf(WindowOperator.class);

        DataStream<Tuple2<String, Integer>> window2 =
                source.keyBy(x -> x.f0)
                        .window(TumblingProcessingTimeWindows.of(Duration.ofMillis(1000)))
                        .apply(
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    @Override
                                    public void apply(
                                            String str,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out) {}
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform2 =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window2.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator2 =
                transform2.getOperator();
        assertThat(operator2).isInstanceOf(WindowOperator.class);
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceEventTimeWindows() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(x -> x.f0)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Duration.ofMillis(1000), Duration.ofMillis(100)))
                        .reduce(new DummyReducer());

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 =
                transform1.getOperator();
        assertThat(operator1).isInstanceOf(WindowOperator.class);
        WindowOperator winOperator1 = (WindowOperator) operator1;
        assertThat(winOperator1.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator1.getWindowAssigner()).isInstanceOf(SlidingEventTimeWindows.class);
        assertThat(winOperator1.getStateDescriptor()).isInstanceOf(ReducingStateDescriptor.class);
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testApplyEventTimeWindows() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(x -> x.f0)
                        .window(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
                        .apply(
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    @Override
                                    public void apply(
                                            String str,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out) {}
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 =
                transform1.getOperator();
        assertThat(operator1).isInstanceOf(WindowOperator.class);
        WindowOperator winOperator1 = (WindowOperator) operator1;
        assertThat(winOperator1.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator1.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator1.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);
    }

    // ------------------------------------------------------------------------
    //  UDFs
    // ------------------------------------------------------------------------

    private static class DummyReducer implements ReduceFunction<Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, Integer> reduce(
                Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            return value1;
        }
    }
}
