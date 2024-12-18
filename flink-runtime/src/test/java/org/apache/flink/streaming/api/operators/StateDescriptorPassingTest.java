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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorFactory;
import org.apache.flink.util.Collector;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Various tests around the proper passing of state descriptors to the operators and their
 * serialization.
 *
 * <p>The tests use an arbitrary generic type to validate the behavior.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StateDescriptorPassingTest {

    @Test
    void testReduceWindowState() {
        Configuration configuration = new Configuration();
        String serializerConfigStr =
                "{java.io.File: {type: kryo, kryo-type: registered, class: com.esotericsoftware.kryo.serializers.JavaSerializer}}";
        configuration.setString(PipelineOptions.SERIALIZATION_CONFIG.key(), serializerConfigStr);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        DataStream<File> src =
                env.fromData(new File("/"))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<File>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (file, ts) -> System.currentTimeMillis()));

        SingleOutputStreamOperator<?> result =
                src.keyBy(
                                new KeySelector<File, String>() {
                                    @Override
                                    public String getKey(File value) {
                                        return null;
                                    }
                                })
                        .window(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
                        .reduce(
                                new ReduceFunction<File>() {

                                    @Override
                                    public File reduce(File value1, File value2) {
                                        return null;
                                    }
                                });

        validateStateDescriptorConfigured(result);
    }

    @Test
    void testApplyWindowState() {
        Configuration configuration = new Configuration();
        String serializerConfigStr =
                "{java.io.File: {type: kryo, kryo-type: registered, class: com.esotericsoftware.kryo.serializers.JavaSerializer}}";
        configuration.setString(PipelineOptions.SERIALIZATION_CONFIG.key(), serializerConfigStr);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        DataStream<File> src =
                env.fromData(new File("/"))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<File>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (file, ts) -> System.currentTimeMillis()));

        SingleOutputStreamOperator<?> result =
                src.keyBy(
                                new KeySelector<File, String>() {
                                    @Override
                                    public String getKey(File value) {
                                        return null;
                                    }
                                })
                        .window(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
                        .apply(
                                new WindowFunction<File, String, String, TimeWindow>() {
                                    @Override
                                    public void apply(
                                            String s,
                                            TimeWindow window,
                                            Iterable<File> input,
                                            Collector<String> out) {}
                                });

        validateListStateDescriptorConfigured(result);
    }

    @Test
    void testProcessWindowState() {
        Configuration configuration = new Configuration();
        String serializerConfigStr =
                "{java.io.File: {type: kryo, kryo-type: registered, class: com.esotericsoftware.kryo.serializers.JavaSerializer}}";
        configuration.setString(PipelineOptions.SERIALIZATION_CONFIG.key(), serializerConfigStr);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        DataStream<File> src =
                env.fromData(new File("/"))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<File>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (file, ts) -> System.currentTimeMillis()));

        SingleOutputStreamOperator<?> result =
                src.keyBy(
                                new KeySelector<File, String>() {
                                    @Override
                                    public String getKey(File value) {
                                        return null;
                                    }
                                })
                        .window(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
                        .process(
                                new ProcessWindowFunction<File, String, String, TimeWindow>() {
                                    @Override
                                    public void process(
                                            String s,
                                            Context ctx,
                                            Iterable<File> input,
                                            Collector<String> out) {}
                                });

        validateListStateDescriptorConfigured(result);
    }

    @Test
    void testProcessAllWindowState() {
        Configuration configuration = new Configuration();
        String serializerConfigStr =
                "{java.io.File: {type: kryo, kryo-type: registered, class: com.esotericsoftware.kryo.serializers.JavaSerializer}}";
        configuration.setString(PipelineOptions.SERIALIZATION_CONFIG.key(), serializerConfigStr);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // simulate ingestion time
        DataStream<File> src =
                env.fromData(new File("/"))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<File>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (file, ts) -> System.currentTimeMillis()));

        SingleOutputStreamOperator<?> result =
                src.windowAll(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
                        .process(
                                new ProcessAllWindowFunction<File, String, TimeWindow>() {
                                    @Override
                                    public void process(
                                            Context ctx,
                                            Iterable<File> input,
                                            Collector<String> out) {}
                                });

        validateListStateDescriptorConfigured(result);
    }

    @Test
    void testReduceWindowAllState() {
        Configuration configuration = new Configuration();
        String serializerConfigStr =
                "{java.io.File: {type: kryo, kryo-type: registered, class: com.esotericsoftware.kryo.serializers.JavaSerializer}}";
        configuration.setString(PipelineOptions.SERIALIZATION_CONFIG.key(), serializerConfigStr);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // simulate ingestion time
        DataStream<File> src =
                env.fromData(new File("/"))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<File>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (file, ts) -> System.currentTimeMillis()));

        SingleOutputStreamOperator<?> result =
                src.windowAll(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
                        .reduce(
                                new ReduceFunction<File>() {

                                    @Override
                                    public File reduce(File value1, File value2) {
                                        return null;
                                    }
                                });

        validateStateDescriptorConfigured(result);
    }

    @Test
    void testApplyWindowAllState() {
        Configuration configuration = new Configuration();
        String serializerConfigStr =
                "{java.io.File: {type: kryo, kryo-type: registered, class: com.esotericsoftware.kryo.serializers.JavaSerializer}}";
        configuration.setString(PipelineOptions.SERIALIZATION_CONFIG.key(), serializerConfigStr);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // simulate ingestion time
        DataStream<File> src =
                env.fromData(new File("/"))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<File>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (file, ts) -> System.currentTimeMillis()));

        SingleOutputStreamOperator<?> result =
                src.windowAll(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
                        .apply(
                                new AllWindowFunction<File, String, TimeWindow>() {
                                    @Override
                                    public void apply(
                                            TimeWindow window,
                                            Iterable<File> input,
                                            Collector<String> out) {}
                                });

        validateListStateDescriptorConfigured(result);
    }

    // ------------------------------------------------------------------------
    //  generic validation
    // ------------------------------------------------------------------------

    private void validateStateDescriptorConfigured(SingleOutputStreamOperator<?> result) {
        OneInputTransformation<?, ?> transform =
                (OneInputTransformation<?, ?>) result.getTransformation();
        StreamOperatorFactory<?> factory = transform.getOperatorFactory();
        StateDescriptor<?, ?> descr;
        if (factory instanceof WindowOperatorFactory) {
            descr = ((WindowOperatorFactory<?, ?, ?, ?, ?>) factory).getStateDescriptor();
        } else {
            WindowOperator<?, ?, ?, ?, ?> op =
                    (WindowOperator<?, ?, ?, ?, ?>) transform.getOperator();
            descr = op.getStateDescriptor();
        }

        // this would be the first statement to fail if state descriptors were not properly
        // initialized
        TypeSerializer<?> serializer = descr.getSerializer();
        assertThat(serializer).isInstanceOf(KryoSerializer.class);

        Kryo kryo = ((KryoSerializer<?>) serializer).getKryo();

        assertThat(kryo.getSerializer(File.class))
                .as("serializer registration was not properly passed on")
                .isInstanceOf(JavaSerializer.class);
    }

    private void validateListStateDescriptorConfigured(SingleOutputStreamOperator<?> result) {
        OneInputTransformation<?, ?> transform =
                (OneInputTransformation<?, ?>) result.getTransformation();
        StreamOperatorFactory<?> factory = transform.getOperatorFactory();
        StateDescriptor<?, ?> descr;
        if (factory instanceof WindowOperatorFactory) {
            descr = ((WindowOperatorFactory<?, ?, ?, ?, ?>) factory).getStateDescriptor();
        } else {
            WindowOperator<?, ?, ?, ?, ?> op =
                    (WindowOperator<?, ?, ?, ?, ?>) transform.getOperator();
            descr = op.getStateDescriptor();
        }

        assertThat(descr).isInstanceOf(ListStateDescriptor.class);

        ListStateDescriptor<?> listDescr = (ListStateDescriptor<?>) descr;

        // this would be the first statement to fail if state descriptors were not properly
        // initialized
        TypeSerializer<?> serializer = listDescr.getSerializer();
        assertThat(serializer).isInstanceOf(ListSerializer.class);

        TypeSerializer<?> elementSerializer = listDescr.getElementSerializer();
        assertThat(elementSerializer).isInstanceOf(KryoSerializer.class);

        Kryo kryo = ((KryoSerializer<?>) elementSerializer).getKryo();

        assertThat(kryo.getSerializer(File.class))
                .as("serializer registration was not properly passed on")
                .isInstanceOf(JavaSerializer.class);
    }
}
