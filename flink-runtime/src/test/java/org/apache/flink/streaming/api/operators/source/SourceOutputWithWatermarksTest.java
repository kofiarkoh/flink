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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.RecordTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link SourceOutputWithWatermarks}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class SourceOutputWithWatermarksTest {

    /**
     * Creates a new SourceOutputWithWatermarks that emits records to the given DataOutput and
     * watermarks to the (possibly different) WatermarkOutput.
     */
    private static <E> SourceOutputWithWatermarks<E> createWithSameOutputs(
            PushingAsyncDataInput.DataOutput<E> recordsAndWatermarksOutput,
            TimestampAssigner<E> timestampAssigner,
            WatermarkGenerator<E> watermarkGenerator) {

        final WatermarkOutput watermarkOutput =
                new WatermarkToDataOutput(recordsAndWatermarksOutput);

        return new SourceOutputWithWatermarks<>(
                recordsAndWatermarksOutput,
                watermarkOutput,
                watermarkOutput,
                timestampAssigner,
                watermarkGenerator);
    }

    @Test
    void testNoTimestampValue() {
        final CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();
        final SourceOutputWithWatermarks<Integer> out =
                createWithSameOutputs(
                        dataOutput, new RecordTimestampAssigner<>(), new NoWatermarksGenerator<>());

        out.collect(17);

        final Object event = dataOutput.events.get(0);
        assertThat(event).isInstanceOf(StreamRecord.class);
        assertThat(((StreamRecord<?>) event).getTimestamp())
                .isEqualTo(TimestampAssigner.NO_TIMESTAMP);
    }

    @Test
    void eventsAreBeforeWatermarks() {
        final CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();
        final SourceOutputWithWatermarks<Integer> out =
                createWithSameOutputs(
                        dataOutput,
                        new RecordTimestampAssigner<>(),
                        new TestWatermarkGenerator<>());

        out.collect(42, 12345L);

        assertThat(dataOutput.events)
                .contains(
                        new StreamRecord<>(42, 12345L),
                        new org.apache.flink.streaming.api.watermark.Watermark(12345L));
    }

    // ------------------------------------------------------------------------

    private static final class TestWatermarkGenerator<T> implements WatermarkGenerator<T> {

        private long lastTimestamp;

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            lastTimestamp = eventTimestamp;
            output.emitWatermark(new Watermark(eventTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(lastTimestamp));
        }
    }
}
