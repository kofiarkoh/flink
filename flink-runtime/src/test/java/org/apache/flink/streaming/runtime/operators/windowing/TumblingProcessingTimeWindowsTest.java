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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link TumblingProcessingTimeWindows}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TumblingProcessingTimeWindowsTest {

    @Test
    void testWindowAssignment() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingProcessingTimeWindows assigner =
                TumblingProcessingTimeWindows.of(Duration.ofMillis(5000));

        when(mockContext.getCurrentProcessingTime()).thenReturn(0L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(0, 5000));

        when(mockContext.getCurrentProcessingTime()).thenReturn(4999L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(0, 5000));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5000L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(5000, 10000));
    }

    @Test
    void testWindowAssignmentWithStagger() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingProcessingTimeWindows assigner =
                TumblingProcessingTimeWindows.of(
                        Duration.ofMillis(5000), Duration.ofMillis(0), WindowStagger.NATURAL);

        when(mockContext.getCurrentProcessingTime()).thenReturn(150L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(150, 5150));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5049L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(150, 5150));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5150L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(5150, 10150));
    }

    @Test
    void testWindowAssignmentWithGlobalOffset() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingProcessingTimeWindows assigner =
                TumblingProcessingTimeWindows.of(Duration.ofMillis(5000), Duration.ofMillis(100));

        when(mockContext.getCurrentProcessingTime()).thenReturn(100L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(100, 5100));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5099L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(100, 5100));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5100L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(5100, 10100));
    }

    @Test
    void testWindowAssignmentWithNegativeGlobalOffset() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingProcessingTimeWindows assigner =
                TumblingProcessingTimeWindows.of(Duration.ofMillis(5000), Duration.ofMillis(-100));

        when(mockContext.getCurrentProcessingTime()).thenReturn(100L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(-100, 4900));

        when(mockContext.getCurrentProcessingTime()).thenReturn(4899L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(-100, 4900));

        when(mockContext.getCurrentProcessingTime()).thenReturn(4900L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(4900, 9900));
    }

    @Test
    void testTimeUnits() {
        // sanity check with one other time unit

        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingProcessingTimeWindows assigner =
                TumblingProcessingTimeWindows.of(Duration.ofSeconds(5), Duration.ofSeconds(1));

        when(mockContext.getCurrentProcessingTime()).thenReturn(1000L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(1000, 6000));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5999L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(1000, 6000));

        when(mockContext.getCurrentProcessingTime()).thenReturn(6000L);
        assertThat(assigner.assignWindows("String", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(6000, 11000));
    }

    @Test
    void testInvalidParameters() {

        assertThatThrownBy(() -> TumblingProcessingTimeWindows.of(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < size");

        assertThatThrownBy(
                        () ->
                                TumblingProcessingTimeWindows.of(
                                        Duration.ofSeconds(10), Duration.ofSeconds(20)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < size");

        assertThatThrownBy(
                        () ->
                                TumblingProcessingTimeWindows.of(
                                        Duration.ofSeconds(10), Duration.ofSeconds(-11)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < size");
    }

    @Test
    void testProperties() {
        TumblingProcessingTimeWindows assigner =
                TumblingProcessingTimeWindows.of(Duration.ofSeconds(5), Duration.ofMillis(100));

        assertThat(assigner.isEventTime()).isFalse();
        assertThat(assigner.getWindowSerializer(new ExecutionConfig()))
                .isEqualTo(new TimeWindow.Serializer());
        assertThat(assigner.getDefaultTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
    }
}
