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

package org.apache.flink.api.common.eventtime;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link AscendingTimestampsWatermarks} class. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class BoundedOutOfOrdernessWatermarksTest {

    @Test
    void testWatermarkBeforeRecords() {
        final TestingWatermarkOutput output = new TestingWatermarkOutput();
        final BoundedOutOfOrdernessWatermarks<Object> watermarks =
                new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(10));

        watermarks.onPeriodicEmit(output);

        assertThat(output.lastWatermark()).isNotNull();
        assertThat(output.lastWatermark().getTimestamp()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void testWatermarkAfterEvent() {
        final TestingWatermarkOutput output = new TestingWatermarkOutput();
        final BoundedOutOfOrdernessWatermarks<Object> watermarks =
                new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(10));

        watermarks.onEvent(new Object(), 1337L, output);
        watermarks.onPeriodicEmit(output);

        assertThat(output.lastWatermark().getTimestamp()).isEqualTo(1326L);
    }

    @Test
    void testWatermarkAfterNonMonotonousEvents() {
        final TestingWatermarkOutput output = new TestingWatermarkOutput();
        final BoundedOutOfOrdernessWatermarks<Object> watermarks =
                new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(10));

        watermarks.onEvent(new Object(), 12345L, output);
        watermarks.onEvent(new Object(), 12300L, output);
        watermarks.onEvent(new Object(), 12340L, output);
        watermarks.onEvent(new Object(), 12280L, output);
        watermarks.onPeriodicEmit(output);

        assertThat(output.lastWatermark().getTimestamp()).isEqualTo(12334L);
    }

    @Test
    void testRepeatedProbe() {
        final TestingWatermarkOutput output = new TestingWatermarkOutput();
        final BoundedOutOfOrdernessWatermarks<Object> watermarks =
                new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(10));

        watermarks.onEvent(new Object(), 723456L, new TestingWatermarkOutput());
        watermarks.onPeriodicEmit(new TestingWatermarkOutput());

        watermarks.onPeriodicEmit(output);

        assertThat(output.lastWatermark().getTimestamp()).isEqualTo(723445L);
    }
}
