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

package org.apache.flink.streaming.runtime.metrics;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link MinWatermarkGauge}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class MinWatermarkGaugeTest {

    @Test
    void testSetCurrentLowWatermark() {
        WatermarkGauge metric1 = new WatermarkGauge();
        WatermarkGauge metric2 = new WatermarkGauge();
        MinWatermarkGauge metric = new MinWatermarkGauge(metric1, metric2);

        assertThat(metric.getValue()).isEqualTo(Long.MIN_VALUE);

        metric1.setCurrentWatermark(1);
        assertThat(metric.getValue()).isEqualTo(Long.MIN_VALUE);

        metric2.setCurrentWatermark(2);
        assertThat(metric.getValue()).isOne();

        metric1.setCurrentWatermark(3);
        assertThat(metric.getValue()).isEqualTo(2L);
    }
}
