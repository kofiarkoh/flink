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

/** Tests for the {@link WatermarkGauge}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class WatermarkGaugeTest {

    @Test
    void testSetCurrentLowWatermark() {
        WatermarkGauge metric = new WatermarkGauge();

        assertThat(metric.getValue()).isEqualTo(Long.MIN_VALUE);

        metric.setCurrentWatermark(64);
        assertThat(metric.getValue()).isEqualTo(64L);
    }
}
