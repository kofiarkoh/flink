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

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LatencyTrackingValueState}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class LatencyTrackingValueStateTest extends LatencyTrackingStateTestBase<Integer> {
    @Override
    @SuppressWarnings("unchecked")
    ValueStateDescriptor<Long> getStateDescriptor() {
        return new ValueStateDescriptor<>("value", Long.class);
    }

    @Override
    IntSerializer getKeySerializer() {
        return IntSerializer.INSTANCE;
    }

    @Override
    void setCurrentKey(AbstractKeyedStateBackend<Integer> keyedBackend) {
        keyedBackend.setCurrentKey(1);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testLatencyTrackingValueState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingValueState<Integer, VoidNamespace, Long> latencyTrackingState =
                    (LatencyTrackingValueState)
                            createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            LatencyTrackingValueState.ValueStateLatencyMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            assertThat(latencyTrackingStateMetric.getUpdateCount()).isZero();
            assertThat(latencyTrackingStateMetric.getGetCount()).isZero();

            setCurrentKey(keyedBackend);
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.update(ThreadLocalRandom.current().nextLong());
                assertThat(latencyTrackingStateMetric.getUpdateCount()).isEqualTo(expectedResult);

                latencyTrackingState.value();
                assertThat(latencyTrackingStateMetric.getGetCount()).isEqualTo(expectedResult);
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }
}
