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

package org.apache.flink.runtime.checkpoint;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class SubTaskInitializationMetricsTest {

    @Test
    public void testBuildingInvalidMetrics() {
        Assertions.assertThatIllegalArgumentException()
                .isThrownBy(
                        () -> {
                            SubTaskInitializationMetricsBuilder
                                    subTaskInitializationMetricsBuilder =
                                            new SubTaskInitializationMetricsBuilder(10);
                            subTaskInitializationMetricsBuilder.build(9);
                        });
        Assertions.assertThatIllegalArgumentException()
                .isThrownBy(
                        () -> {
                            SubTaskInitializationMetricsBuilder
                                    subTaskInitializationMetricsBuilder =
                                            new SubTaskInitializationMetricsBuilder(-1);
                            subTaskInitializationMetricsBuilder.build(50);
                        });
    }

    @Test
    public void testDurationMetrics() {
        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder =
                new SubTaskInitializationMetricsBuilder(10);
        subTaskInitializationMetricsBuilder.addDurationMetric("A", 20);
        subTaskInitializationMetricsBuilder.addDurationMetric("B", 10);
        assertThat(subTaskInitializationMetricsBuilder.build(50).getDurationMetrics())
                .containsOnlyKeys("A", "B")
                .containsEntry("A", 20L)
                .containsEntry("B", 10L);
    }
}
