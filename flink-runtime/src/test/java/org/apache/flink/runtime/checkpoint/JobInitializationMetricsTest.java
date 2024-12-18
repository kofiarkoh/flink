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

import org.apache.flink.runtime.checkpoint.JobInitializationMetrics.SumMaxDuration;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class JobInitializationMetricsTest {

    @Test
    public void testBuildingJobInitializationMetricsFromSingleSubtask() {
        final ExecutionAttemptID executionAttemptID = ExecutionAttemptID.randomId();
        JobInitializationMetricsBuilder initializationMetricsBuilder =
                new JobInitializationMetricsBuilder(Collections.singleton(executionAttemptID), 0);
        assertThat(initializationMetricsBuilder.isComplete()).isFalse();

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder =
                new SubTaskInitializationMetricsBuilder(0);
        subTaskInitializationMetricsBuilder.addDurationMetric("A", 5);
        subTaskInitializationMetricsBuilder.addDurationMetric("A", 10);
        subTaskInitializationMetricsBuilder.addDurationMetric("B", 20);
        SubTaskInitializationMetrics subTaskInitializationMetrics =
                subTaskInitializationMetricsBuilder
                        .setStatus(InitializationStatus.COMPLETED)
                        .build();

        initializationMetricsBuilder.reportInitializationMetrics(
                executionAttemptID, subTaskInitializationMetrics);
        assertThat(initializationMetricsBuilder.isComplete()).isTrue();

        JobInitializationMetrics jobInitializationMetrics = initializationMetricsBuilder.build();
        assertThat(jobInitializationMetrics.getStartTs()).isEqualTo(0);
        assertThat(jobInitializationMetrics.getEndTs())
                .isEqualTo(subTaskInitializationMetrics.getEndTs());
        assertThat(jobInitializationMetrics.getStatus()).isEqualTo(InitializationStatus.COMPLETED);
        assertThat(jobInitializationMetrics.getDurationMetrics())
                .containsOnlyKeys("A", "B")
                .containsEntry("A", new SumMaxDuration("A").addDuration(15))
                .containsEntry("B", new SumMaxDuration("B").addDuration(20));
    }

    @Test
    public void testBuildingJobInitializationMetricsFromMultipleSubtasks() {
        final ExecutionAttemptID firstId = ExecutionAttemptID.randomId();
        final ExecutionAttemptID secondId = ExecutionAttemptID.randomId();
        final ExecutionAttemptID thirdId = ExecutionAttemptID.randomId();
        JobInitializationMetricsBuilder initializationMetricsBuilder =
                new JobInitializationMetricsBuilder(
                        new HashSet<>(Arrays.asList(firstId, secondId, thirdId)), 0);
        assertThat(initializationMetricsBuilder.isComplete()).isFalse();

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder1 =
                new SubTaskInitializationMetricsBuilder(0);
        subTaskInitializationMetricsBuilder1.addDurationMetric("A", 5);
        subTaskInitializationMetricsBuilder1.addDurationMetric("B", 5);
        initializationMetricsBuilder.reportInitializationMetrics(
                firstId,
                subTaskInitializationMetricsBuilder1
                        .setStatus(InitializationStatus.COMPLETED)
                        .build(35));
        assertThat(initializationMetricsBuilder.isComplete()).isFalse();

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder =
                new SubTaskInitializationMetricsBuilder(100);
        subTaskInitializationMetricsBuilder.addDurationMetric("A", 1);
        subTaskInitializationMetricsBuilder.addDurationMetric("B", 10);
        initializationMetricsBuilder.reportInitializationMetrics(
                secondId,
                subTaskInitializationMetricsBuilder
                        .setStatus(InitializationStatus.COMPLETED)
                        .build(140));
        assertThat(initializationMetricsBuilder.isComplete()).isFalse();

        initializationMetricsBuilder.reportInitializationMetrics(
                thirdId,
                new SubTaskInitializationMetricsBuilder(200)
                        .setStatus(InitializationStatus.FAILED)
                        .build(1000));
        assertThat(initializationMetricsBuilder.isComplete()).isTrue();

        JobInitializationMetrics jobInitializationMetrics = initializationMetricsBuilder.build();
        assertThat(jobInitializationMetrics.getStartTs()).isEqualTo(0);
        assertThat(jobInitializationMetrics.getEndTs()).isEqualTo(1000);
        assertThat(jobInitializationMetrics.getStatus()).isEqualTo(InitializationStatus.FAILED);
        assertThat(jobInitializationMetrics.getDurationMetrics())
                .containsOnlyKeys("A", "B")
                .containsEntry("A", new SumMaxDuration("A").addDuration(5).addDuration(1))
                .containsEntry("B", new SumMaxDuration("B").addDuration(5).addDuration(10));
    }

    @Test
    public void testSumMaxDuration() throws Exception {
        SumMaxDuration duration = new SumMaxDuration("A");
        duration.addDuration(1);
        assertThat(duration.getSum()).isEqualTo(1);
        assertThat(duration.getMax()).isEqualTo(1);

        duration.addDuration(5);
        assertThat(duration.getSum()).isEqualTo(6);
        assertThat(duration.getMax()).isEqualTo(5);

        duration.addDuration(4);
        assertThat(duration.getSum()).isEqualTo(10);
        assertThat(duration.getMax()).isEqualTo(5);
    }
}
