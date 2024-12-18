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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartialFinishedInputConsumableDecider}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PartialFinishedInputConsumableDeciderTest {
    @Test
    void testNotFinishedBlockingInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        PartialFinishedInputConsumableDecider inputConsumableDecider =
                createPartialFinishedInputConsumableDecider();

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isFalse();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), Collections.emptySet(), new HashMap<>()))
                .isFalse();
    }

    @Test
    void testPartialFinishedBlockingInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        topology.getResultPartition(producers.get(0).getProducedResults().iterator().next().getId())
                .markFinished();

        PartialFinishedInputConsumableDecider inputConsumableDecider =
                createPartialFinishedInputConsumableDecider();

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isFalse();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), Collections.emptySet(), new HashMap<>()))
                .isFalse();
    }

    @Test
    void testAllFinishedBlockingInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.ALL_DATA_PRODUCED)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        PartialFinishedInputConsumableDecider inputConsumableDecider =
                createPartialFinishedInputConsumableDecider();

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isTrue();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), Collections.emptySet(), new HashMap<>()))
                .isTrue();
    }

    @Test
    void testNotFinishedHybridInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.HYBRID_FULL)
                .finish();

        PartialFinishedInputConsumableDecider inputConsumableDecider =
                createPartialFinishedInputConsumableDecider();

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isFalse();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), Collections.emptySet(), new HashMap<>()))
                .isFalse();
    }

    @Test
    void testPartialFinishedHybridInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.ALL_DATA_PRODUCED)
                .withResultPartitionType(ResultPartitionType.HYBRID_FULL)
                .finish();

        topology.getResultPartition(producers.get(0).getProducedResults().iterator().next().getId())
                .markFinished();

        PartialFinishedInputConsumableDecider inputConsumableDecider =
                createPartialFinishedInputConsumableDecider();

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isTrue();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), Collections.emptySet(), new HashMap<>()))
                .isTrue();
    }

    @Test
    void testAllFinishedHybridInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.ALL_DATA_PRODUCED)
                .withResultPartitionType(ResultPartitionType.HYBRID_FULL)
                .finish();

        PartialFinishedInputConsumableDecider inputConsumableDecider =
                createPartialFinishedInputConsumableDecider();

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isTrue();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), Collections.emptySet(), new HashMap<>()))
                .isTrue();
    }

    private static PartialFinishedInputConsumableDecider
            createPartialFinishedInputConsumableDecider() {
        return new PartialFinishedInputConsumableDecider();
    }
}
