/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.partitionrelease;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingPipelinedRegion;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PipelinedRegionExecutionView}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PipelinedRegionExecutionViewTest {

    private static final ExecutionVertexID TEST_EXECUTION_VERTEX_ID =
            new ExecutionVertexID(new JobVertexID(), 0);

    private static final TestingSchedulingPipelinedRegion TEST_PIPELINED_REGION =
            new TestingSchedulingPipelinedRegion(
                    Collections.singleton(
                            TestingSchedulingExecutionVertex.withExecutionVertexID(
                                    TEST_EXECUTION_VERTEX_ID.getJobVertexId(),
                                    TEST_EXECUTION_VERTEX_ID.getSubtaskIndex())));

    @Test
    public void regionIsUnfinishedIfNotAllVerticesAreFinished() {
        final PipelinedRegionExecutionView pipelinedRegionExecutionView =
                new PipelinedRegionExecutionView(TEST_PIPELINED_REGION);

        assertThat(pipelinedRegionExecutionView.isFinished()).isFalse();
    }

    @Test
    public void regionIsFinishedIfAllVerticesAreFinished() {
        final PipelinedRegionExecutionView pipelinedRegionExecutionView =
                new PipelinedRegionExecutionView(TEST_PIPELINED_REGION);

        pipelinedRegionExecutionView.vertexFinished(TEST_EXECUTION_VERTEX_ID);

        assertThat(pipelinedRegionExecutionView.isFinished()).isTrue();
    }

    @Test
    public void vertexCanBeUnfinished() {
        final PipelinedRegionExecutionView pipelinedRegionExecutionView =
                new PipelinedRegionExecutionView(TEST_PIPELINED_REGION);

        pipelinedRegionExecutionView.vertexFinished(TEST_EXECUTION_VERTEX_ID);
        pipelinedRegionExecutionView.vertexUnfinished(TEST_EXECUTION_VERTEX_ID);

        assertThat(pipelinedRegionExecutionView.isFinished()).isFalse();
    }

    @Test
    public void finishingUnknownVertexThrowsException() {
        final PipelinedRegionExecutionView pipelinedRegionExecutionView =
                new PipelinedRegionExecutionView(TEST_PIPELINED_REGION);

        final ExecutionVertexID unknownVertexId = new ExecutionVertexID(new JobVertexID(), 0);
        assertThatThrownBy(() -> pipelinedRegionExecutionView.vertexFinished(unknownVertexId))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
