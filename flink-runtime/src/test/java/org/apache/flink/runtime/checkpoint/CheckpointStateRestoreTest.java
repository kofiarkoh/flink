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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.SerializableObject;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests concerning the restoring of state from a checkpoint to the task executions. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CheckpointStateRestoreTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    /** Tests that on restore the task state is reset for each stateful task. */
    @Test
    void testSetState() {
        try {
            KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 0);
            List<SerializableObject> testStates =
                    Collections.singletonList(new SerializableObject());
            final KeyedStateHandle serializedKeyGroupStates =
                    CheckpointCoordinatorTestingUtils.generateKeyGroupState(
                            keyGroupRange, testStates);

            final JobVertexID statefulId = new JobVertexID();
            final JobVertexID statelessId = new JobVertexID();

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(statefulId, 3, 256)
                            .addJobVertex(statelessId, 2, 256)
                            .build(EXECUTOR_RESOURCE.getExecutor());

            ExecutionJobVertex stateful = graph.getJobVertex(statefulId);
            ExecutionJobVertex stateless = graph.getJobVertex(statelessId);

            ExecutionVertex stateful1 = stateful.getTaskVertices()[0];
            ExecutionVertex stateful2 = stateful.getTaskVertices()[1];
            ExecutionVertex stateful3 = stateful.getTaskVertices()[2];
            ExecutionVertex stateless1 = stateless.getTaskVertices()[0];
            ExecutionVertex stateless2 = stateless.getTaskVertices()[1];

            Execution statefulExec1 = stateful1.getCurrentExecutionAttempt();
            Execution statefulExec2 = stateful2.getCurrentExecutionAttempt();
            Execution statefulExec3 = stateful3.getCurrentExecutionAttempt();
            Execution statelessExec1 = stateless1.getCurrentExecutionAttempt();
            Execution statelessExec2 = stateless2.getCurrentExecutionAttempt();

            ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
                    new ManuallyTriggeredScheduledExecutor();

            CheckpointCoordinator coord =
                    new CheckpointCoordinatorBuilder()
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build(graph);

            // create ourselves a checkpoint with state
            coord.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();

            PendingCheckpoint pending = coord.getPendingCheckpoints().values().iterator().next();
            final long checkpointId = pending.getCheckpointID();

            final TaskStateSnapshot subtaskStates = new TaskStateSnapshot();

            subtaskStates.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(statefulId),
                    OperatorSubtaskState.builder()
                            .setManagedKeyedState(serializedKeyGroupStates)
                            .build());

            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            graph.getJobID(),
                            statefulExec1.getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskStates),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            graph.getJobID(),
                            statefulExec2.getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskStates),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            graph.getJobID(),
                            statefulExec3.getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskStates),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            graph.getJobID(), statelessExec1.getAttemptId(), checkpointId),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            graph.getJobID(), statelessExec2.getAttemptId(), checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            assertThat(coord.getNumberOfRetainedSuccessfulCheckpoints()).isOne();
            assertThat(coord.getNumberOfPendingCheckpoints()).isZero();

            // let the coordinator inject the state
            assertThat(
                            coord.restoreLatestCheckpointedStateToAll(
                                    new HashSet<>(Arrays.asList(stateful, stateless)), false))
                    .isTrue();

            // verify that each stateful vertex got the state
            assertThat(statefulExec1.getTaskRestore().getTaskStateSnapshot())
                    .isEqualTo(subtaskStates);
            assertThat(statefulExec2.getTaskRestore().getTaskStateSnapshot())
                    .isEqualTo(subtaskStates);
            assertThat(statefulExec3.getTaskRestore().getTaskStateSnapshot())
                    .isEqualTo(subtaskStates);
            assertThat(statelessExec1.getTaskRestore()).isNull();
            assertThat(statelessExec2.getTaskRestore()).isNull();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testNoCheckpointAvailable() {
        try {
            CheckpointCoordinator coord =
                    new CheckpointCoordinatorBuilder().build(EXECUTOR_RESOURCE.getExecutor());

            final boolean restored =
                    coord.restoreLatestCheckpointedStateToAll(Collections.emptySet(), false);
            assertThat(restored).isFalse();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Tests that the allow non restored state flag is correctly handled.
     *
     * <p>The flag only applies for state that is part of the checkpoint.
     */
    @Test
    void testNonRestoredState() throws Exception {
        // --- (1) Create tasks to restore checkpoint with ---
        JobVertexID jobVertexId1 = new JobVertexID();
        JobVertexID jobVertexId2 = new JobVertexID();

        OperatorID operatorId1 = OperatorID.fromJobVertexID(jobVertexId1);

        // 1st JobVertex
        ExecutionVertex vertex11 = mockExecutionVertex(mockExecution(), jobVertexId1, 0, 3);
        ExecutionVertex vertex12 = mockExecutionVertex(mockExecution(), jobVertexId1, 1, 3);
        ExecutionVertex vertex13 = mockExecutionVertex(mockExecution(), jobVertexId1, 2, 3);
        // 2nd JobVertex
        ExecutionVertex vertex21 = mockExecutionVertex(mockExecution(), jobVertexId2, 0, 2);
        ExecutionVertex vertex22 = mockExecutionVertex(mockExecution(), jobVertexId2, 1, 2);

        ExecutionJobVertex jobVertex1 =
                mockExecutionJobVertex(
                        jobVertexId1, new ExecutionVertex[] {vertex11, vertex12, vertex13});
        ExecutionJobVertex jobVertex2 =
                mockExecutionJobVertex(jobVertexId2, new ExecutionVertex[] {vertex21, vertex22});

        Set<ExecutionJobVertex> tasks = new HashSet<>();
        tasks.add(jobVertex1);
        tasks.add(jobVertex2);

        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder().build(EXECUTOR_RESOURCE.getExecutor());

        // --- (2) Checkpoint misses state for a jobVertex (should work) ---
        Map<OperatorID, OperatorState> checkpointTaskStates = new HashMap<>();
        {
            OperatorState taskState = new OperatorState(null, null, operatorId1, 3, 3);
            taskState.putState(0, OperatorSubtaskState.builder().build());
            taskState.putState(1, OperatorSubtaskState.builder().build());
            taskState.putState(2, OperatorSubtaskState.builder().build());

            checkpointTaskStates.put(operatorId1, taskState);
        }
        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        new JobID(),
                        0,
                        1,
                        2,
                        new HashMap<>(checkpointTaskStates),
                        Collections.<MasterState>emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        coord.getCheckpointStore()
                .addCheckpointAndSubsumeOldestOne(checkpoint, new CheckpointsCleaner(), () -> {});

        assertThat(coord.restoreLatestCheckpointedStateToAll(tasks, false)).isTrue();
        assertThat(coord.restoreLatestCheckpointedStateToAll(tasks, true)).isTrue();

        // --- (3) JobVertex missing for task state that is part of the checkpoint ---
        JobVertexID newJobVertexID = new JobVertexID();
        OperatorID newOperatorID = OperatorID.fromJobVertexID(newJobVertexID);

        // There is no task for this
        {
            OperatorState taskState = new OperatorState(null, null, newOperatorID, 1, 1);
            taskState.putState(0, OperatorSubtaskState.builder().build());

            checkpointTaskStates.put(newOperatorID, taskState);
        }

        checkpoint =
                new CompletedCheckpoint(
                        new JobID(),
                        1,
                        2,
                        3,
                        new HashMap<>(checkpointTaskStates),
                        Collections.<MasterState>emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        coord.getCheckpointStore()
                .addCheckpointAndSubsumeOldestOne(checkpoint, new CheckpointsCleaner(), () -> {});

        // (i) Allow non restored state (should succeed)
        final boolean restored = coord.restoreLatestCheckpointedStateToAll(tasks, true);
        assertThat(restored).isTrue();

        // (ii) Don't allow non restored state (should fail)
        try {
            coord.restoreLatestCheckpointedStateToAll(tasks, false);
            fail("Did not throw the expected Exception.");
        } catch (IllegalStateException ignored) {
        }
    }

    // ------------------------------------------------------------------------

    private Execution mockExecution() {
        return mockExecution(ExecutionState.RUNNING);
    }

    private Execution mockExecution(ExecutionState state) {
        Execution mock = mock(Execution.class);
        when(mock.getAttemptId()).thenReturn(createExecutionAttemptId());
        when(mock.getState()).thenReturn(state);
        return mock;
    }

    private ExecutionVertex mockExecutionVertex(
            Execution execution, JobVertexID vertexId, int subtask, int parallelism) {
        ExecutionVertex mock = mock(ExecutionVertex.class);
        when(mock.getJobvertexId()).thenReturn(vertexId);
        when(mock.getParallelSubtaskIndex()).thenReturn(subtask);
        when(mock.getCurrentExecutionAttempt()).thenReturn(execution);
        when(mock.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
        when(mock.getMaxParallelism()).thenReturn(parallelism);
        return mock;
    }

    private ExecutionJobVertex mockExecutionJobVertex(JobVertexID id, ExecutionVertex[] vertices) {
        ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
        when(vertex.getParallelism()).thenReturn(vertices.length);
        when(vertex.getMaxParallelism()).thenReturn(vertices.length);
        when(vertex.getJobVertexId()).thenReturn(id);
        when(vertex.getTaskVertices()).thenReturn(vertices);
        when(vertex.getOperatorIDs())
                .thenReturn(
                        Collections.singletonList(
                                OperatorIDPair.generatedIDOnly(OperatorID.fromJobVertexID(id))));
        when(vertex.getProducedDataSets()).thenReturn(new IntermediateResult[0]);

        for (ExecutionVertex v : vertices) {
            when(v.getJobVertex()).thenReturn(vertex);
        }
        return vertex;
    }
}
