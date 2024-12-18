/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.TestingAccessExecution;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link Canceling} state of the {@link AdaptiveScheduler}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CancelingTest {

    private static final Logger log = LoggerFactory.getLogger(CancelingTest.class);

    @Test
    void testExecutionGraphCancelationOnEnter() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            StateTrackingMockExecutionGraph stateTrackingMockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            createCancelingState(ctx, stateTrackingMockExecutionGraph);

            assertThat(stateTrackingMockExecutionGraph.getState()).isEqualTo(JobStatus.CANCELLING);
        }
    }

    @Test
    void testTransitionToFinishedWhenCancellationCompletes() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            StateTrackingMockExecutionGraph stateTrackingMockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            Canceling canceling = createCancelingState(ctx, stateTrackingMockExecutionGraph);
            assertThat(stateTrackingMockExecutionGraph.getState()).isEqualTo(JobStatus.CANCELLING);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.CANCELED));
            // this transitions the EG from CANCELLING to CANCELLED.
            stateTrackingMockExecutionGraph.completeTerminationFuture(JobStatus.CANCELED);
        }
    }

    @Test
    void testTransitionToSuspend() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx, new StateTrackingMockExecutionGraph());
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.SUSPENDED));
            canceling.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    void testCancelIsIgnored() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx, new StateTrackingMockExecutionGraph());
            canceling.cancel();
            ctx.assertNoStateTransition();
        }
    }

    @Test
    void testGlobalFailuresAreIgnored() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx, new StateTrackingMockExecutionGraph());
            canceling.handleGlobalFailure(
                    new RuntimeException("test"), FailureEnricherUtils.EMPTY_FAILURE_LABELS);
            ctx.assertNoStateTransition();
        }
    }

    @Test
    void testTaskFailuresAreIgnored() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Canceling canceling = createCancelingState(ctx, meg);
            // register execution at EG

            Exception exception = new RuntimeException();
            TestingAccessExecution execution =
                    TestingAccessExecution.newBuilder()
                            .withExecutionState(ExecutionState.FAILED)
                            .withErrorInfo(new ErrorInfo(exception, System.currentTimeMillis()))
                            .build();
            meg.registerExecution(execution);
            TaskExecutionStateTransition update =
                    ExecutingTest.createFailingStateTransition(execution.getAttemptId(), exception);
            canceling.updateTaskExecutionState(update, FailureEnricherUtils.EMPTY_FAILURE_LABELS);
            ctx.assertNoStateTransition();
        }
    }

    @Test
    void testStateDoesNotExposeGloballyTerminalExecutionGraph() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Canceling canceling = createCancelingState(ctx, meg);

            // ideally we'd delay the async call to #onGloballyTerminalState instead, but the
            // context does not support that
            ctx.setExpectFinished(eg -> {});

            meg.completeTerminationFuture(JobStatus.CANCELED);

            // this is just a sanity check for the test
            assertThat(meg.getState()).isEqualTo(JobStatus.CANCELED);

            assertThat(canceling.getJobStatus()).isEqualTo(JobStatus.CANCELLING);
            assertThat(canceling.getJob().getState()).isEqualTo(JobStatus.CANCELLING);
            assertThat(canceling.getJob().getStatusTimestamp(JobStatus.CANCELED)).isZero();
        }
    }

    private Canceling createCancelingState(
            MockStateWithExecutionGraphContext ctx, ExecutionGraph executionGraph) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new TestingOperatorCoordinatorHandler();
        executionGraph.transitionToRunning();
        Canceling canceling =
                new Canceling(
                        ctx,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        log,
                        ClassLoader.getSystemClassLoader(),
                        new ArrayList<>());
        return canceling;
    }
}
