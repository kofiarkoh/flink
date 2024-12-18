/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.ExecutionDeploymentReport;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.clusterframework.types.ResourceID.generate;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultExecutionDeploymentReconciler}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class DefaultExecutionDeploymentReconcilerTest {

    @Test
    void testMatchingDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID attemptId = createExecutionAttemptId();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(Collections.singleton(attemptId)),
                Collections.singletonMap(attemptId, ExecutionDeploymentState.DEPLOYED));

        assertThat(handler.getMissingExecutions()).isEmpty();
        assertThat(handler.getUnknownExecutions()).isEmpty();
    }

    @Test
    void testMissingDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID attemptId = createExecutionAttemptId();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(Collections.emptySet()),
                Collections.singletonMap(attemptId, ExecutionDeploymentState.DEPLOYED));

        assertThat(handler.getUnknownExecutions()).isEmpty();
        assertThat(handler.getMissingExecutions()).contains(attemptId);
    }

    @Test
    void testUnknownDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID attemptId = createExecutionAttemptId();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(Collections.singleton(attemptId)),
                Collections.emptyMap());

        assertThat(handler.getMissingExecutions()).isEmpty();
        assertThat(handler.getUnknownExecutions()).contains(attemptId);
    }

    @Test
    void testMissingAndUnknownDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID unknownId = createExecutionAttemptId();
        ExecutionAttemptID missingId = createExecutionAttemptId();
        ExecutionAttemptID matchingId = createExecutionAttemptId();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(new HashSet<>(Arrays.asList(unknownId, matchingId))),
                Stream.of(missingId, matchingId)
                        .collect(Collectors.toMap(x -> x, x -> ExecutionDeploymentState.DEPLOYED)));

        assertThat(handler.getMissingExecutions()).contains(missingId);
        assertThat(handler.getUnknownExecutions()).contains(unknownId);
    }

    @Test
    void testPendingDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID matchingId = createExecutionAttemptId();
        ExecutionAttemptID unknownId = createExecutionAttemptId();
        ExecutionAttemptID missingId = createExecutionAttemptId();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(new HashSet<>(Arrays.asList(matchingId, unknownId))),
                Stream.of(matchingId, missingId)
                        .collect(Collectors.toMap(x -> x, x -> ExecutionDeploymentState.PENDING)));

        assertThat(handler.getMissingExecutions()).isEmpty();
        assertThat(handler.getUnknownExecutions()).contains(unknownId);
    }

    private static class TestingExecutionDeploymentReconciliationHandler
            implements ExecutionDeploymentReconciliationHandler {
        private final Collection<ExecutionAttemptID> missingExecutions = new ArrayList<>();
        private final Collection<ExecutionAttemptID> unknownExecutions = new ArrayList<>();

        @Override
        public void onMissingDeploymentsOf(
                Collection<ExecutionAttemptID> executionAttemptIds,
                ResourceID hostingTaskExecutor) {
            missingExecutions.addAll(executionAttemptIds);
        }

        @Override
        public void onUnknownDeploymentsOf(
                Collection<ExecutionAttemptID> executionAttemptIds,
                ResourceID hostingTaskExecutor) {
            unknownExecutions.addAll(executionAttemptIds);
        }

        public Collection<ExecutionAttemptID> getMissingExecutions() {
            return missingExecutions;
        }

        public Collection<ExecutionAttemptID> getUnknownExecutions() {
            return unknownExecutions;
        }
    }
}
