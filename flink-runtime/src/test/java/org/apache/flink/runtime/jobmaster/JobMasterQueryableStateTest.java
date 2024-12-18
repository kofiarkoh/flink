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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the queryable-state logic of the {@link JobMaster}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class JobMasterQueryableStateTest {

    private static final Duration testingTimeout = Duration.ofSeconds(10L);

    private static TestingRpcService rpcService;

    private static final int PARALLELISM = 4;
    private static final JobVertex JOB_VERTEX_1;
    private static final JobVertex JOB_VERTEX_2;
    private static final JobGraph JOB_GRAPH;

    static {
        JOB_VERTEX_1 = new JobVertex("v1");
        JOB_VERTEX_1.setParallelism(PARALLELISM);
        JOB_VERTEX_1.setInvokableClass(AbstractInvokable.class);

        JOB_VERTEX_2 = new JobVertex("v2");
        JOB_VERTEX_2.setParallelism(PARALLELISM);
        JOB_VERTEX_2.setInvokableClass(AbstractInvokable.class);

        // explicit configuration required for #testRegisterAndUnregisterKvState
        JOB_VERTEX_1.setMaxParallelism(16);
        JOB_VERTEX_2.setMaxParallelism(16);

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        JOB_VERTEX_1.setSlotSharingGroup(slotSharingGroup);
        JOB_VERTEX_2.setSlotSharingGroup(slotSharingGroup);

        JOB_GRAPH = JobGraphTestUtils.streamingJobGraph(JOB_VERTEX_1, JOB_VERTEX_2);
        JOB_GRAPH.setJobType(JobType.STREAMING);
    }

    @BeforeAll
    private static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @AfterEach
    private void teardown() throws Exception {
        rpcService.clearGateways();
    }

    @AfterAll
    private static void teardownClass() {
        if (rpcService != null) {
            rpcService.closeAsync();
            rpcService = null;
        }
    }

    @Test
    void testRequestKvStateWithoutRegistration() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

            assertThatThrownBy(
                            () ->
                                    jobMasterGateway
                                            .requestKvStateLocation(JOB_GRAPH.getJobID(), "unknown")
                                            .get())
                    .satisfies(FlinkAssertions.anyCauseMatches(UnknownKvStateLocation.class));
        }
    }

    @Test
    void testRequestKvStateOfWrongJob() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

            assertThatThrownBy(
                            () ->
                                    jobMasterGateway
                                            .requestKvStateLocation(new JobID(), "unknown")
                                            .get())
                    .satisfies(FlinkAssertions.anyCauseMatches(FlinkJobNotFoundException.class));
        }
    }

    @Test
    void testRequestKvStateWithIrrelevantRegistration() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

            // register an irrelevant KvState
            assertThatThrownBy(
                            () ->
                                    registerKvState(
                                            jobMasterGateway,
                                            new JobID(),
                                            new JobVertexID(),
                                            "any-name"))
                    .satisfies(FlinkAssertions.anyCauseMatches(FlinkJobNotFoundException.class));
        }
    }

    @Test
    void testRegisterKvState() throws Exception {
        try (JobMaster jobMaster = new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster()) {
            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

            final String registrationName = "register-me";
            final KvStateID kvStateID = new KvStateID();
            final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
            final InetSocketAddress address =
                    new InetSocketAddress(InetAddress.getLocalHost(), 1029);

            jobMasterGateway
                    .notifyKvStateRegistered(
                            JOB_GRAPH.getJobID(),
                            JOB_VERTEX_1.getID(),
                            keyGroupRange,
                            registrationName,
                            kvStateID,
                            address)
                    .get();

            final KvStateLocation location =
                    jobMasterGateway
                            .requestKvStateLocation(JOB_GRAPH.getJobID(), registrationName)
                            .get();

            assertThat(location.getJobId()).isEqualTo(JOB_GRAPH.getJobID());
            assertThat(location.getJobVertexId()).isEqualTo(JOB_VERTEX_1.getID());
            assertThat(location.getNumKeyGroups()).isEqualTo(JOB_VERTEX_1.getMaxParallelism());
            assertThat(location.getNumRegisteredKeyGroups()).isOne();
            assertThat(keyGroupRange.getNumberOfKeyGroups()).isOne();
            assertThat(location.getKvStateID(keyGroupRange.getStartKeyGroup()))
                    .isEqualTo(kvStateID);
            assertThat(location.getKvStateServerAddress(keyGroupRange.getStartKeyGroup()))
                    .isEqualTo(address);
        }
    }

    @Test
    void testUnregisterKvState() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

            final String registrationName = "register-me";
            final KvStateID kvStateID = new KvStateID();
            final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
            final InetSocketAddress address =
                    new InetSocketAddress(InetAddress.getLocalHost(), 1029);

            jobMasterGateway
                    .notifyKvStateRegistered(
                            JOB_GRAPH.getJobID(),
                            JOB_VERTEX_1.getID(),
                            keyGroupRange,
                            registrationName,
                            kvStateID,
                            address)
                    .get();

            jobMasterGateway
                    .notifyKvStateUnregistered(
                            JOB_GRAPH.getJobID(),
                            JOB_VERTEX_1.getID(),
                            keyGroupRange,
                            registrationName)
                    .get();

            assertThatThrownBy(
                            () -> {
                                jobMasterGateway
                                        .requestKvStateLocation(
                                                JOB_GRAPH.getJobID(), registrationName)
                                        .get();
                            })
                    .as("Expected to fail with an UnknownKvStateLocation.")
                    .isInstanceOf(Exception.class)
                    .hasCauseInstanceOf(UnknownKvStateLocation.class);
        }
    }

    @Test
    void testDuplicatedKvStateRegistrationsFailTask() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

            // duplicate registration fails task

            final String registrationName = "duplicate-me";

            registerKvState(
                    jobMasterGateway, JOB_GRAPH.getJobID(), JOB_VERTEX_1.getID(), registrationName);
            assertThatThrownBy(
                            () ->
                                    registerKvState(
                                            jobMasterGateway,
                                            JOB_GRAPH.getJobID(),
                                            JOB_VERTEX_2.getID(),
                                            registrationName))
                    .as("Expected to fail because of clashing registration message.")
                    .isInstanceOf(Exception.class)
                    .hasMessageContaining("Registration name clash");
            assertThat(jobMasterGateway.requestJobStatus(testingTimeout).get())
                    .satisfies(
                            (Consumer<JobStatus>)
                                    jobStatus -> {
                                        assert jobStatus == JobStatus.FAILED
                                                || jobStatus == JobStatus.FAILING;
                                    });
        }
    }

    private static void registerSlotsRequiredForJobExecution(
            JobMasterGateway jobMasterGateway, JobID jobId)
            throws ExecutionException, InterruptedException {
        final OneShotLatch oneTaskSubmittedLatch = new OneShotLatch();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setSubmitTaskConsumer(
                                (taskDeploymentDescriptor, jobMasterId) -> {
                                    oneTaskSubmittedLatch.trigger();
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        JobMasterTestUtils.registerTaskExecutorAndOfferSlots(
                rpcService,
                jobMasterGateway,
                jobId,
                PARALLELISM,
                taskExecutorGateway,
                testingTimeout);

        oneTaskSubmittedLatch.await();
    }

    private static void registerKvState(
            KvStateRegistryGateway stateRegistryGateway,
            JobID jobId,
            JobVertexID jobVertexId,
            String registrationName)
            throws UnknownHostException, ExecutionException, InterruptedException {
        stateRegistryGateway
                .notifyKvStateRegistered(
                        jobId,
                        jobVertexId,
                        new KeyGroupRange(0, 0),
                        registrationName,
                        new KvStateID(),
                        new InetSocketAddress(InetAddress.getLocalHost(), 1233))
                .get();
    }
}
