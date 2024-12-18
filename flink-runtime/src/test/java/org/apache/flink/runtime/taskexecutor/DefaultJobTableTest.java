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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.taskmanager.NoOpCheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.util.function.SupplierWithException;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultJobTable}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class DefaultJobTableTest {

    private static final SupplierWithException<JobTable.JobServices, RuntimeException>
            DEFAULT_JOB_SERVICES_SUPPLIER = () -> TestingJobServices.newBuilder().build();

    private final JobID jobId = new JobID();

    private DefaultJobTable jobTable;

    @BeforeEach
    void setup() {
        jobTable = DefaultJobTable.create();
    }

    @AfterEach
    void teardown() {
        if (jobTable != null) {
            jobTable.close();
        }
    }

    @Test
    void getOrCreateJob_NoRegisteredJob_WillCreateNewJob() {
        final JobTable.Job newJob = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);

        assertThat(newJob.getJobId()).isEqualTo(jobId);
        assertThat(jobTable.getJob(jobId)).isPresent();
    }

    @Test
    void getOrCreateJob_RegisteredJob_WillReturnRegisteredJob() {
        final JobTable.Job newJob = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);
        final JobTable.Job otherJob = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);

        assertThat(otherJob).isSameAs(newJob);
    }

    @Test
    void closeJob_WillCloseJobServices() throws InterruptedException {
        final OneShotLatch shutdownLibraryCacheManagerLatch = new OneShotLatch();
        final TestingJobServices jobServices =
                TestingJobServices.newBuilder()
                        .setCloseRunnable(shutdownLibraryCacheManagerLatch::trigger)
                        .build();
        final JobTable.Job job = jobTable.getOrCreateJob(jobId, () -> jobServices);

        job.close();

        shutdownLibraryCacheManagerLatch.await();
    }

    @Test
    void closeJob_WillRemoveItFromJobTable() {
        final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);

        job.close();

        assertThat(jobTable.getJob(jobId)).isNotPresent();
    }

    @Test
    void connectJob_NotConnected_Succeeds() {
        final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);

        final ResourceID resourceId = ResourceID.generate();
        final JobTable.Connection connection = connectJob(job, resourceId);

        assertThat(connection.getJobId()).isEqualTo(jobId);
        assertThat(connection.getResourceId()).isEqualTo(resourceId);
        assertThat(jobTable.getConnection(jobId)).isPresent();
        assertThat(jobTable.getConnection(resourceId)).isPresent();
    }

    private JobTable.Connection connectJob(JobTable.Job job, ResourceID resourceId) {
        return job.connect(
                resourceId,
                new TestingJobMasterGatewayBuilder().build(),
                new NoOpTaskManagerActions(),
                NoOpCheckpointResponder.INSTANCE,
                new TestGlobalAggregateManager(),
                new NoOpPartitionProducerStateChecker());
    }

    @Test
    void connectJob_Connected_Fails() {
        final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);

        connectJob(job, ResourceID.generate());

        assertThatThrownBy(() -> connectJob(job, ResourceID.generate()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void disconnectConnection_RemovesConnection() {
        final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);

        final ResourceID resourceId = ResourceID.generate();
        final JobTable.Connection connection = connectJob(job, resourceId);

        connection.disconnect();

        assertThat(jobTable.getConnection(jobId)).isNotPresent();
        assertThat(jobTable.getConnection(resourceId)).isNotPresent();
    }

    @Test
    void access_AfterBeingClosed_WillFail() {
        final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);

        job.close();

        assertThatThrownBy(job::asConnection).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void connectJob_AfterBeingClosed_WillFail() {
        final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);

        job.close();

        assertThatThrownBy(() -> connectJob(job, ResourceID.generate()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void accessJobManagerGateway_AfterBeingDisconnected_WillFail() {
        final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_JOB_SERVICES_SUPPLIER);

        final JobTable.Connection connection = connectJob(job, ResourceID.generate());

        connection.disconnect();

        assertThatThrownBy(connection::getJobManagerGateway)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void close_WillCloseAllRegisteredJobs() throws InterruptedException {
        final CountDownLatch shutdownLibraryCacheManagerLatch = new CountDownLatch(2);
        final TestingJobServices jobServices1 =
                TestingJobServices.newBuilder()
                        .setCloseRunnable(shutdownLibraryCacheManagerLatch::countDown)
                        .build();
        final TestingJobServices jobServices2 =
                TestingJobServices.newBuilder()
                        .setCloseRunnable(shutdownLibraryCacheManagerLatch::countDown)
                        .build();

        jobTable.getOrCreateJob(jobId, () -> jobServices1);
        jobTable.getOrCreateJob(new JobID(), () -> jobServices2);

        jobTable.close();

        shutdownLibraryCacheManagerLatch.await();
        assertThat(jobTable.isEmpty()).isTrue();
    }
}
