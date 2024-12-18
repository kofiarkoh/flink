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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link FineGrainedTaskManagerRegistration}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class FineGrainedTaskManagerRegistrationTest {
    private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    @Test
    void testFreeSlot() {
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final FineGrainedTaskManagerRegistration taskManager =
                new FineGrainedTaskManagerRegistration(
                        TASK_EXECUTOR_CONNECTION, totalResource, totalResource);
        final AllocationID allocationId = new AllocationID();
        final JobID jobId = new JobID();
        final FineGrainedTaskManagerSlot slot =
                new FineGrainedTaskManagerSlot(
                        allocationId,
                        jobId,
                        ResourceProfile.fromResources(2, 100),
                        TASK_EXECUTOR_CONNECTION,
                        SlotState.ALLOCATED);
        taskManager.notifyAllocation(allocationId, slot);

        taskManager.freeSlot(allocationId);
        assertThat(taskManager.getAvailableResource()).isEqualTo(totalResource);
        assertThat(taskManager.getIdleSince()).isNotEqualTo(Long.MAX_VALUE);
        assertThat(taskManager.getAllocatedSlots()).isEmpty();
    }

    @Test
    void testNotifyAllocation() {
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final FineGrainedTaskManagerRegistration taskManager =
                new FineGrainedTaskManagerRegistration(
                        TASK_EXECUTOR_CONNECTION, totalResource, totalResource);
        final AllocationID allocationId = new AllocationID();
        final JobID jobId = new JobID();
        final FineGrainedTaskManagerSlot slot =
                new FineGrainedTaskManagerSlot(
                        allocationId,
                        jobId,
                        ResourceProfile.fromResources(2, 100),
                        TASK_EXECUTOR_CONNECTION,
                        SlotState.ALLOCATED);

        taskManager.notifyAllocation(allocationId, slot);
        assertThat(taskManager.getAvailableResource())
                .isEqualTo(ResourceProfile.fromResources(8, 900));
        assertThat(taskManager.getIdleSince()).isEqualTo(Long.MAX_VALUE);
        assertThat(taskManager.getAllocatedSlots()).containsKey(allocationId);
    }

    @Test
    void testNotifyAllocationComplete() {
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final FineGrainedTaskManagerRegistration taskManager =
                new FineGrainedTaskManagerRegistration(
                        TASK_EXECUTOR_CONNECTION, totalResource, totalResource);
        final AllocationID allocationId = new AllocationID();
        final JobID jobId = new JobID();
        final FineGrainedTaskManagerSlot slot =
                new FineGrainedTaskManagerSlot(
                        allocationId,
                        jobId,
                        ResourceProfile.fromResources(2, 100),
                        TASK_EXECUTOR_CONNECTION,
                        SlotState.PENDING);

        taskManager.notifyAllocation(allocationId, slot);
        assertThat(taskManager.getAvailableResource())
                .isEqualTo(ResourceProfile.fromResources(8, 900));
        assertThat(taskManager.getIdleSince()).isEqualTo(Long.MAX_VALUE);
        assertThat(taskManager.getAllocatedSlots()).containsKey(allocationId);

        taskManager.notifyAllocationComplete(allocationId);
        assertThat(taskManager.getAvailableResource())
                .isEqualTo(ResourceProfile.fromResources(8, 900));
        assertThat(taskManager.getIdleSince()).isEqualTo(Long.MAX_VALUE);
        assertThat(taskManager.getAllocatedSlots()).containsKey(allocationId);
        assertThat(taskManager.getAllocatedSlots().get(allocationId).getState())
                .isEqualTo(SlotState.ALLOCATED);
    }

    @Test
    void testNotifyAllocationWithoutEnoughResource() {
        final ResourceProfile totalResource = ResourceProfile.fromResources(1, 100);
        final FineGrainedTaskManagerRegistration taskManager =
                new FineGrainedTaskManagerRegistration(
                        TASK_EXECUTOR_CONNECTION, totalResource, totalResource);
        final AllocationID allocationId = new AllocationID();
        final JobID jobId = new JobID();
        final FineGrainedTaskManagerSlot slot1 =
                new FineGrainedTaskManagerSlot(
                        allocationId,
                        jobId,
                        ResourceProfile.fromResources(2, 100),
                        TASK_EXECUTOR_CONNECTION,
                        SlotState.PENDING);
        final FineGrainedTaskManagerSlot slot2 =
                new FineGrainedTaskManagerSlot(
                        allocationId,
                        jobId,
                        ResourceProfile.fromResources(2, 100),
                        TASK_EXECUTOR_CONNECTION,
                        SlotState.ALLOCATED);

        final List<RuntimeException> exceptions = new ArrayList<>();
        try {
            taskManager.notifyAllocation(allocationId, slot1);
        } catch (IllegalStateException e) {
            exceptions.add(e);
        }
        try {
            taskManager.notifyAllocation(allocationId, slot2);
        } catch (IllegalArgumentException e) {
            exceptions.add(e);
        }
        assertThat(exceptions).hasSize(2);
    }
}
