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
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.ResourceCounter;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for the {@link FineGrainedTaskManagerTracker}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class FineGrainedTaskManagerTrackerTest {
    private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    @Test
    void testInitState() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        assertThat(taskManagerTracker.getPendingTaskManagers()).isEmpty();
        assertThat(taskManagerTracker.getRegisteredTaskManagers()).isEmpty();
    }

    @Test
    void testAddAndRemoveTaskManager() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();

        // Add task manager
        taskManagerTracker.addTaskManager(
                TASK_EXECUTOR_CONNECTION, ResourceProfile.ANY, ResourceProfile.ANY);
        assertThat(taskManagerTracker.getRegisteredTaskManagers()).hasSize(1);
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .isPresent();

        // Remove task manager
        taskManagerTracker.removeTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID());
        assertThat(taskManagerTracker.getRegisteredTaskManagers()).isEmpty();
    }

    @Test
    void testRemoveUnknownTaskManager() {
        assertThatThrownBy(
                        () -> {
                            final FineGrainedTaskManagerTracker taskManagerTracker =
                                    new FineGrainedTaskManagerTracker();
                            taskManagerTracker.removeTaskManager(new InstanceID());
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testAddAndRemovePendingTaskManager() {
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(ResourceProfile.ANY, 1);
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final JobID jobId = new JobID();
        final ResourceCounter resourceCounter =
                ResourceCounter.withResource(ResourceProfile.ANY, 1);

        // Add pending task manager
        taskManagerTracker.addPendingTaskManager(pendingTaskManager);
        taskManagerTracker.replaceAllPendingAllocations(
                Collections.singletonMap(
                        pendingTaskManager.getPendingTaskManagerId(),
                        Collections.singletonMap(jobId, resourceCounter)));
        assertThat(taskManagerTracker.getPendingTaskManagers()).hasSize(1);
        assertThat(
                        taskManagerTracker
                                .getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                                        ResourceProfile.ANY, ResourceProfile.ANY))
                .hasSize(1);

        // Remove pending task manager
        final Map<JobID, ResourceCounter> records =
                taskManagerTracker.removePendingTaskManager(
                        pendingTaskManager.getPendingTaskManagerId());
        assertThat(taskManagerTracker.getPendingTaskManagers()).isEmpty();
        assertThat(
                        taskManagerTracker
                                .getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                                        ResourceProfile.ANY, ResourceProfile.ANY))
                .isEmpty();
        assertThat(records).containsKey(jobId);
        assertThat(records.get(jobId).getResourceCount(ResourceProfile.ANY)).isEqualTo(1);
    }

    @Test
    void testRemoveUnknownPendingTaskManager() {
        assertThatThrownBy(
                        () -> {
                            final FineGrainedTaskManagerTracker taskManagerTracker =
                                    new FineGrainedTaskManagerTracker();

                            taskManagerTracker.removePendingTaskManager(
                                    PendingTaskManagerId.generate());
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testSlotAllocation() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final JobID jobId = new JobID();
        taskManagerTracker.addTaskManager(TASK_EXECUTOR_CONNECTION, totalResource, totalResource);
        // Notify free slot is now pending
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.PENDING);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1)).isPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(7, 800)));

        // Notify pending slot is now allocated
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.ALLOCATED);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1)).isPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(7, 800)));

        // Notify free slot is now allocated
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(2, 300),
                SlotState.ALLOCATED);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2)).isPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(5, 500)));
    }

    @Test
    void testFreeSlot() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final JobID jobId = new JobID();
        taskManagerTracker.addTaskManager(TASK_EXECUTOR_CONNECTION, totalResource, totalResource);
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.PENDING);
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(2, 300),
                SlotState.ALLOCATED);

        // Free pending slot
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.FREE);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1)).isNotPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(8, 700)));
        // Free allocated slot
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(2, 300),
                SlotState.FREE);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2)).isNotPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(totalResource));
    }

    @Test
    void testFreeUnknownSlot() {
        assertThatThrownBy(
                        () -> {
                            final FineGrainedTaskManagerTracker taskManagerTracker =
                                    new FineGrainedTaskManagerTracker();

                            taskManagerTracker.notifySlotStatus(
                                    new AllocationID(),
                                    new JobID(),
                                    new InstanceID(),
                                    ResourceProfile.ANY,
                                    SlotState.FREE);
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testRecordPendingAllocations() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final PendingTaskManager pendingTaskManager1 =
                new PendingTaskManager(ResourceProfile.ANY, 1);
        final PendingTaskManager pendingTaskManager2 =
                new PendingTaskManager(ResourceProfile.ANY, 1);
        final JobID jobId = new JobID();
        final ResourceCounter resourceCounter =
                ResourceCounter.withResource(ResourceProfile.ANY, 1);
        taskManagerTracker.addPendingTaskManager(pendingTaskManager1);
        taskManagerTracker.addPendingTaskManager(pendingTaskManager2);

        taskManagerTracker.replaceAllPendingAllocations(
                Collections.singletonMap(
                        pendingTaskManager1.getPendingTaskManagerId(),
                        Collections.singletonMap(jobId, resourceCounter)));
        // Only the last time is recorded
        taskManagerTracker.replaceAllPendingAllocations(
                Collections.singletonMap(
                        pendingTaskManager2.getPendingTaskManagerId(),
                        Collections.singletonMap(jobId, resourceCounter)));
        assertThat(pendingTaskManager1.getPendingSlotAllocationRecords()).isEmpty();
        assertThat(pendingTaskManager2.getPendingSlotAllocationRecords())
                .contains(entry(jobId, ResourceCounter.withResource(ResourceProfile.ANY, 1)));
    }

    @Test
    void testPendingTaskManagerUnusedResources() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final ResourceProfile defaultSlotResource = ResourceProfile.fromResources(1, 100);
        final PendingTaskManager pendingTaskManager = new PendingTaskManager(totalResource, 10);
        final JobID jobId = new JobID();
        final ResourceCounter resourceCounter =
                ResourceCounter.withResource(defaultSlotResource, 1);

        assertThat(pendingTaskManager.getUnusedResource()).isEqualTo(totalResource);

        taskManagerTracker.addPendingTaskManager(pendingTaskManager);
        taskManagerTracker.replaceAllPendingAllocations(
                Collections.singletonMap(
                        pendingTaskManager.getPendingTaskManagerId(),
                        Collections.singletonMap(jobId, resourceCounter)));

        assertThat(pendingTaskManager.getUnusedResource())
                .isEqualTo(totalResource.subtract(defaultSlotResource));
    }

    @Test
    void testGetStatistics() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final ResourceProfile defaultSlotResource = ResourceProfile.fromResources(1, 100);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final JobID jobId = new JobID();
        taskManagerTracker.addTaskManager(
                TASK_EXECUTOR_CONNECTION, totalResource, defaultSlotResource);
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.ALLOCATED);
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                defaultSlotResource,
                SlotState.ALLOCATED);
        taskManagerTracker.addPendingTaskManager(
                new PendingTaskManager(ResourceProfile.fromResources(4, 200), 1));

        assertThat(taskManagerTracker.getFreeResource())
                .isEqualTo(ResourceProfile.fromResources(6, 700));
        assertThat(taskManagerTracker.getRegisteredResource()).isEqualTo(totalResource);
        assertThat(taskManagerTracker.getNumberRegisteredSlots()).isEqualTo(10);
        assertThat(taskManagerTracker.getNumberFreeSlots()).isEqualTo(8);
        assertThat(taskManagerTracker.getPendingResource())
                .isEqualTo(ResourceProfile.fromResources(4, 200));
    }
}
