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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.function.RunnableWithException;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;

/**
 * Base @ExtendWith(CTestJUnit5Extension.class) @CTestClass class for the tests of {@link
 * FineGrainedSlotManager}.
 */
abstract @ExtendWith(CTestJUnit5Extension.class) @CTestClass class FineGrainedSlotManagerTestBase {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final long FUTURE_TIMEOUT_SECOND = 5;
    private static final long FUTURE_EXPECT_TIMEOUT_MS = 50;
    static final FlinkException TEST_EXCEPTION = new FlinkException("Test exception");
    static final WorkerResourceSpec DEFAULT_WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(10.0)
                    .setTaskHeapMemoryMB(1000)
                    .setTaskOffHeapMemoryMB(1000)
                    .setNetworkMemoryMB(1000)
                    .setManagedMemoryMB(1000)
                    .build();
    static final int DEFAULT_NUM_SLOTS_PER_WORKER = 2;
    static final ResourceProfile DEFAULT_TOTAL_RESOURCE_PROFILE =
            SlotManagerUtils.generateTaskManagerTotalResourceProfile(DEFAULT_WORKER_RESOURCE_SPEC);
    static final ResourceProfile DEFAULT_SLOT_RESOURCE_PROFILE =
            SlotManagerUtils.generateDefaultSlotResourceProfile(
                    DEFAULT_WORKER_RESOURCE_SPEC, DEFAULT_NUM_SLOTS_PER_WORKER);

    protected abstract Optional<ResourceAllocationStrategy> getResourceAllocationStrategy(
            SlotManagerConfiguration slotManagerConfiguration);

    static SlotStatus createAllocatedSlotStatus(
            JobID jobId, AllocationID allocationID, ResourceProfile resourceProfile) {
        return new SlotStatus(
                new SlotID(ResourceID.generate(), 0), resourceProfile, jobId, allocationID);
    }

    static int getTotalResourceCount(Collection<ResourceRequirement> resources) {
        if (resources == null) {
            return 0;
        }
        return resources.stream()
                .map(ResourceRequirement::getNumberOfRequiredSlots)
                .reduce(0, Integer::sum);
    }

    static ResourceRequirements createResourceRequirementsForSingleSlot() {
        return createResourceRequirementsForSingleSlot(new JobID());
    }

    static ResourceRequirements createResourceRequirementsForSingleSlot(JobID jobId) {
        return createResourceRequirements(jobId, 1);
    }

    static ResourceRequirements createResourceRequirements(JobID jobId, int numRequiredSlots) {
        return createResourceRequirements(jobId, numRequiredSlots, ResourceProfile.UNKNOWN);
    }

    static ResourceRequirements createResourceRequirements(
            JobID jobId, int numRequiredSlots, ResourceProfile resourceProfile) {
        return ResourceRequirements.create(
                jobId,
                "foobar",
                Collections.singleton(
                        ResourceRequirement.create(resourceProfile, numRequiredSlots)));
    }

    static TaskExecutorConnection createTaskExecutorConnection() {
        return new TaskExecutorConnection(
                ResourceID.generate(),
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());
    }

    static TaskExecutorConnection createTaskExecutorConnection(
            TestingTaskExecutorGateway taskExecutorGateway) {
        return new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
    }

    static <T> T assertFutureCompleteAndReturn(CompletableFuture<T> completableFuture)
            throws Exception {
        return completableFuture.get(FUTURE_TIMEOUT_SECOND, TimeUnit.SECONDS);
    }

    static void assertFutureNotComplete(CompletableFuture<?> completableFuture) {
        assertThatFuture(completableFuture)
                .withFailMessage("Expected to fail with a timeout.")
                .failsWithin(FUTURE_EXPECT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .withThrowableOfType(TimeoutException.class);
    }

    /**
     * This @ExtendWith(CTestJUnit5Extension.class) @CTestClass class provides a self-contained
     * context for each test case.
     */
    protected class Context {
        private final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        private final ResourceTracker resourceTracker = new DefaultResourceTracker();
        private final TaskManagerTracker taskManagerTracker = new FineGrainedTaskManagerTracker();
        private final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(Duration.ofSeconds(10L));
        private SlotManagerMetricGroup slotManagerMetricGroup =
                UnregisteredMetricGroups.createUnregisteredSlotManagerMetricGroup();
        private BlockedTaskManagerChecker blockedTaskManagerChecker = resourceID -> false;
        private final ScheduledExecutor scheduledExecutor =
                new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor());
        private final Executor mainThreadExecutor = EXECUTOR_RESOURCE.getExecutor();
        private FineGrainedSlotManager slotManager;

        final TestingResourceAllocationStrategy.Builder resourceAllocationStrategyBuilder =
                TestingResourceAllocationStrategy.newBuilder();

        final TestingResourceAllocatorBuilder resourceAllocatorBuilder =
                new TestingResourceAllocatorBuilder();

        final TestingResourceEventListenerBuilder resourceEventListenerBuilder =
                new TestingResourceEventListenerBuilder();
        final SlotManagerConfigurationBuilder slotManagerConfigurationBuilder =
                SlotManagerConfigurationBuilder.newBuilder();

        FineGrainedSlotManager getSlotManager() {
            return slotManager;
        }

        ResourceTracker getResourceTracker() {
            return resourceTracker;
        }

        TaskManagerTracker getTaskManagerTracker() {
            return taskManagerTracker;
        }

        ResourceManagerId getResourceManagerId() {
            return resourceManagerId;
        }

        public void setSlotManagerMetricGroup(SlotManagerMetricGroup slotManagerMetricGroup) {
            this.slotManagerMetricGroup = slotManagerMetricGroup;
        }

        public void setBlockedTaskManagerChecker(
                BlockedTaskManagerChecker blockedTaskManagerChecker) {
            this.blockedTaskManagerChecker = blockedTaskManagerChecker;
        }

        void runInMainThread(Runnable runnable) {
            mainThreadExecutor.execute(runnable);
        }

        void runInMainThreadAndWait(Runnable runnable) throws InterruptedException {
            final OneShotLatch latch = new OneShotLatch();
            mainThreadExecutor.execute(
                    () -> {
                        runnable.run();
                        latch.trigger();
                    });
            latch.await();
        }

        protected final void runTest(RunnableWithException testMethod) throws Exception {
            SlotManagerConfiguration configuration = slotManagerConfigurationBuilder.build();
            slotManager =
                    FineGrainedSlotManagerBuilder.newBuilder(scheduledExecutor)
                            .setSlotManagerConfiguration(configuration)
                            .setSlotManagerMetricGroup(slotManagerMetricGroup)
                            .setResourceTracker(resourceTracker)
                            .setTaskManagerTracker(taskManagerTracker)
                            .setSlotStatusSyncer(slotStatusSyncer)
                            .setResourceAllocationStrategy(
                                    getResourceAllocationStrategy(configuration)
                                            .orElse(resourceAllocationStrategyBuilder.build()))
                            .build();

            runInMainThreadAndWait(
                    () ->
                            slotManager.start(
                                    resourceManagerId,
                                    mainThreadExecutor,
                                    resourceAllocatorBuilder.build(),
                                    resourceEventListenerBuilder.build(),
                                    blockedTaskManagerChecker));

            testMethod.run();

            CompletableFuture<Void> closeFuture = new CompletableFuture<>();
            runInMainThread(
                    () -> {
                        try {
                            slotManager.close();
                        } catch (Exception e) {
                            closeFuture.completeExceptionally(e);
                        }
                        closeFuture.complete(null);
                    });
            FutureUtils.assertNoException(closeFuture);
        }
    }
}
