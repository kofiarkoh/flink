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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.exceptions.UnfulfillableSlotRequestException;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.concurrent.FutureUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for batch slot requests. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class SlotPoolBatchSlotRequestTest {

    private static final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1024);
    public static final CompletableFuture[] COMPLETABLE_FUTURES_EMPTY_ARRAY =
            new CompletableFuture[0];
    private static ScheduledExecutorService singleThreadScheduledExecutorService;
    private static ComponentMainThreadExecutor mainThreadExecutor;

    @BeforeAll
    private static void setupClass() {
        singleThreadScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        singleThreadScheduledExecutorService);
    }

    @AfterAll
    private static void teardownClass() {
        if (singleThreadScheduledExecutorService != null) {
            singleThreadScheduledExecutorService.shutdownNow();
        }
    }

    /**
     * Tests that a batch slot request fails if there is no slot which can fulfill the slot request.
     */
    @Test
    void testPendingBatchSlotRequestTimeout() throws Exception {
        try (final SlotPool slotPool =
                createAndSetUpSlotPool(mainThreadExecutor, null, Duration.ofMillis(2L))) {
            final CompletableFuture<PhysicalSlot> slotFuture =
                    SlotPoolUtils.requestNewAllocatedBatchSlot(
                            slotPool, mainThreadExecutor, ResourceProfile.UNKNOWN);

            assertThatThrownBy(slotFuture::get)
                    .withFailMessage("Expected that slot future times out.")
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(TimeoutException.class);
        }
    }

    /**
     * Tests that a batch slot request won't time out if there exists a slot in the SlotPool which
     * fulfills the requested {@link ResourceProfile}.
     */
    @Test
    void testPendingBatchSlotRequestDoesNotTimeoutIfFulfillingSlotExists() throws Exception {
        final Duration batchSlotTimeout = Duration.ofMillis(2L);
        final ManualClock clock = new ManualClock();

        try (final DeclarativeSlotPoolBridge slotPool =
                createAndSetUpSlotPool(mainThreadExecutor, null, batchSlotTimeout, clock)) {

            SlotPoolUtils.requestNewAllocatedBatchSlot(
                    slotPool, mainThreadExecutor, resourceProfile);

            SlotPoolUtils.offerSlots(slotPool, mainThreadExecutor, Arrays.asList(resourceProfile));

            final CompletableFuture<PhysicalSlot> firstPendingSlotFuture =
                    SlotPoolUtils.requestNewAllocatedBatchSlot(
                            slotPool, mainThreadExecutor, ResourceProfile.UNKNOWN);
            final CompletableFuture<PhysicalSlot> secondPendingSlotFuture =
                    SlotPoolUtils.requestNewAllocatedBatchSlot(
                            slotPool, mainThreadExecutor, resourceProfile);

            final List<CompletableFuture<PhysicalSlot>> slotFutures =
                    Arrays.asList(firstPendingSlotFuture, secondPendingSlotFuture);

            advanceTimeAndTriggerCheckBatchSlotTimeout(
                    slotPool, mainThreadExecutor, clock, batchSlotTimeout);

            for (CompletableFuture<PhysicalSlot> slotFuture : slotFutures) {
                assertThatFuture(slotFuture).isNotDone();
            }
        }
    }

    /**
     * Tests that a batch slot request won't fail if its resource manager request fails with
     * exceptions other than {@link UnfulfillableSlotRequestException}.
     */
    @Test
    void testPendingBatchSlotRequestDoesNotFailIfResourceDeclaringFails() throws Exception {
        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        testingResourceManagerGateway.setDeclareRequiredResourcesFunction(
                (jobMasterId, resourceRequirements) ->
                        FutureUtils.completedExceptionally(new FlinkException("Failed request")));

        final Duration batchSlotTimeout = Duration.ofMillis(1000L);
        try (final SlotPool slotPool =
                createAndSetUpSlotPool(
                        mainThreadExecutor, testingResourceManagerGateway, batchSlotTimeout)) {

            final CompletableFuture<PhysicalSlot> slotFuture =
                    SlotPoolUtils.requestNewAllocatedBatchSlot(
                            slotPool, mainThreadExecutor, resourceProfile);

            assertThatFuture(slotFuture).willNotCompleteWithin(Duration.ofMillis(50L));
        }
    }

    /**
     * Tests that a pending batch slot request times out after the last fulfilling slot gets
     * released.
     */
    @Test
    void testPendingBatchSlotRequestTimeoutAfterSlotRelease() throws Exception {
        final ManualClock clock = new ManualClock();
        final Duration batchSlotTimeout = Duration.ofMillis(10000L);

        try (final DeclarativeSlotPoolBridge slotPool =
                createAndSetUpSlotPool(mainThreadExecutor, null, batchSlotTimeout, clock)) {

            SlotPoolUtils.requestNewAllocatedBatchSlot(
                    slotPool, mainThreadExecutor, resourceProfile);

            final ResourceID taskManagerResourceId =
                    SlotPoolUtils.offerSlots(
                            slotPool, mainThreadExecutor, Arrays.asList(resourceProfile));

            final CompletableFuture<PhysicalSlot> firstPendingSlotFuture =
                    SlotPoolUtils.requestNewAllocatedBatchSlot(
                            slotPool, mainThreadExecutor, ResourceProfile.UNKNOWN);
            final CompletableFuture<PhysicalSlot> secondPendingSlotFuture =
                    SlotPoolUtils.requestNewAllocatedBatchSlot(
                            slotPool, mainThreadExecutor, resourceProfile);

            final List<CompletableFuture<PhysicalSlot>> slotFutures =
                    Arrays.asList(firstPendingSlotFuture, secondPendingSlotFuture);

            // initial batch slot timeout check
            advanceTimeAndTriggerCheckBatchSlotTimeout(
                    slotPool, mainThreadExecutor, clock, batchSlotTimeout);

            assertThatFuture(
                            CompletableFuture.anyOf(
                                    slotFutures.toArray(COMPLETABLE_FUTURES_EMPTY_ARRAY)))
                    .isNotDone();

            SlotPoolUtils.releaseTaskManager(slotPool, mainThreadExecutor, taskManagerResourceId);

            advanceTimeAndTriggerCheckBatchSlotTimeout(
                    slotPool, mainThreadExecutor, clock, batchSlotTimeout);

            for (CompletableFuture<PhysicalSlot> slotFuture : slotFutures) {
                assertThatFuture(slotFuture).isCompletedExceptionally();

                assertThatThrownBy(slotFuture::get)
                        .withFailMessage("Expected that the slot future times out.")
                        .isInstanceOf(ExecutionException.class)
                        .hasRootCauseInstanceOf(TimeoutException.class);
            }
        }
    }

    private void advanceTimeAndTriggerCheckBatchSlotTimeout(
            DeclarativeSlotPoolBridge slotPool,
            ComponentMainThreadExecutor componentMainThreadExecutor,
            ManualClock clock,
            Duration batchSlotTimeout) {
        // trigger batch slot timeout check which marks unfulfillable slots
        runBatchSlotTimeoutCheck(slotPool, componentMainThreadExecutor);

        // advance clock behind timeout
        clock.advanceTime(batchSlotTimeout.toMillis() + 1L, TimeUnit.MILLISECONDS);

        // timeout all as unfulfillable marked slots
        runBatchSlotTimeoutCheck(slotPool, componentMainThreadExecutor);
    }

    private void runBatchSlotTimeoutCheck(
            DeclarativeSlotPoolBridge slotPool,
            ComponentMainThreadExecutor componentMainThreadExecutor) {
        CompletableFuture.runAsync(slotPool::checkBatchSlotTimeout, componentMainThreadExecutor)
                .join();
    }

    private DeclarativeSlotPoolBridge createAndSetUpSlotPool(
            final ComponentMainThreadExecutor componentMainThreadExecutor,
            @Nullable final ResourceManagerGateway resourceManagerGateway,
            final Duration batchSlotTimeout)
            throws Exception {

        return new DeclarativeSlotPoolBridgeBuilder()
                .setResourceManagerGateway(resourceManagerGateway)
                .setBatchSlotTimeout(batchSlotTimeout)
                .setMainThreadExecutor(componentMainThreadExecutor)
                .buildAndStart();
    }

    private DeclarativeSlotPoolBridge createAndSetUpSlotPool(
            final ComponentMainThreadExecutor componentMainThreadExecutor,
            @Nullable final ResourceManagerGateway resourceManagerGateway,
            final Duration batchSlotTimeout,
            final Clock clock)
            throws Exception {

        return new DeclarativeSlotPoolBridgeBuilder()
                .setResourceManagerGateway(resourceManagerGateway)
                .setBatchSlotTimeout(batchSlotTimeout)
                .setClock(clock)
                .setMainThreadExecutor(componentMainThreadExecutor)
                .buildAndStart();
    }
}
