/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfileTestingUtils;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LocationPreferenceSlotSelectionStrategy}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class LocationPreferenceSlotSelectionStrategyTest extends SlotSelectionStrategyTestBase {

    @BeforeEach
    void setUp() {
        this.selectionStrategy = LocationPreferenceSlotSelectionStrategy.createDefault();
    }

    @Test
    void testPhysicalSlotResourceProfileRespected() {

        final SlotProfile slotProfile =
                SlotProfile.priorAllocation(
                        resourceProfile,
                        biggerResourceProfile,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptySet());

        Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);
        assertThat(match)
                .hasValueSatisfying(
                        slotInfoAndLocality ->
                                assertThat(
                                                slotInfoAndLocality
                                                        .getSlotInfo()
                                                        .getResourceProfile()
                                                        .isMatching(
                                                                slotProfile
                                                                        .getPhysicalSlotResourceProfile()))
                                        .isTrue());

        ResourceProfile evenBiggerResourceProfile =
                ResourceProfile.fromResources(
                        biggerResourceProfile.getCpuCores().getValue().doubleValue() + 1.0,
                        resourceProfile.getTaskHeapMemory().getMebiBytes());
        final SlotProfile slotProfileNotMatching =
                SlotProfile.priorAllocation(
                        resourceProfile,
                        evenBiggerResourceProfile,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptySet());

        match = runMatching(slotProfileNotMatching);
        assertThat(match).isNotPresent();
    }

    @Test
    void matchNoRequirements() {

        SlotProfile slotProfile = SlotProfileTestingUtils.noRequirements();
        Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

        assertMatchingSlotLocalityAndInCandidates(match, Locality.UNCONSTRAINED, candidates);
    }

    @Test
    void returnsHostLocalMatchingIfExactTMLocationCannotBeFulfilled() {

        SlotProfile slotProfile =
                SlotProfileTestingUtils.preferredLocality(
                        resourceProfile, Collections.singletonList(tmlX));
        Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

        assertMatchingSlotLocalityAndInCandidates(match, Locality.HOST_LOCAL, candidates);
    }

    @Test
    void returnsNonLocalMatchingIfResourceProfileCanBeFulfilledButNotTheTMLocationPreferences()
            throws Exception {
        final InetAddress nonHostLocalInetAddress =
                InetAddress.getByAddress(new byte[] {10, 0, 0, 24});
        final TaskManagerLocation nonLocalTm =
                new TaskManagerLocation(
                        new ResourceID("non-local-tm"), nonHostLocalInetAddress, 42);
        SlotProfile slotProfile =
                SlotProfileTestingUtils.preferredLocality(
                        resourceProfile, Collections.singletonList(nonLocalTm));
        Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

        assertMatchingSlotLocalityAndInCandidates(match, Locality.NON_LOCAL, candidates);
    }

    @Test
    void matchPreferredLocation() {

        SlotProfile slotProfile =
                SlotProfileTestingUtils.preferredLocality(
                        biggerResourceProfile, Collections.singletonList(tml2));
        Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

        assertMatchingSlotEqualsToSlotInfo(match, slot2);

        slotProfile =
                SlotProfileTestingUtils.preferredLocality(
                        resourceProfile, Arrays.asList(tmlX, tml4));
        match = runMatching(slotProfile);

        assertMatchingSlotEqualsToSlotInfo(match, slot4);

        slotProfile =
                SlotProfileTestingUtils.preferredLocality(
                        resourceProfile, Arrays.asList(tml3, tml1, tml3, tmlX));
        match = runMatching(slotProfile);

        assertMatchingSlotEqualsToSlotInfo(match, slot3);
    }

    @Test
    void matchPreviousLocationAvailableButAlsoBlacklisted() {
        HashSet<AllocationID> blacklisted = new HashSet<>(4);
        blacklisted.add(aid1);
        blacklisted.add(aid2);
        blacklisted.add(aid3);
        blacklisted.add(aid4);
        SlotProfile slotProfile =
                SlotProfile.priorAllocation(
                        resourceProfile,
                        resourceProfile,
                        Collections.singletonList(tml3),
                        Collections.singletonList(aid3),
                        blacklisted);
        Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

        // available previous allocation should override blacklisting
        assertMatchingSlotEqualsToSlotInfo(match, slot3);
    }

    protected static void assertMatchingSlotEqualsToSlotInfo(
            Optional<SlotSelectionStrategy.SlotInfoAndLocality> matchingSlot, SlotInfo slotInfo) {
        assertThat(matchingSlot)
                .hasValueSatisfying(
                        slotInfoAndLocality ->
                                assertThat(slotInfoAndLocality.getSlotInfo()).isEqualTo(slotInfo));
    }

    protected static void assertMatchingSlotLocalityAndInCandidates(
            Optional<SlotSelectionStrategy.SlotInfoAndLocality> matchingSlot,
            Locality locality,
            FreeSlotTracker candidates) {
        assertThat(matchingSlot)
                .hasValueSatisfying(
                        slotInfoAndLocality -> {
                            assertThat(slotInfoAndLocality.getLocality()).isEqualTo(locality);
                            assertThat(candidates.getAvailableSlots())
                                    .anySatisfy(
                                            allocationId ->
                                                    assertThat(candidates.getSlotInfo(allocationId))
                                                            .isEqualTo(
                                                                    slotInfoAndLocality
                                                                            .getSlotInfo()));
                        });
    }
}
