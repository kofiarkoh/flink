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

import org.apache.flink.util.clock.ManualClock;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PhysicalSlotRequestBulkWithTimestamp}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PhysicalSlotRequestBulkWithTimestampTest {

    private final ManualClock clock = new ManualClock();

    @Test
    void testMarkBulkUnfulfillable() {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                createPhysicalSlotRequestBulkWithTimestamp();

        clock.advanceTime(456, TimeUnit.MILLISECONDS);
        bulk.markUnfulfillable(clock.relativeTimeMillis());

        assertThat(bulk.getUnfulfillableSince()).isEqualTo(clock.relativeTimeMillis());
    }

    @Test
    void testUnfulfillableTimestampWillNotBeOverriddenByFollowingUnfulfillableTimestamp() {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                createPhysicalSlotRequestBulkWithTimestamp();

        final long unfulfillableSince = clock.relativeTimeMillis();
        bulk.markUnfulfillable(unfulfillableSince);

        clock.advanceTime(456, TimeUnit.MILLISECONDS);
        bulk.markUnfulfillable(clock.relativeTimeMillis());

        assertThat(bulk.getUnfulfillableSince()).isEqualTo(unfulfillableSince);
    }

    private static PhysicalSlotRequestBulkWithTimestamp
            createPhysicalSlotRequestBulkWithTimestamp() {
        return TestingPhysicalSlotRequestBulkBuilder.newBuilder()
                .buildPhysicalSlotRequestBulkWithTimestamp();
    }
}
