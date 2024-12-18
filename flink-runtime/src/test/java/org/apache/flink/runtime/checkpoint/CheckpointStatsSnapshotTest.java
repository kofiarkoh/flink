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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.testutils.CommonTestUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class CheckpointStatsSnapshotTest {

    /** Tests that the snapshot is actually serializable. */
    @Test
    void testIsJavaSerializable() throws Exception {
        CheckpointStatsCounts counts = new CheckpointStatsCounts();
        counts.incrementInProgressCheckpoints();
        counts.incrementInProgressCheckpoints();
        counts.incrementInProgressCheckpoints();
        counts.incrementCompletedCheckpoints();
        counts.incrementFailedCheckpoints();
        counts.incrementRestoredCheckpoints();

        CompletedCheckpointStatsSummary summary = new CompletedCheckpointStatsSummary();
        summary.updateSummary(createCompletedCheckpointsStats(12398, 9919));
        summary.updateSummary(createCompletedCheckpointsStats(2221, 3333));

        CheckpointStatsHistory history = new CheckpointStatsHistory(1);
        RestoredCheckpointStats restored =
                new RestoredCheckpointStats(
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        99119,
                        null,
                        42);

        CheckpointStatsSnapshot snapshot =
                new CheckpointStatsSnapshot(counts, summary.createSnapshot(), history, restored);

        CheckpointStatsSnapshot copy = CommonTestUtils.createCopySerializable(snapshot);

        assertThat(copy.getCounts().getNumberOfCompletedCheckpoints())
                .isEqualTo(counts.getNumberOfCompletedCheckpoints());
        assertThat(copy.getCounts().getNumberOfFailedCheckpoints())
                .isEqualTo(counts.getNumberOfFailedCheckpoints());
        assertThat(copy.getCounts().getNumberOfInProgressCheckpoints())
                .isEqualTo(counts.getNumberOfInProgressCheckpoints());
        assertThat(copy.getCounts().getNumberOfRestoredCheckpoints())
                .isEqualTo(counts.getNumberOfRestoredCheckpoints());
        assertThat(copy.getCounts().getTotalNumberOfCheckpoints())
                .isEqualTo(counts.getTotalNumberOfCheckpoints());

        assertThat(copy.getSummaryStats().getStateSizeStats().getSum())
                .isEqualTo(summary.getStateSizeStats().getSum());
        assertThat(copy.getSummaryStats().getEndToEndDurationStats().getSum())
                .isEqualTo(summary.getEndToEndDurationStats().getSum());

        assertThat(copy.getLatestRestoredCheckpoint().getCheckpointId())
                .isEqualTo(restored.getCheckpointId());
    }

    private CompletedCheckpointStats createCompletedCheckpointsStats(
            long stateSize, long endToEndDuration) {

        CompletedCheckpointStats completed = mock(CompletedCheckpointStats.class);
        when(completed.getStateSize()).thenReturn(stateSize);
        when(completed.getEndToEndDuration()).thenReturn(endToEndDuration);

        return completed;
    }
}
