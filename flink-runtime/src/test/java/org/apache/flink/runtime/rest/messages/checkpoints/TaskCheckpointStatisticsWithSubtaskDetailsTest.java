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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

/** Tests (un)marshalling of {@link TaskCheckpointStatisticsWithSubtaskDetails}. */
@ExtendWith(NoOpTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TaskCheckpointStatisticsWithSubtaskDetailsTest
        extends RestResponseMarshallingTestBase<TaskCheckpointStatisticsWithSubtaskDetails> {

    @Override
    protected Class<TaskCheckpointStatisticsWithSubtaskDetails> getTestResponseClass() {
        return TaskCheckpointStatisticsWithSubtaskDetails.class;
    }

    @Override
    protected TaskCheckpointStatisticsWithSubtaskDetails getTestResponseInstance()
            throws Exception {
        final TaskCheckpointStatisticsWithSubtaskDetails.Summary summary =
                new TaskCheckpointStatisticsWithSubtaskDetails.Summary(
                        new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0),
                        new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0),
                        new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0),
                        new TaskCheckpointStatisticsWithSubtaskDetails.CheckpointDuration(
                                new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0),
                                new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0)),
                        new TaskCheckpointStatisticsWithSubtaskDetails.CheckpointAlignment(
                                new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0),
                                new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0),
                                new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0),
                                new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0)),
                        new StatsSummaryDto(1L, 2L, 3L, 0, 0, 0, 0, 0));

        List<SubtaskCheckpointStatistics> subtaskCheckpointStatistics = new ArrayList<>(2);

        subtaskCheckpointStatistics.add(
                new SubtaskCheckpointStatistics.PendingSubtaskCheckpointStatistics(0));
        subtaskCheckpointStatistics.add(
                new SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics(
                        1,
                        4L,
                        13L,
                        1337L,
                        1337L,
                        new SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics
                                .CheckpointDuration(1L, 2L),
                        new SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics
                                .CheckpointAlignment(2L, 4L, 5L, 3L),
                        42L,
                        true,
                        false));

        return new TaskCheckpointStatisticsWithSubtaskDetails(
                4L,
                CheckpointStatsStatus.COMPLETED,
                4L,
                1337L,
                1337L,
                1L,
                2L,
                10,
                11,
                8,
                9,
                summary,
                subtaskCheckpointStatistics);
    }
}
