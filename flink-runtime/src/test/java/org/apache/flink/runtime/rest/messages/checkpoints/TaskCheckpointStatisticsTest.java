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

/** Tests the (un)marshalling of {@link TaskCheckpointStatistics}. */
@ExtendWith(NoOpTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TaskCheckpointStatisticsTest
        extends RestResponseMarshallingTestBase<TaskCheckpointStatistics> {

    @Override
    protected Class<TaskCheckpointStatistics> getTestResponseClass() {
        return TaskCheckpointStatistics.class;
    }

    @Override
    protected TaskCheckpointStatistics getTestResponseInstance() throws Exception {
        return new TaskCheckpointStatistics(
                1L, CheckpointStatsStatus.FAILED, 42L, 1L, 1L, 23L, 1337L, 1338, 1339, 9, 8);
    }
}
