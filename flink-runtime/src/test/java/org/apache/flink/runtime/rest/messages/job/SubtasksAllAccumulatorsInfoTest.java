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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

/** Tests (un)marshalling of the {@link SubtasksAllAccumulatorsInfo}. */
@ExtendWith(NoOpTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class SubtasksAllAccumulatorsInfoTest
        extends RestResponseMarshallingTestBase<SubtasksAllAccumulatorsInfo> {
    @Override
    protected Class<SubtasksAllAccumulatorsInfo> getTestResponseClass() {
        return SubtasksAllAccumulatorsInfo.class;
    }

    @Override
    protected SubtasksAllAccumulatorsInfo getTestResponseInstance() throws Exception {
        List<SubtasksAllAccumulatorsInfo.SubtaskAccumulatorsInfo> subtaskAccumulatorsInfos =
                new ArrayList<>(3);

        List<UserAccumulator> userAccumulators = new ArrayList<>(2);
        userAccumulators.add(new UserAccumulator("test name1", "test type1", "test value1"));
        userAccumulators.add(new UserAccumulator("test name2", "test type2", "test value2"));

        for (int i = 0; i < 3; ++i) {
            subtaskAccumulatorsInfos.add(
                    new SubtasksAllAccumulatorsInfo.SubtaskAccumulatorsInfo(
                            i, i, "host-" + i + ":123", userAccumulators));
        }
        return new SubtasksAllAccumulatorsInfo(new JobVertexID(), 4, subtaskAccumulatorsInfos);
    }
}
