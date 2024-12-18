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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

/** Tests that the {@link JobConfigInfo} can be marshalled and unmarshalled. */
@ExtendWith(NoOpTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class JobConfigInfoTest extends RestResponseMarshallingTestBase<JobConfigInfo> {

    @Override
    protected Class<JobConfigInfo> getTestResponseClass() {
        return JobConfigInfo.class;
    }

    @Override
    protected JobConfigInfo getTestResponseInstance() {
        final Map<String, String> globalJobParameters = new HashMap<>(3);
        globalJobParameters.put("foo", "bar");
        globalJobParameters.put("bar", "foo");
        globalJobParameters.put("hi", "ho");

        final JobConfigInfo.ExecutionConfigInfo executionConfigInfo =
                new JobConfigInfo.ExecutionConfigInfo("always", 42, false, globalJobParameters);
        return new JobConfigInfo(new JobID(), "testJob", executionConfigInfo);
    }
}
