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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for the {@link JobStatusInfo}. */
@ExtendWith(NoOpTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class JobStatusInfoTest extends RestResponseMarshallingTestBase<JobStatusInfo> {
    @Override
    protected Class<JobStatusInfo> getTestResponseClass() {
        return JobStatusInfo.class;
    }

    @Override
    protected JobStatusInfo getTestResponseInstance() throws Exception {
        return new JobStatusInfo(JobStatus.CREATED);
    }
}
