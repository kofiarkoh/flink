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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.SubtaskIndexPathParameter;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SubtaskMetricsHeaders}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class SubtaskMetricsHeadersTest {

    private final SubtaskMetricsHeaders subtaskMetricsHeaders = SubtaskMetricsHeaders.getInstance();

    @Test
    void testUrl() {
        assertThat(subtaskMetricsHeaders.getTargetRestEndpointURL())
                .isEqualTo(
                        "/jobs/:"
                                + JobIDPathParameter.KEY
                                + "/vertices/:"
                                + JobVertexIdPathParameter.KEY
                                + "/subtasks/:"
                                + SubtaskIndexPathParameter.KEY
                                + "/metrics");
    }

    @Test
    void testMessageParameters() {
        assertThat(subtaskMetricsHeaders.getUnresolvedMessageParameters())
                .isInstanceOf(SubtaskMetricsMessageParameters.class);
    }
}
