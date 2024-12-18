/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionConfigBuilder;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobConfigHeaders;
import org.apache.flink.runtime.rest.messages.JobConfigInfo;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link JobConfigHandler}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class JobConfigHandlerTest {

    @Test
    void handleRequest_executionConfigWithSecretValues_excludesSecretValuesFromResponse()
            throws HandlerRequestException {
        final JobConfigHandler jobConfigHandler =
                new JobConfigHandler(
                        () -> null,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobConfigHeaders.getInstance(),
                        new DefaultExecutionGraphCache(TestingUtils.TIMEOUT, TestingUtils.TIMEOUT),
                        Executors.directExecutor());

        final Map<String, String> globalJobParameters = new HashMap<>();
        globalJobParameters.put("foobar", "barfoo");
        globalJobParameters.put("bar.secret.foo", "my secret");
        globalJobParameters.put("password.to.my.safe", "12345");

        final ArchivedExecutionConfig archivedExecutionConfig =
                new ArchivedExecutionConfigBuilder()
                        .setGlobalJobParameters(globalJobParameters)
                        .build();
        final AccessExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setArchivedExecutionConfig(archivedExecutionConfig)
                        .build();
        final HandlerRequest<EmptyRequestBody> handlerRequest =
                createRequest(archivedExecutionGraph.getJobID());

        final JobConfigInfo jobConfigInfoResponse =
                jobConfigHandler.handleRequest(handlerRequest, archivedExecutionGraph);

        final Map<String, String> filteredGlobalJobParameters =
                filterSecretValues(globalJobParameters);

        assertThat(jobConfigInfoResponse.getExecutionConfigInfo().getGlobalJobParameters())
                .isEqualTo(filteredGlobalJobParameters);
    }

    private Map<String, String> filterSecretValues(Map<String, String> globalJobParameters) {
        return ConfigurationUtils.hideSensitiveValues(globalJobParameters);
    }

    private HandlerRequest<EmptyRequestBody> createRequest(JobID jobId)
            throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new JobMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
