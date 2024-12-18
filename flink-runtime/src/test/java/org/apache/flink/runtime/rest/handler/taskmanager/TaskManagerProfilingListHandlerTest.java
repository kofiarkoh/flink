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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.ProfilingInfoList;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerProfilingListHeaders;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link TaskManagerProfilingListHandler}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TaskManagerProfilingListHandlerTest {

    private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();
    private TestingResourceManagerGateway resourceManagerGateway;
    private TaskManagerProfilingListHandler taskManagerProfilingListHandler;
    private HandlerRequest<EmptyRequestBody> handlerRequest;

    @BeforeEach
    void setUp() throws HandlerRequestException {
        resourceManagerGateway = new TestingResourceManagerGateway();
        taskManagerProfilingListHandler =
                new TaskManagerProfilingListHandler(
                        () -> CompletableFuture.completedFuture(null),
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        TaskManagerProfilingListHeaders.getInstance(),
                        () -> CompletableFuture.completedFuture(resourceManagerGateway));
        handlerRequest = createRequest(EXPECTED_TASK_MANAGER_ID);
    }

    @Test
    void testGetTaskManagerProfilingList() throws Exception {
        ProfilingInfoList profilingInfoList =
                new ProfilingInfoList(
                        Arrays.asList(
                                ProfilingInfo.create(30, ProfilingInfo.ProfilingMode.ITIMER),
                                ProfilingInfo.create(10, ProfilingInfo.ProfilingMode.CPU),
                                ProfilingInfo.create(15, ProfilingInfo.ProfilingMode.ALLOC)));
        resourceManagerGateway.setRequestProfilingListFunction(
                EXPECTED_TASK_MANAGER_ID ->
                        CompletableFuture.completedFuture(profilingInfoList.getProfilingInfos()));
        ProfilingInfoList profilingInfoListResp =
                taskManagerProfilingListHandler
                        .handleRequest(handlerRequest, resourceManagerGateway)
                        .get();
        assertThat(profilingInfoListResp.getProfilingInfos())
                .containsExactlyInAnyOrderElementsOf(profilingInfoList.getProfilingInfos());
    }

    @Test
    void testGetTaskManagerProfilingListForUnknownTaskExecutorException() throws Exception {
        resourceManagerGateway.setRequestProfilingListFunction(
                EXPECTED_TASK_MANAGER_ID ->
                        FutureUtils.completedExceptionally(
                                new UnknownTaskExecutorException(EXPECTED_TASK_MANAGER_ID)));
        try {
            taskManagerProfilingListHandler
                    .handleRequest(handlerRequest, resourceManagerGateway)
                    .get();
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            assertThat(cause).isInstanceOf(RestHandlerException.class);

            final RestHandlerException restHandlerException = (RestHandlerException) cause;
            assertThat(restHandlerException.getHttpResponseStatus())
                    .isEqualTo(HttpResponseStatus.NOT_FOUND);
            assertThat(restHandlerException.getMessage())
                    .contains("Could not find TaskExecutor " + EXPECTED_TASK_MANAGER_ID);
        }
    }

    private static HandlerRequest<EmptyRequestBody> createRequest(ResourceID taskManagerId)
            throws HandlerRequestException {
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(TaskManagerIdPathParameter.KEY, taskManagerId.toString());
        Map<String, List<String>> queryParameters = Collections.emptyMap();

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new TaskManagerMessageParameters(),
                pathParameters,
                queryParameters,
                Collections.emptyList());
    }
}
