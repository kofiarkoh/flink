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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.rest.util.NoOpExecutionGraphCache;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests for {@link JobVertexWatermarksHandler}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class JobVertexWatermarksHandlerTest {

    private static final JobID TEST_JOB_ID = new JobID();

    private static final JobVertexID TEST_VERTEX_ID = new JobVertexID();

    private MetricFetcher metricFetcher;
    private MetricStore.TaskMetricStore taskMetricStore;
    private JobVertexWatermarksHandler watermarkHandler;
    private HandlerRequest<EmptyRequestBody> request;
    private AccessExecutionJobVertex vertex;

    @BeforeEach
    void before() throws Exception {
        taskMetricStore = Mockito.mock(MetricStore.TaskMetricStore.class);

        MetricStore metricStore = Mockito.mock(MetricStore.class);
        Mockito.when(
                        metricStore.getTaskMetricStore(
                                TEST_JOB_ID.toString(), TEST_VERTEX_ID.toString()))
                .thenReturn(taskMetricStore);

        metricFetcher = Mockito.mock(MetricFetcher.class);
        Mockito.when(metricFetcher.getMetricStore()).thenReturn(metricStore);

        watermarkHandler =
                new JobVertexWatermarksHandler(
                        Mockito.mock(LeaderGatewayRetriever.class),
                        Duration.ofSeconds(1),
                        Collections.emptyMap(),
                        metricFetcher,
                        NoOpExecutionGraphCache.INSTANCE,
                        Mockito.mock(Executor.class));

        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, TEST_JOB_ID.toString());
        pathParameters.put(JobVertexIdPathParameter.KEY, TEST_VERTEX_ID.toString());

        request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        new JobVertexMessageParameters(),
                        pathParameters,
                        Collections.emptyMap(),
                        Collections.emptyList());

        vertex = Mockito.mock(AccessExecutionJobVertex.class);
        Mockito.when(vertex.getJobVertexId()).thenReturn(TEST_VERTEX_ID);

        AccessExecutionVertex firstTask = Mockito.mock(AccessExecutionVertex.class);
        AccessExecutionVertex secondTask = Mockito.mock(AccessExecutionVertex.class);
        Mockito.when(firstTask.getParallelSubtaskIndex()).thenReturn(0);
        Mockito.when(secondTask.getParallelSubtaskIndex()).thenReturn(1);

        AccessExecutionVertex[] accessExecutionVertices = {firstTask, secondTask};
        Mockito.when(vertex.getTaskVertices()).thenReturn(accessExecutionVertices);
    }

    @AfterEach
    void after() {
        Mockito.verify(metricFetcher).update();
    }

    @Test
    void testWatermarksRetrieval() throws Exception {
        Mockito.when(taskMetricStore.getMetric("0.currentInputWatermark")).thenReturn("23");
        Mockito.when(taskMetricStore.getMetric("1.currentInputWatermark")).thenReturn("42");

        MetricCollectionResponseBody response = watermarkHandler.handleRequest(request, vertex);

        assertThat(response.getMetrics())
                .satisfies(
                        matching(
                                containsInAnyOrder(
                                        new MetricMatcher("0.currentInputWatermark", "23"),
                                        new MetricMatcher("1.currentInputWatermark", "42"))));
    }

    @Test
    void testPartialWatermarksAvailable() throws Exception {
        Mockito.when(taskMetricStore.getMetric("0.currentInputWatermark")).thenReturn("23");
        Mockito.when(taskMetricStore.getMetric("1.currentInputWatermark")).thenReturn(null);

        MetricCollectionResponseBody response = watermarkHandler.handleRequest(request, vertex);

        assertThat(response.getMetrics())
                .satisfies(matching(contains(new MetricMatcher("0.currentInputWatermark", "23"))));
    }

    @Test
    void testNoWatermarksAvailable() throws Exception {
        Mockito.when(taskMetricStore.getMetric("0.currentInputWatermark")).thenReturn(null);
        Mockito.when(taskMetricStore.getMetric("1.currentInputWatermark")).thenReturn(null);

        MetricCollectionResponseBody response = watermarkHandler.handleRequest(request, vertex);

        assertThat(response.getMetrics()).isEmpty();
    }

    private static class MetricMatcher extends BaseMatcher<Metric> {

        private String id;
        @Nullable private String value;

        MetricMatcher(String id, @Nullable String value) {
            this.id = id;
            this.value = value;
        }

        @Override
        public boolean matches(Object o) {
            if (!(o instanceof Metric)) {
                return false;
            }
            Metric actual = (Metric) o;
            return actual.getId().equals(id) && Objects.equals(value, actual.getValue());
        }

        @Override
        public void describeTo(Description description) {
            description.appendValue(new Metric(id, value));
        }
    }
}
