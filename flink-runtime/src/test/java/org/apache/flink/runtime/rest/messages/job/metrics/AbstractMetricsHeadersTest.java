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

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AbstractMetricsHeaders}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class AbstractMetricsHeadersTest {

    private AbstractMetricsHeaders<EmptyMessageParameters> metricsHandlerHeaders;

    @BeforeEach
    void setUp() throws Exception {
        metricsHandlerHeaders =
                new AbstractMetricsHeaders<EmptyMessageParameters>() {
                    @Override
                    public EmptyMessageParameters getUnresolvedMessageParameters() {
                        return EmptyMessageParameters.getInstance();
                    }

                    @Override
                    public String getTargetRestEndpointURL() {
                        return "/";
                    }

                    @Override
                    public String getDescription() {
                        return "";
                    }
                };
    }

    @Test
    void testHttpMethod() {
        assertThat(metricsHandlerHeaders.getHttpMethod()).isEqualTo(HttpMethodWrapper.GET);
    }

    @Test
    void testResponseStatus() {
        assertThat(metricsHandlerHeaders.getResponseStatusCode()).isEqualTo(HttpResponseStatus.OK);
    }

    @Test
    void testRequestClass() {
        assertThat(metricsHandlerHeaders.getRequestClass()).isEqualTo(EmptyRequestBody.class);
    }

    @Test
    void testResponseClass() {
        assertThat(metricsHandlerHeaders.getResponseClass())
                .isEqualTo(MetricCollectionResponseBody.class);
    }
}
