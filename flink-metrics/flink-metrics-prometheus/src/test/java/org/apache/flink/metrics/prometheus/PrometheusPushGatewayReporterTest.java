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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.metrics.MetricConfig;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PrometheusPushGatewayReporter}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PrometheusPushGatewayReporterTest {

    @Test
    void testParseGroupingKey() {
        Map<String, String> groupingKey =
                PrometheusPushGatewayReporterFactory.parseGroupingKey("k1=v1;k2=v2");
        assertThat(groupingKey).containsEntry("k1", "v1");
        assertThat(groupingKey).containsEntry("k2", "v2");
    }

    @Test
    void testParseIncompleteGroupingKey() {
        Map<String, String> groupingKey =
                PrometheusPushGatewayReporterFactory.parseGroupingKey("k1=");
        assertThat(groupingKey).isEmpty();

        groupingKey = PrometheusPushGatewayReporterFactory.parseGroupingKey("=v1");
        assertThat(groupingKey).isEmpty();

        groupingKey = PrometheusPushGatewayReporterFactory.parseGroupingKey("k1");
        assertThat(groupingKey).isEmpty();
    }

    @Test
    void testConnectToPushGatewayUsingHostUrl() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(HOST_URL.key(), "https://localhost:18080");
        String gatewayBaseURL = factory.createMetricReporter(metricConfig).hostUrl.toString();
        assertThat(gatewayBaseURL).isEqualTo("https://localhost:18080");
    }

    @Test
    void testConnectToPushGatewayThrowsExceptionWithoutHostInformation() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        assertThatThrownBy(() -> factory.createMetricReporter(metricConfig))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
