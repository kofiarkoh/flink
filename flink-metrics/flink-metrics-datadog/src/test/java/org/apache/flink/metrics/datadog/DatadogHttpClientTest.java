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

package org.apache.flink.metrics.datadog;

import org.apache.flink.metrics.util.TestCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the DatadogHttpClient. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class DatadogHttpClientTest {

    private static final List<String> tags = Arrays.asList("tag1", "tag2");
    private static final String TAGS_AS_JSON =
            tags.stream().collect(Collectors.joining("\",\"", "[\"", "\"]"));
    private static final String HOST = "localhost";
    private static final String METRIC = "testMetric";

    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    }

    private static final long MOCKED_SYSTEM_MILLIS = 123L;

    @Test
    void testClientWithEmptyKey() {
        assertThatThrownBy(() -> new DatadogHttpClient("", null, 123, DataCenter.US, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testClientWithNullKey() {
        assertThatThrownBy(() -> new DatadogHttpClient(null, null, 123, DataCenter.US, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetProxyWithNullProxyHost() {
        DatadogHttpClient client =
                new DatadogHttpClient("anApiKey", null, 123, DataCenter.US, false);
        assert (client.getProxy() == Proxy.NO_PROXY);
    }

    @Test
    void testGetProxy() {
        DatadogHttpClient client =
                new DatadogHttpClient("anApiKey", "localhost", 123, DataCenter.US, false);

        assertThat(client.getProxy().address()).isInstanceOf(InetSocketAddress.class);

        InetSocketAddress proxyAddress = (InetSocketAddress) client.getProxy().address();

        assertThat(proxyAddress.getPort()).isEqualTo(123);
        assertThat(proxyAddress.getHostString()).isEqualTo("localhost");
    }

    @Test
    void serializeGauge() throws JsonProcessingException {
        DSeries series = new DSeries();
        series.add(new DGauge(() -> 1, METRIC, HOST, tags, () -> MOCKED_SYSTEM_MILLIS));

        assertSerialization(
                DatadogHttpClient.serialize(series),
                new MetricAssertion(MetricType.gauge, true, "1"));
    }

    @Test
    void serializeGaugeWithoutHost() throws JsonProcessingException {
        DSeries series = new DSeries();
        series.add(new DGauge(() -> 1, METRIC, null, tags, () -> MOCKED_SYSTEM_MILLIS));

        assertSerialization(
                DatadogHttpClient.serialize(series),
                new MetricAssertion(MetricType.gauge, false, "1"));
    }

    @Test
    void serializeCounter() throws JsonProcessingException {
        DSeries series = new DSeries();
        series.add(
                new DCounter(new TestCounter(1), METRIC, HOST, tags, () -> MOCKED_SYSTEM_MILLIS));

        assertSerialization(
                DatadogHttpClient.serialize(series),
                new MetricAssertion(MetricType.count, true, "1"));
    }

    @Test
    void serializeCounterWithoutHost() throws JsonProcessingException {
        DSeries series = new DSeries();
        series.add(
                new DCounter(new TestCounter(1), METRIC, null, tags, () -> MOCKED_SYSTEM_MILLIS));

        assertSerialization(
                DatadogHttpClient.serialize(series),
                new MetricAssertion(MetricType.count, false, "1"));
    }

    @Test
    void serializeMeter() throws JsonProcessingException {
        DSeries series = new DSeries();
        series.add(new DMeter(new TestMeter(0, 1), METRIC, HOST, tags, () -> MOCKED_SYSTEM_MILLIS));

        assertSerialization(
                DatadogHttpClient.serialize(series),
                new MetricAssertion(MetricType.gauge, true, "1.0"));
    }

    @Test
    void serializeMeterWithoutHost() throws JsonProcessingException {
        DSeries series = new DSeries();
        series.add(new DMeter(new TestMeter(0, 1), METRIC, null, tags, () -> MOCKED_SYSTEM_MILLIS));

        assertSerialization(
                DatadogHttpClient.serialize(series),
                new MetricAssertion(MetricType.gauge, false, "1.0"));
    }

    @Test
    void serializeHistogram() throws JsonProcessingException {
        DHistogram h =
                new DHistogram(new TestHistogram(), METRIC, HOST, tags, () -> MOCKED_SYSTEM_MILLIS);

        DSeries series = new DSeries();
        h.addTo(series);

        assertSerialization(
                DatadogHttpClient.serialize(series),
                new MetricAssertion(MetricType.gauge, true, "4.0", DHistogram.SUFFIX_AVG),
                new MetricAssertion(MetricType.gauge, true, "1", DHistogram.SUFFIX_COUNT),
                new MetricAssertion(MetricType.gauge, true, "0.5", DHistogram.SUFFIX_MEDIAN),
                new MetricAssertion(
                        MetricType.gauge, true, "0.95", DHistogram.SUFFIX_95_PERCENTILE),
                new MetricAssertion(MetricType.gauge, true, "7", DHistogram.SUFFIX_MIN),
                new MetricAssertion(MetricType.gauge, true, "6", DHistogram.SUFFIX_MAX));
    }

    private static void assertSerialization(String json, MetricAssertion... metricAssertions)
            throws JsonProcessingException {
        final JsonNode series = MAPPER.readTree(json).get(DSeries.FIELD_NAME_SERIES);

        for (int i = 0; i < metricAssertions.length; i++) {
            final JsonNode parsedJson = series.get(i);
            final MetricAssertion metricAssertion = metricAssertions[i];

            if (metricAssertion.expectHost) {
                assertThat(parsedJson.get(DMetric.FIELD_NAME_HOST).asText()).isEqualTo(HOST);
            } else {
                assertThat(parsedJson.get(DMetric.FIELD_NAME_HOST)).isNull();
            }
            assertThat(parsedJson.get(DMetric.FIELD_NAME_METRIC).asText())
                    .isEqualTo(METRIC + metricAssertion.metricNameSuffix);
            assertThat(parsedJson.get(DMetric.FIELD_NAME_TYPE).asText())
                    .isEqualTo(metricAssertion.expectedType.name());
            assertThat(parsedJson.get(DMetric.FIELD_NAME_POINTS).toString())
                    .isEqualTo(String.format("[[123,%s]]", metricAssertion.expectedValue));
            assertThat(parsedJson.get(DMetric.FIELD_NAME_TAGS).toString()).isEqualTo(TAGS_AS_JSON);
        }
    }

    private static final class MetricAssertion {
        final MetricType expectedType;
        final boolean expectHost;
        final String expectedValue;
        final String metricNameSuffix;

        private MetricAssertion(MetricType expectedType, boolean expectHost, String expectedValue) {
            this(expectedType, expectHost, expectedValue, "");
        }

        private MetricAssertion(
                MetricType expectedType,
                boolean expectHost,
                String expectedValue,
                String metricNameSuffix) {
            this.expectedType = expectedType;
            this.expectHost = expectHost;
            this.expectedValue = expectedValue;
            this.metricNameSuffix = metricNameSuffix;
        }
    }
}
