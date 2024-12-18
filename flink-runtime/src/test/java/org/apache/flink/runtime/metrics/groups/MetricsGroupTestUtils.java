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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Util @ExtendWith(CTestJUnit5Extension.class) @CTestClass class to create metric groups for SinkV2
 * tests.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class MetricsGroupTestUtils {

    public static InternalSinkWriterMetricGroup mockWriterMetricGroup() {
        return new InternalSinkWriterMetricGroup(
                new UnregisteredMetricsGroup(),
                UnregisteredMetricsGroup.createOperatorIOMetricGroup());
    }

    public static InternalSinkWriterMetricGroup mockWriterMetricGroup(MetricGroup metricGroup) {
        return new InternalSinkWriterMetricGroup(
                metricGroup, UnregisteredMetricsGroup.createOperatorIOMetricGroup());
    }

    public static InternalSinkWriterMetricGroup mockWriterMetricGroup(
            MetricGroup metricGroup, OperatorIOMetricGroup operatorIOMetricGroup) {
        return new InternalSinkWriterMetricGroup(metricGroup, operatorIOMetricGroup);
    }

    public static InternalSinkCommitterMetricGroup mockCommitterMetricGroup() {
        return new InternalSinkCommitterMetricGroup(
                new UnregisteredMetricsGroup(),
                UnregisteredMetricsGroup.createOperatorIOMetricGroup());
    }
}
