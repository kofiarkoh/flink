/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics;

import org.apache.flink.configuration.Configuration;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test utilities for the metric registries. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class MetricRegistryTestUtils {

    private static volatile MetricRegistryConfiguration defaultConfiguration;

    public static MetricRegistryConfiguration fromConfiguration(Configuration configuration) {
        return MetricRegistryConfiguration.fromConfiguration(configuration, 10485760);
    }

    public static MetricRegistryConfiguration defaultMetricRegistryConfiguration() {
        // create the default metric registry configuration only once
        if (defaultConfiguration == null) {
            synchronized (MetricRegistryConfiguration.class) {
                if (defaultConfiguration == null) {
                    defaultConfiguration = fromConfiguration(new Configuration());
                }
            }
        }

        return defaultConfiguration;
    }
}
