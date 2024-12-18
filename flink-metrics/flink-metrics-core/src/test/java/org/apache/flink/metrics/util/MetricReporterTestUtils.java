/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.util;

import org.apache.flink.metrics.reporter.MetricReporterFactory;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for metric reporters. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class MetricReporterTestUtils {

    /**
     * Verifies that the given {@link MetricReporterFactory} class can be loaded by the {@link
     * ServiceLoader}.
     *
     * <p>Essentially, this verifies that the {@code
     * META-INF/services/org.apache.flink.metrics.reporter.MetricReporterFactory} file exists and
     * contains the expected factory class references.
     *
     * @param clazz class to load
     */
    public static void testMetricReporterSetupViaSPI(
            final Class<? extends MetricReporterFactory> clazz) {
        final Set<Class<? extends MetricReporterFactory>> loadedFactories =
                StreamSupport.stream(
                                ServiceLoader.load(MetricReporterFactory.class).spliterator(),
                                false)
                        .map(MetricReporterFactory::getClass)
                        .collect(Collectors.toSet());
        assertThat(loadedFactories).contains(clazz);
    }
}
