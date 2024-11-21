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

package org.apache.flink.api.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ParameterizedTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class ExecutionConfigFromConfigurationTest {

    @Parameters(name = "{0}")
    private static Collection<TestSpec> specs() {
        return Arrays.asList(
                TestSpec.testValue(false)
                        .whenSetFromFile("pipeline.auto-generate-uids", "false")
                        .viaSetter(
                                booleanSetter(
                                        ExecutionConfig::enableAutoGeneratedUIDs,
                                        ExecutionConfig::disableAutoGeneratedUIDs))
                        .getterVia(ExecutionConfig::hasAutoGeneratedUIDsEnabled)
                        .nonDefaultValue(false),
                TestSpec.testValue(120000L)
                        .whenSetFromFile("pipeline.auto-watermark-interval", "2 min")
                        .viaSetter(ExecutionConfig::setAutoWatermarkInterval)
                        .getterVia(ExecutionConfig::getAutoWatermarkInterval)
                        .nonDefaultValue(123L),
                TestSpec.testValue(ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL)
                        .whenSetFromFile("pipeline.closure-cleaner-level", "TOP_LEVEL")
                        .viaSetter(ExecutionConfig::setClosureCleanerLevel)
                        .getterVia(ExecutionConfig::getClosureCleanerLevel)
                        .nonDefaultValue(ExecutionConfig.ClosureCleanerLevel.NONE),
                TestSpec.testValue(true)
                        .whenSetFromFile("pipeline.force-avro", "true")
                        .viaSetter(
                                booleanSetter(
                                        (ec) -> ec.getSerializerConfig().setForceAvro(true),
                                        (ec) -> ec.getSerializerConfig().setForceAvro(false)))
                        .getterVia((ec) -> ec.getSerializerConfig().isForceAvroEnabled())
                        .nonDefaultValue(true),
                TestSpec.testValue(false)
                        .whenSetFromFile("pipeline.force-kryo", "false")
                        .viaSetter(
                                booleanSetter(
                                        (ec) -> ec.getSerializerConfig().setForceKryo(true),
                                        (ec) -> ec.getSerializerConfig().setForceKryo(false)))
                        .getterVia((ec) -> ec.getSerializerConfig().isForceKryoEnabled())
                        .nonDefaultValue(false),
                TestSpec.testValue(false)
                        .whenSetFromFile("pipeline.generic-types", "false")
                        .viaSetter(
                                booleanSetter(
                                        (ec) -> ec.getSerializerConfig().setGenericTypes(true),
                                        (ec) -> ec.getSerializerConfig().setGenericTypes(false)))
                        .getterVia(
                                execConfig ->
                                        !execConfig.getSerializerConfig().hasGenericTypesDisabled())
                        .nonDefaultValue(false),
                TestSpec.testValue(getTestGlobalJobParameters())
                        .whenSetFromFile(
                                "pipeline.global-job-parameters", "key1:value1,key2:value2")
                        .viaSetter(ExecutionConfig::setGlobalJobParameters)
                        .getterVia(ExecutionConfig::getGlobalJobParameters)
                        .nonDefaultValue(getOtherTestGlobalJobParameters()),
                TestSpec.testValue(64)
                        .whenSetFromFile("pipeline.max-parallelism", "64")
                        .viaSetter(ExecutionConfig::setMaxParallelism)
                        .getterVia(ExecutionConfig::getMaxParallelism)
                        .nonDefaultValue(13),
                TestSpec.testValue(true)
                        .whenSetFromFile("pipeline.object-reuse", "true")
                        .viaSetter(
                                booleanSetter(
                                        ExecutionConfig::enableObjectReuse,
                                        ExecutionConfig::disableObjectReuse))
                        .getterVia(ExecutionConfig::isObjectReuseEnabled)
                        .nonDefaultValue(true),
                TestSpec.testValue(true)
                        .whenSetFromFile("execution.checkpointing.snapshot-compression", "true")
                        .viaSetter(ExecutionConfig::setUseSnapshotCompression)
                        .getterVia(ExecutionConfig::isUseSnapshotCompression)
                        .nonDefaultValue(true),
                TestSpec.testValue(12)
                        .whenSetFromFile("parallelism.default", "12")
                        .viaSetter(ExecutionConfig::setParallelism)
                        .getterVia(ExecutionConfig::getParallelism)
                        .nonDefaultValue(21),
                TestSpec.testValue(12000L)
                        .whenSetFromFile("task.cancellation.interval", "12000")
                        .viaSetter(ExecutionConfig::setTaskCancellationInterval)
                        .getterVia(ExecutionConfig::getTaskCancellationInterval)
                        .nonDefaultValue(21L),
                TestSpec.testValue(12100L)
                        .whenSetFromFile("task.cancellation.timeout", "12100")
                        .viaSetter(ExecutionConfig::setTaskCancellationTimeout)
                        .getterVia(ExecutionConfig::getTaskCancellationTimeout)
                        .nonDefaultValue(21L),
                TestSpec.testValue(12300L)
                        .whenSetFromFile("metrics.latency.interval", "12300")
                        .viaSetter(ExecutionConfig::setLatencyTrackingInterval)
                        .getterVia(ExecutionConfig::getLatencyTrackingInterval)
                        .nonDefaultValue(21L));
    }

    @Parameter private TestSpec spec;

    @TestTemplate
    void testLoadingFromConfiguration() {
        ExecutionConfig configFromSetters = new ExecutionConfig();
        ExecutionConfig configFromFile = new ExecutionConfig();

        Configuration configuration = new Configuration();
        configuration.setString(spec.key, spec.value);
        configFromFile.configure(configuration, ExecutionConfigTest.class.getClassLoader());

        spec.setValue(configFromSetters);
        spec.assertEqual(configFromFile, configFromSetters);
    }

    @TestTemplate
    void testNotOverridingIfNotSet() {
        ExecutionConfig executionConfig = new ExecutionConfig();

        spec.setNonDefaultValue(executionConfig);
        Configuration configuration = new Configuration();
        executionConfig.configure(configuration, ExecutionConfigTest.class.getClassLoader());

        spec.assertEqualNonDefault(executionConfig);
    }

    private static ExecutionConfig.GlobalJobParameters getTestGlobalJobParameters() {
        Configuration globalJobParameters = new Configuration();
        globalJobParameters.setString("key1", "value1");
        globalJobParameters.setString("key2", "value2");
        return globalJobParameters;
    }

    private static ExecutionConfig.GlobalJobParameters getOtherTestGlobalJobParameters() {
        Configuration globalJobParameters = new Configuration();
        globalJobParameters.setString("key1", "value1");
        globalJobParameters.setString("key2", "value2");
        globalJobParameters.setString("key3", "value3");
        return globalJobParameters;
    }

    private static class TestSpec<T> {
        private String key;
        private String value;
        private final T objectValue;
        private T nonDefaultValue;
        private BiConsumer<ExecutionConfig, T> setter;
        private Function<ExecutionConfig, T> getter;

        private TestSpec(T value) {
            this.objectValue = value;
        }

        public static <T> TestSpec<T> testValue(T value) {
            return new TestSpec<>(value);
        }

        public TestSpec<T> whenSetFromFile(String key, String value) {
            this.key = key;
            this.value = value;
            return this;
        }

        public TestSpec<T> viaSetter(BiConsumer<ExecutionConfig, T> setter) {
            this.setter = setter;
            return this;
        }

        public TestSpec<T> getterVia(Function<ExecutionConfig, T> getter) {
            this.getter = getter;
            return this;
        }

        public TestSpec<T> nonDefaultValue(T nonDefaultValue) {
            this.nonDefaultValue = nonDefaultValue;
            return this;
        }

        public void setValue(ExecutionConfig config) {
            setter.accept(config, objectValue);
        }

        public void setNonDefaultValue(ExecutionConfig config) {
            setter.accept(config, nonDefaultValue);
        }

        public void assertEqual(ExecutionConfig configFromFile, ExecutionConfig configFromSetters) {
            assertThat(getter.apply(configFromSetters)).isEqualTo(getter.apply(configFromFile));
        }

        public void assertEqualNonDefault(ExecutionConfig configFromFile) {
            assertThat(getter.apply(configFromFile)).isEqualTo(nonDefaultValue);
        }

        @Override
        public String toString() {
            return "key='" + key + '\'';
        }
    }

    private static BiConsumer<ExecutionConfig, Boolean> booleanSetter(
            Consumer<ExecutionConfig> enable, Consumer<ExecutionConfig> disable) {
        return (execConfig, value) -> {
            if (value) {
                enable.accept(execConfig);
            } else {
                disable.accept(execConfig);
            }
        };
    }
}
