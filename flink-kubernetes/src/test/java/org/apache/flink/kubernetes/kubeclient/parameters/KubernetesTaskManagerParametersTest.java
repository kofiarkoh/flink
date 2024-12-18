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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** General tests for the {@link KubernetesTaskManagerParameters}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class KubernetesTaskManagerParametersTest extends KubernetesTestBase {

    private static final int TASK_MANAGER_MEMORY = 1024;
    private static final double TASK_MANAGER_CPU = 1.2;
    private static final double TASK_MANAGER_CPU_LIMIT_FACTOR = 2.0;
    private static final double TASK_MANAGER_MEMORY_LIMIT_FACTOR = 2.0;
    private static final int RPC_PORT = 13001;

    private static final String POD_NAME = "task-manager-pod-1";
    private static final String DYNAMIC_PROPERTIES = "-Dkey.b='b2'";
    private static final String JVM_MEM_OPTS_ENV = "-Xmx512m";

    private final Map<String, String> customizedEnvs =
            new HashMap<String, String>() {
                {
                    put("key1", "value1");
                    put("key2", "value2");
                }
            };

    private KubernetesTaskManagerParameters kubernetesTaskManagerParameters;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        flinkConfig.set(TaskManagerOptions.CPU_CORES, TASK_MANAGER_CPU);
        flinkConfig.set(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                MemorySize.parse(TASK_MANAGER_MEMORY + "m"));
        flinkConfig.set(TaskManagerOptions.RPC_PORT, String.valueOf(RPC_PORT));
        flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR,
                TASK_MANAGER_CPU_LIMIT_FACTOR);
        flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_MEMORY_LIMIT_FACTOR,
                TASK_MANAGER_MEMORY_LIMIT_FACTOR);

        customizedEnvs.forEach(
                (k, v) ->
                        flinkConfig.setString(
                                ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX + k,
                                v));
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(flinkConfig);
        final ContaineredTaskManagerParameters containeredTaskManagerParameters =
                ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

        this.kubernetesTaskManagerParameters =
                new KubernetesTaskManagerParameters(
                        flinkConfig,
                        POD_NAME,
                        DYNAMIC_PROPERTIES,
                        JVM_MEM_OPTS_ENV,
                        containeredTaskManagerParameters,
                        Collections.emptyMap(),
                        Collections.emptySet());
    }

    @Test
    void testGetEnvironments() {
        assertThat(kubernetesTaskManagerParameters.getEnvironments()).isEqualTo(customizedEnvs);
    }

    @Test
    void testGetEmptyAnnotations() {
        assertThat(kubernetesTaskManagerParameters.getAnnotations()).isEmpty();
    }

    @Test
    void testGetAnnotations() {
        final Map<String, String> expectedAnnotations = new HashMap<>();
        expectedAnnotations.put("a1", "v1");
        expectedAnnotations.put("a2", "v2");

        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS, expectedAnnotations);

        final Map<String, String> resultAnnotations =
                kubernetesTaskManagerParameters.getAnnotations();

        assertThat(resultAnnotations).isEqualTo(expectedAnnotations);
    }

    @Test
    void testGetPodName() {
        assertThat(kubernetesTaskManagerParameters.getPodName()).isEqualTo(POD_NAME);
    }

    @Test
    void testGetTaskManagerMemoryMB() {
        assertThat(kubernetesTaskManagerParameters.getTaskManagerMemoryMB())
                .isEqualTo(TASK_MANAGER_MEMORY);
    }

    @Test
    void testGetTaskManagerCPU() {
        assertThat(kubernetesTaskManagerParameters.getTaskManagerCPU())
                .isEqualTo(TASK_MANAGER_CPU, within(0.00001));
    }

    @Test
    void testGetTaskManagerCPULimitFactor() {
        assertThat(kubernetesTaskManagerParameters.getTaskManagerCPULimitFactor())
                .isEqualTo(TASK_MANAGER_CPU_LIMIT_FACTOR, within(0.00001));
    }

    @Test
    void testGetTaskManagerMemoryLimitFactor() {
        assertThat(kubernetesTaskManagerParameters.getTaskManagerMemoryLimitFactor())
                .isEqualTo(TASK_MANAGER_MEMORY_LIMIT_FACTOR, within(0.00001));
    }

    @Test
    void testGetRpcPort() {
        assertThat(kubernetesTaskManagerParameters.getRPCPort()).isEqualTo(RPC_PORT);
    }

    @Test
    void testGetDynamicProperties() {
        assertThat(kubernetesTaskManagerParameters.getDynamicProperties())
                .isEqualTo(DYNAMIC_PROPERTIES);
    }

    @Test
    void testGetJvmMemOptsEnv() {
        assertThat(kubernetesTaskManagerParameters.getJvmMemOptsEnv()).isEqualTo(JVM_MEM_OPTS_ENV);
    }

    @Test
    void testPrioritizeBuiltInLabels() {
        final Map<String, String> userLabels = new HashMap<>();
        userLabels.put(Constants.LABEL_TYPE_KEY, "user-label-type");
        userLabels.put(Constants.LABEL_APP_KEY, "user-label-app");
        userLabels.put(Constants.LABEL_COMPONENT_KEY, "user-label-component-tm");

        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_LABELS, userLabels);

        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
        assertThat(kubernetesTaskManagerParameters.getLabels()).isEqualTo(expectedLabels);
    }

    @Test
    void testGetServiceAccount() {
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_SERVICE_ACCOUNT, "flink");
        assertThat(kubernetesTaskManagerParameters.getServiceAccount()).isEqualTo("flink");
    }

    @Test
    void testGetServiceAccountFallback() {
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, "flink-fallback");
        assertThat(kubernetesTaskManagerParameters.getServiceAccount()).isEqualTo("flink-fallback");
    }

    @Test
    void testGetServiceAccountShouldReturnDefaultIfNotExplicitlySet() {
        assertThat(kubernetesTaskManagerParameters.getServiceAccount()).isEqualTo("default");
    }
}
