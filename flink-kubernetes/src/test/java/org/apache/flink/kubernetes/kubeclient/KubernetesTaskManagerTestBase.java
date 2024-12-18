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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Base test class for the TaskManager side. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class KubernetesTaskManagerTestBase extends KubernetesPodTestBase {

    protected static final int RPC_PORT = 12345;

    protected static final String POD_NAME = "taskmanager-pod-1";
    protected static final String DYNAMIC_PROPERTIES = "";
    protected static final String JVM_MEM_OPTS_ENV = "-Xmx512m";

    protected static final int TOTAL_PROCESS_MEMORY = 1184;
    protected static final double TASK_MANAGER_CPU = 2.0;
    protected static final double TASK_MANAGER_CPU_LIMIT_FACTOR = 1.5;
    protected static final double TASK_MANAGER_MEMORY_LIMIT_FACTOR = 2.0;
    protected static final String ENTRYPOINT_ARGS = "entrypoint args";

    protected TaskExecutorProcessSpec taskExecutorProcessSpec;

    protected ContaineredTaskManagerParameters containeredTaskManagerParameters;

    protected KubernetesTaskManagerParameters kubernetesTaskManagerParameters;

    protected static final Set<String> BLOCKED_NODES =
            new HashSet<>(Arrays.asList("blockedNode1", "blockedNode2"));

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        flinkConfig.set(TaskManagerOptions.RPC_PORT, String.valueOf(RPC_PORT));
        flinkConfig.set(TaskManagerOptions.CPU_CORES, TASK_MANAGER_CPU);
        flinkConfig.set(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                MemorySize.parse(TOTAL_PROCESS_MEMORY + "m"));
        customizedEnvs.forEach(
                (k, v) ->
                        flinkConfig.setString(
                                ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX + k,
                                v));
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_LABELS, userLabels);
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR, nodeSelector);
        flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR,
                TASK_MANAGER_CPU_LIMIT_FACTOR);
        flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_MEMORY_LIMIT_FACTOR,
                TASK_MANAGER_MEMORY_LIMIT_FACTOR);
        this.flinkConfig.set(
                KubernetesConfigOptions.KUBERNETES_TASKMANAGER_ENTRYPOINT_ARGS, ENTRYPOINT_ARGS);
    }

    @Override
    protected void onSetup() throws Exception {
        taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(flinkConfig);
        containeredTaskManagerParameters =
                ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);
        kubernetesTaskManagerParameters =
                new KubernetesTaskManagerParameters(
                        flinkConfig,
                        POD_NAME,
                        DYNAMIC_PROPERTIES,
                        JVM_MEM_OPTS_ENV,
                        containeredTaskManagerParameters,
                        ExternalResourceUtils.getExternalResourceConfigurationKeys(
                                flinkConfig,
                                KubernetesConfigOptions
                                        .EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX),
                        BLOCKED_NODES);
    }
}
