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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import io.fabric8.kubernetes.api.model.ContainerPort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link InitJobManagerDecorator} with pod template. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class InitJobManagerDecoratorWithPodTemplateTest extends DecoratorWithPodTemplateTestBase {

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();
        this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS, ANNOTATIONS);
        this.flinkConfig.setString(
                KubernetesConfigOptions.JOB_MANAGER_TOLERATIONS.key(), TOLERATION_STRING);
        this.customizedEnvs.forEach(
                (k, v) ->
                        this.flinkConfig.setString(
                                ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + k, v));
        this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_LABELS, userLabels);
        this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_NODE_SELECTOR, nodeSelector);
    }

    @Override
    public FlinkPod getResultPod(FlinkPod podTemplate) {
        final KubernetesJobManagerParameters kubernetesJobManagerParameters =
                new KubernetesJobManagerParameters(
                        flinkConfig,
                        new KubernetesClusterClientFactory().getClusterSpecification(flinkConfig));
        final InitJobManagerDecorator initJobManagerDecorator =
                new InitJobManagerDecorator(kubernetesJobManagerParameters);
        return initJobManagerDecorator.decorateFlinkPod(podTemplate);
    }

    @Test
    void testJobManagerManagerMainContainerPortsMerging() {
        final List<String> expectedContainerPorts = new ArrayList<>();
        expectedContainerPorts.add(Constants.REST_PORT_NAME);
        expectedContainerPorts.add(Constants.JOB_MANAGER_RPC_PORT_NAME);
        expectedContainerPorts.add(Constants.BLOB_SERVER_PORT_NAME);
        // Add port from pod template
        expectedContainerPorts.add("testing-port");
        assertThat(
                        this.resultPod.getMainContainer().getPorts().stream()
                                .map(ContainerPort::getName))
                .containsExactlyInAnyOrderElementsOf(expectedContainerPorts);
    }
}
