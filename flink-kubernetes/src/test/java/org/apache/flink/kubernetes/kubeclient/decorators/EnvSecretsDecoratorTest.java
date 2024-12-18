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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import io.fabric8.kubernetes.api.model.EnvVar;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the {@link EnvSecretsDecorator}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class EnvSecretsDecoratorTest extends KubernetesJobManagerTestBase {

    private static final String ENV_NAME = "MY_FOO";
    private static final String ENV_SERCET_KEY = "env:MY_FOO,secret:foo,key:key_foo";

    private EnvSecretsDecorator envSecretsDecorator;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_ENV_SECRET_KEY_REF.key(), ENV_SERCET_KEY);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();
        this.envSecretsDecorator = new EnvSecretsDecorator(kubernetesJobManagerParameters);
    }

    @Test
    void testWhetherPodOrContainerIsDecorated() {
        final FlinkPod resultFlinkPod = envSecretsDecorator.decorateFlinkPod(baseFlinkPod);
        List<EnvVar> envVarList = resultFlinkPod.getMainContainer().getEnv();

        assertThat(envVarList).extracting(EnvVar::getName).containsExactly(ENV_NAME);
    }
}
