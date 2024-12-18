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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;

import static org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector.LEADER_ANNOTATION_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KubernetesLeaderElector}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class KubernetesLeaderElectorTest extends KubernetesTestBase {

    private String lockIdentity;
    private KubernetesConfigMap leaderConfigMap;

    private static final String CONFIGMAP_NAME = "test-config-map";

    public void onSetup() {
        lockIdentity = UUID.randomUUID().toString();
        leaderConfigMap = new TestingFlinkKubeClient.MockKubernetesConfigMap(CONFIGMAP_NAME);
    }

    @Test
    void testNoAnnotation() {
        assertThat(KubernetesLeaderElector.hasLeadership(leaderConfigMap, lockIdentity)).isFalse();
    }

    @Test
    void testAnnotationNotMatch() {
        leaderConfigMap.getAnnotations().put(LEADER_ANNOTATION_KEY, "wrong lock");
        assertThat(KubernetesLeaderElector.hasLeadership(leaderConfigMap, lockIdentity)).isFalse();
    }

    @Test
    void testAnnotationMatched() {
        leaderConfigMap
                .getAnnotations()
                .put(LEADER_ANNOTATION_KEY, "other information " + lockIdentity);
        assertThat(KubernetesLeaderElector.hasLeadership(leaderConfigMap, lockIdentity)).isTrue();
    }
}
