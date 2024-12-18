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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.util.ConfigurationException;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ActiveResourceManagerFactory}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class ActiveResourceManagerFactoryTest {

    private static final MemorySize TOTAL_FLINK_SIZE = MemorySize.ofMebiBytes(2 * 1024);
    private static final MemorySize TOTAL_PROCESS_SIZE = MemorySize.ofMebiBytes(3 * 1024);

    @Test
    void testGetEffectiveConfigurationForResourceManagerFineGrained() {
        final Configuration config = new Configuration();
        config.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_SIZE);
        config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, TOTAL_PROCESS_SIZE);

        final Configuration effectiveConfig =
                getFactory().getEffectiveConfigurationForResourceManager(config);

        assertThat(effectiveConfig.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY)).isFalse();
        assertThat(effectiveConfig.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY)).isFalse();
    }

    private static ActiveResourceManagerFactory<ResourceID> getFactory() {
        return new ActiveResourceManagerFactory<ResourceID>() {
            @Override
            protected ResourceManagerRuntimeServicesConfiguration
                    createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
                            throws ConfigurationException {
                return null;
            }

            @Override
            protected ResourceManagerDriver<ResourceID> createResourceManagerDriver(
                    Configuration configuration,
                    @Nullable String webInterfaceUrl,
                    String rpcAddress)
                    throws Exception {
                return null;
            }
        };
    }
}
