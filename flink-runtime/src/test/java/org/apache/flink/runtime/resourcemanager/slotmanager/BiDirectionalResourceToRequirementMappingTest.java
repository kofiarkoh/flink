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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link BiDirectionalResourceToRequirementMapping}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class BiDirectionalResourceToRequirementMappingTest {

    @Test
    void testIncrement() {
        BiDirectionalResourceToRequirementMapping mapping =
                new BiDirectionalResourceToRequirementMapping();

        ResourceProfile requirement = ResourceProfile.UNKNOWN;
        ResourceProfile resource = ResourceProfile.ANY;

        mapping.incrementCount(requirement, resource, 1);

        assertThat(mapping.getRequirementsFulfilledBy(resource))
                .isEqualTo(ResourceCounter.withResource(requirement, 1));
        assertThat(mapping.getResourcesFulfilling(requirement))
                .isEqualTo(ResourceCounter.withResource(resource, 1));

        assertThat(mapping.getAllRequirementProfiles()).contains(requirement);
        assertThat(mapping.getAllResourceProfiles()).contains(resource);
    }

    @Test
    void testDecrement() {
        BiDirectionalResourceToRequirementMapping mapping =
                new BiDirectionalResourceToRequirementMapping();

        ResourceProfile requirement = ResourceProfile.UNKNOWN;
        ResourceProfile resource = ResourceProfile.ANY;

        mapping.incrementCount(requirement, resource, 1);
        mapping.decrementCount(requirement, resource, 1);

        assertThat(mapping.getRequirementsFulfilledBy(resource).isEmpty()).isTrue();
        assertThat(mapping.getResourcesFulfilling(requirement).isEmpty()).isTrue();

        assertThat(mapping.getAllRequirementProfiles()).isEmpty();
        assertThat(mapping.getAllResourceProfiles()).isEmpty();

        assertThat(mapping.isEmpty()).isTrue();
    }
}
