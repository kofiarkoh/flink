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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/** A {@link TypeSerializerUpgradeTestBase} for {@link MapSerializerSnapshot}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class MapSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<Map<Integer, String>, Map<Integer, String>> {

    private static final String SPEC_NAME = "map-serializer";

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        MapSerializerSetup.class,
                        MapSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "map-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class MapSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Map<Integer, String>> {
        @Override
        public TypeSerializer<Map<Integer, String>> createPriorSerializer() {
            return new MapSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        }

        @Override
        public Map<Integer, String> createTestData() {
            Map<Integer, String> data = new HashMap<>(3);
            for (int i = 0; i < 3; ++i) {
                data.put(i, String.valueOf(i));
            }
            return data;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class MapSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Map<Integer, String>> {
        @Override
        public TypeSerializer<Map<Integer, String>> createUpgradedSerializer() {
            return new MapSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        }

        @Override
        public Condition<Map<Integer, String>> testDataCondition() {
            Map<Integer, String> data = new HashMap<>(3);
            for (int i = 0; i < 3; ++i) {
                data.put(i, String.valueOf(i));
            }
            return new Condition<>(data::equals, "is equal to " + data);
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Map<Integer, String>>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
