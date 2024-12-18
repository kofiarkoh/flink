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

package org.apache.flink.runtime.state;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;

/** A {@link TypeSerializerUpgradeTestBase} for {@link ArrayListSerializerSnapshot}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class ArrayListSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<ArrayList<String>, ArrayList<String>> {

    private static final String SPEC_NAME = "arraylist-serializer";

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        ArrayListSerializerSetup.class,
                        ArrayListSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "arraylist-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ArrayListSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<ArrayList<String>> {
        @Override
        public TypeSerializer<ArrayList<String>> createPriorSerializer() {
            return new ArrayListSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public ArrayList<String> createTestData() {
            ArrayList<String> data = new ArrayList<>(2);
            data.add("Apache");
            data.add("Flink");
            return data;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ArrayListSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<ArrayList<String>> {
        @Override
        public TypeSerializer<ArrayList<String>> createUpgradedSerializer() {
            return new ArrayListSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public Condition<ArrayList<String>> testDataCondition() {
            ArrayList<String> data = new ArrayList<>(2);
            data.add("Apache");
            data.add("Flink");
            return new Condition<>(data::equals, "value is equal to " + data);
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<ArrayList<String>>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
