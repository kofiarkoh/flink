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

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;

/** A {@link TypeSerializerUpgradeTestBase} for {@link VoidNamespaceSerializer}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class VoidNamespaceSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<VoidNamespace, VoidNamespace> {

    private static final String SPEC_NAME = "void-namespace-serializer";

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        VoidNamespaceSerializerSetup.class,
                        VoidNamespaceSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "voidnamespace-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class VoidNamespaceSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<VoidNamespace> {
        @Override
        public TypeSerializer<VoidNamespace> createPriorSerializer() {
            return VoidNamespaceSerializer.INSTANCE;
        }

        @Override
        public VoidNamespace createTestData() {
            return VoidNamespace.INSTANCE;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class VoidNamespaceSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<VoidNamespace> {
        @Override
        public TypeSerializer<VoidNamespace> createUpgradedSerializer() {
            return VoidNamespaceSerializer.INSTANCE;
        }

        @Override
        public Condition<VoidNamespace> testDataCondition() {
            return new Condition<>(VoidNamespace.INSTANCE::equals, "is VoidNamespace.INSTANCE");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<VoidNamespace>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
