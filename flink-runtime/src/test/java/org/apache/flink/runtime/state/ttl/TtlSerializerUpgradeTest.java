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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/** State migration test for {@link TtlSerializer}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TtlSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<TtlValue<String>, TtlValue<String>> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "ttl-serializer",
                        flinkVersion,
                        TtlSerializerSetup.class,
                        TtlSerializerVerifier.class));

        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "ttl-serializer"
    // ----------------------------------------------------------------------------------------------

    public static final class TtlSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<TtlValue<String>> {

        @Override
        public TypeSerializer<TtlValue<String>> createPriorSerializer() {
            return new TtlSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
        }

        @Override
        public TtlValue<String> createTestData() {
            return new TtlValue<>("hello Gordon", 13);
        }
    }

    public static final class TtlSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<TtlValue<String>> {

        @Override
        public TypeSerializer<TtlValue<String>> createUpgradedSerializer() {
            return new TtlSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
        }

        @Override
        public Condition<TtlValue<String>> testDataCondition() {
            return new Condition<>(
                    ttlValue ->
                            Objects.equals(ttlValue.getUserValue(), "hello Gordon")
                                    && ttlValue.getLastAccessTimestamp() == 13L,
                    "ttlValue");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<TtlValue<String>>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
