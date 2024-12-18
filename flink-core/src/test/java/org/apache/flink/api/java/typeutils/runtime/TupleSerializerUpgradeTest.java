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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;

/** {@link TupleSerializer} upgrade test. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TupleSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<
                Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "tuple-serializer",
                        flinkVersion,
                        TupleSerializerSetup.class,
                        TupleSerializerVerifier.class));

        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "tuple-serializer"
    // ----------------------------------------------------------------------------------------------

    public static final class TupleSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<
                    Tuple3<String, String, Integer>> {

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public TypeSerializer<Tuple3<String, String, Integer>> createPriorSerializer() {
            return new TupleSerializer(
                    Tuple3.class,
                    new TypeSerializer[] {
                        StringSerializer.INSTANCE, StringSerializer.INSTANCE, IntSerializer.INSTANCE
                    });
        }

        @Override
        public Tuple3<String, String, Integer> createTestData() {
            return new Tuple3<>("hello Gordon", "ciao", 14);
        }
    }

    public static final class TupleSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<
                    Tuple3<String, String, Integer>> {

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public TypeSerializer<Tuple3<String, String, Integer>> createUpgradedSerializer() {
            return new TupleSerializer(
                    Tuple3.class,
                    new TypeSerializer[] {
                        StringSerializer.INSTANCE, StringSerializer.INSTANCE, IntSerializer.INSTANCE
                    });
        }

        @Override
        public Condition<Tuple3<String, String, Integer>> testDataCondition() {
            Tuple3<String, String, Integer> value = new Tuple3<>("hello Gordon", "ciao", 14);
            return new Condition<>(value::equals, "value is (hello Gordon, ciao, 14)");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Tuple3<String, String, Integer>>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
