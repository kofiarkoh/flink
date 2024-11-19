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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TypeSerializerSnapshot} */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TypeSerializerSnapshotTest {

    @Test
    void testNestedSchemaCompatibility() {
        TypeSerializerSnapshot<Integer> innerSnapshot =
                new NotCompletedTypeSerializerSnapshot() {
                    @Override
                    public TypeSerializerSchemaCompatibility<Integer> resolveSchemaCompatibility(
                            TypeSerializerSnapshot<Integer> oldSerializerSnapshot) {
                        return TypeSerializerSchemaCompatibility.compatibleAsIs();
                    }
                };

        TypeSerializerSnapshot<Integer> outerSnapshot =
                new NotCompletedTypeSerializerSnapshot() {
                    @Override
                    public TypeSerializerSchemaCompatibility<Integer> resolveSchemaCompatibility(
                            TypeSerializerSnapshot<Integer> newSerializer) {
                        return innerSnapshot.resolveSchemaCompatibility(innerSnapshot);
                    }
                };

        // The result of resolving schema compatibility should be determined by the new method of
        // innerSnapshot
        assertThat(outerSnapshot.resolveSchemaCompatibility(outerSnapshot).isCompatibleAsIs())
                .isTrue();
    }

    private static class NotCompletedTypeSerializer extends TypeSerializer<Integer> {

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<Integer> duplicate() {
            return this;
        }

        @Override
        public Integer createInstance() {
            return 0;
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 1;
        }

        @Override
        public void serialize(Integer record, DataOutputView target) {
            // do nothing
        }

        @Override
        public Integer deserialize(DataInputView source) {
            return 0;
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) {
            return reuse;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) {
            // do nothing
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            return new NotCompletedTypeSerializerSnapshot() {
                @Override
                public TypeSerializer<Integer> restoreSerializer() {
                    return NotCompletedTypeSerializer.this;
                }
            };
        }
    }

    private static class NotCompletedTypeSerializerSnapshot
            implements TypeSerializerSnapshot<Integer> {

        @Override
        public int getCurrentVersion() {
            return 0;
        }

        @Override
        public void writeSnapshot(DataOutputView out) {
            // do nothing
        }

        @Override
        public void readSnapshot(
                int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
            // do nothing
        }

        @Override
        public TypeSerializer<Integer> restoreSerializer() {
            return new NotCompletedTypeSerializer() {
                @Override
                public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
                    return NotCompletedTypeSerializerSnapshot.this;
                }
            };
        }

        @Override
        public TypeSerializerSchemaCompatibility<Integer> resolveSchemaCompatibility(
                TypeSerializerSnapshot<Integer> oldSerializerSnapshot) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
