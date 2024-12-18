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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KeyAndValueSerializer}, which verify variable length keys. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class VariableLengthKeyAndValueSerializerTest
        extends SerializerTestBase<Tuple2<byte[], StreamRecord<String>>> {

    @Override
    protected TypeSerializer<Tuple2<byte[], StreamRecord<String>>> createSerializer() {
        StringSerializer stringSerializer = new StringSerializer();
        return new KeyAndValueSerializer<>(stringSerializer, stringSerializer.getLength());
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Class<Tuple2<byte[], StreamRecord<String>>> getTypeClass() {
        return (Class<Tuple2<byte[], StreamRecord<String>>>) (Class) Tuple2.class;
    }

    @Override
    protected Tuple2<byte[], StreamRecord<String>>[] getTestData() {
        return SerializerComparatorTestData.getOrderedStringTestData();
    }

    @Override
    @Test
    protected void testConfigSnapshotInstantiation() {
        assertThatThrownBy(() -> super.testConfigSnapshotInstantiation())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Override
    @Test
    protected void testSnapshotConfigurationAndReconfigure() {
        assertThatThrownBy(() -> super.testSnapshotConfigurationAndReconfigure())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
