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

package org.apache.flink.streaming.api.operators.sortpartition;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link VariableLengthByteKeyAndValueComparator} in {@link KeyedSortPartitionOperator}.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class VariableLengthByteKeyAndValueComparatorTest
        extends ComparatorTestBase<Tuple2<byte[], String>> {
    @Override
    protected Order[] getTestedOrder() {
        return new Order[] {Order.ASCENDING};
    }

    @Override
    protected TypeComparator<Tuple2<byte[], String>> createComparator(boolean ascending) {
        return new VariableLengthByteKeyAndValueComparator<>(
                BasicTypeInfo.STRING_TYPE_INFO.createComparator(ascending, null));
    }

    @Override
    protected TypeSerializer<Tuple2<byte[], String>> createSerializer() {
        StringSerializer stringSerializer = new StringSerializer();
        return new KeyAndValueSerializer<>(stringSerializer, stringSerializer.getLength());
    }

    @Override
    protected void deepEquals(
            String message, Tuple2<byte[], String> should, Tuple2<byte[], String> is) {
        assertThat(is.f0).as(message).isEqualTo(should.f0);
        assertThat(is.f1).as(message).isEqualTo(should.f1);
    }

    @Override
    protected Tuple2<byte[], String>[] getSortedTestData() {
        return SerializerComparatorTestData.getOrderedStringTestData();
    }
}
