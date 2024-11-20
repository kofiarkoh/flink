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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.testutils.CommonTestUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ValueStateDescriptor}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class ValueStateDescriptorTest {

    @Test
    void testHashCodeAndEquals() throws Exception {
        final String name = "testName";

        ValueStateDescriptor<String> original =
                new ValueStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO);
        ValueStateDescriptor<String> same =
                new ValueStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO);
        ValueStateDescriptor<String> sameBySerializer =
                new ValueStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO);

        // test that hashCode() works on state descriptors with initialized and uninitialized
        // serializers
        assertThat(same).hasSameHashCodeAs(original);
        assertThat(sameBySerializer).hasSameHashCodeAs(original);

        assertThat(same).isEqualTo(original);
        assertThat(sameBySerializer).isEqualTo(original);

        // equality with a clone
        ValueStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(original);
        assertThat(clone).isEqualTo(original);
    }
}
