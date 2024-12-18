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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.api.common.typeutils.TypeInformationTestBase;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test for {@link BasicArrayTypeInfo}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class BasicArrayTypeInfoTest extends TypeInformationTestBase<BasicArrayTypeInfo<?, ?>> {

    @Override
    protected BasicArrayTypeInfo<?, ?>[] getTestData() {
        return new BasicArrayTypeInfo<?, ?>[] {
            BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO,
            BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO,
            BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO,
            BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO,
            BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO,
            BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO,
            BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO,
            BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO,
            BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO
        };
    }
}
