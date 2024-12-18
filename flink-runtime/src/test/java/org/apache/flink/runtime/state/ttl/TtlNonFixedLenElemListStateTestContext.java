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

import org.apache.flink.api.common.typeutils.base.StringSerializer;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;

/**
 * Test suite for {@link TtlListState} with elements of non-fixed byte length in serialized form.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TtlNonFixedLenElemListStateTestContext extends TtlListStateTestContextBase<String> {
    TtlNonFixedLenElemListStateTestContext() {
        super(StringSerializer.INSTANCE);
    }

    @Override
    void initTestValues() {
        emptyValue = Collections.emptyList();

        updateEmpty = Arrays.asList("5", "7", "10");
        updateUnexpired = Arrays.asList("8", "9", "11");
        updateExpired = Arrays.asList("1", "4");

        getUpdateEmpty = updateEmpty;
        getUnexpired = updateUnexpired;
        getUpdateExpired = updateExpired;
    }

    @Override
    String generateRandomElement(int i) {
        return Integer.toString(RANDOM.nextInt(100));
    }
}
