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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.BooleanValue;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Random;

/** A test for the {@link BooleanValueSerializer}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class BooleanValueSerializerTest extends SerializerTestBase<BooleanValue> {

    @Override
    protected TypeSerializer<BooleanValue> createSerializer() {
        return new BooleanValueSerializer();
    }

    @Override
    protected int getLength() {
        return 1;
    }

    @Override
    protected Class<BooleanValue> getTypeClass() {
        return BooleanValue.class;
    }

    @Override
    protected BooleanValue[] getTestData() {
        Random rnd = new Random(874597969123412341L);

        return new BooleanValue[] {
            new BooleanValue(true),
            new BooleanValue(false),
            new BooleanValue(rnd.nextBoolean()),
            new BooleanValue(rnd.nextBoolean()),
            new BooleanValue(rnd.nextBoolean())
        };
    }
}
