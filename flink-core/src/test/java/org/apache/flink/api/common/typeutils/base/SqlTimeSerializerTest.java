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

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Time;

/** A test for the {@link SqlTimeSerializer}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class SqlTimeSerializerTest extends SerializerTestBase<Time> {

    @Override
    protected TypeSerializer<Time> createSerializer() {
        return new SqlTimeSerializer();
    }

    @Override
    protected int getLength() {
        return 8;
    }

    @Override
    protected Class<Time> getTypeClass() {
        return Time.class;
    }

    @Override
    protected Time[] getTestData() {
        return new Time[] {
            new Time(0L),
            Time.valueOf("00:00:00"),
            Time.valueOf("02:42:25"),
            Time.valueOf("14:15:59"),
            Time.valueOf("18:00:45")
        };
    }
}
