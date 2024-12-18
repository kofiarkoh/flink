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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.formats.avro.utils.AvroTestUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test for {@link AvroSerializer} that tests GenericRecord. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class AvroSerializerLargeGenericRecordTest extends SerializerTestBase<GenericRecord> {

    private static final Schema SCHEMA = AvroTestUtils.getLargeSchema();

    @Override
    protected TypeSerializer<GenericRecord> createSerializer() {
        return new AvroSerializer<>(GenericRecord.class, SCHEMA);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<GenericRecord> getTypeClass() {
        return GenericRecord.class;
    }

    @Override
    protected GenericRecord[] getTestData() {
        return new GenericRecord[] {
            new GenericRecordBuilder(SCHEMA).set("field1", "foo bar").build()
        };
    }
}
