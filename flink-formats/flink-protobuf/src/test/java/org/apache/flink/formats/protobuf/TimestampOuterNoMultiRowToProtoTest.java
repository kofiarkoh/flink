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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.TimestampTestOuterNomultiProto;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.Assert.assertEquals;

/**
 * Test conversion of flink internal primitive data to proto timestamp data with outer_classname
 * options.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class TimestampOuterNoMultiRowToProtoTest {

    @Test
    public void testSimple() throws Exception {
        RowData row = GenericRowData.of(GenericRowData.of(1672498800L, 123));

        byte[] bytes =
                ProtobufTestHelper.rowToPbBytes(
                        row, TimestampTestOuterNomultiProto.TimestampTestOuterNoMulti.class);
        TimestampTestOuterNomultiProto.TimestampTestOuterNoMulti timestampTestOuterNoMulti =
                TimestampTestOuterNomultiProto.TimestampTestOuterNoMulti.parseFrom(bytes);
        assertEquals(1672498800, timestampTestOuterNoMulti.getTs().getSeconds());
        assertEquals(123, timestampTestOuterNoMulti.getTs().getNanos());
    }
}
