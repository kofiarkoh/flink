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

import org.apache.flink.formats.protobuf.testproto.TimestampTestMulti;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.Timestamp;
import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.Assert.assertEquals;

/** Test conversion of proto timestamp data with multiple_files options to flink internal data. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class TimestampMultiProtoToRowTest {

    @Test
    public void testSimple() throws Exception {
        TimestampTestMulti timestampTestMulti =
                TimestampTestMulti.newBuilder()
                        .setTs(Timestamp.newBuilder().setSeconds(1672498800).setNanos(123))
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(
                        TimestampTestMulti.class, timestampTestMulti.toByteArray());

        RowData rowData = row.getRow(0, 2);
        assertEquals(1672498800, rowData.getLong(0));
        assertEquals(123, rowData.getInt(1));
    }
}
