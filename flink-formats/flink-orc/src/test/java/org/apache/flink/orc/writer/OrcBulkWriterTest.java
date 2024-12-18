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

package org.apache.flink.orc.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.data.Record;
import org.apache.flink.orc.util.OrcBulkWriterTestUtil;
import org.apache.flink.orc.vector.RecordVectorizer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.UniqueBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.legacy.StreamingFileSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/** Unit test for the ORC BulkWriter implementation. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class OrcBulkWriterTest {

    private final String schema = "struct<_col0:string,_col1:int>";
    private final List<Record> input =
            Arrays.asList(new Record("Shiv", 44), new Record("Jesse", 23), new Record("Walt", 50));

    @Test
    void testOrcBulkWriter(@TempDir File outDir) throws Exception {
        final Properties writerProps = new Properties();
        writerProps.setProperty("orc.compress", "LZ4");

        final OrcBulkWriterFactory<Record> writer =
                new OrcBulkWriterFactory<>(
                        new RecordVectorizer(schema), writerProps, new Configuration());

        StreamingFileSink<Record> sink =
                StreamingFileSink.forBulkFormat(new Path(outDir.toURI()), writer)
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .withBucketCheckInterval(10000)
                        .build();

        try (OneInputStreamOperatorTestHarness<Record, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 1, 1, 0)) {

            testHarness.setup();
            testHarness.open();

            int time = 0;
            for (final Record record : input) {
                testHarness.processElement(record, ++time);
            }

            testHarness.snapshot(1, ++time);
            testHarness.notifyOfCompletedCheckpoint(1);

            OrcBulkWriterTestUtil.validate(outDir, input);
        }
    }
}
