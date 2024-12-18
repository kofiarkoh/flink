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

package org.apache.flink.formats.parquet.avro;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.TestDataGenerators;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.generated.Address;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.UniqueBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.legacy.StreamingFileSink;
import org.apache.flink.test.junit5.MiniClusterExtension;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple integration test case for writing bulk encoded files with the {@link StreamingFileSink}
 * with Parquet.
 */
@ExtendWith(MiniClusterExtension.class)
class AvroParquetStreamingFileSinkITCase {

    @Test
    void testWriteParquetAvroSpecific(@TempDir File folder) throws Exception {

        final List<Address> data =
                Arrays.asList(
                        new Address(1, "a", "b", "c", "12345"),
                        new Address(2, "p", "q", "r", "12345"),
                        new Address(3, "x", "y", "z", "12345"));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        DataStream<Address> stream =
                env.fromSource(
                        TestDataGenerators.fromDataWithSnapshotsLatch(
                                data, TypeInformation.of(Address.class)),
                        WatermarkStrategy.noWatermarks(),
                        "Test Source");

        stream.addSink(
                StreamingFileSink.forBulkFormat(
                                Path.fromLocalFile(folder),
                                AvroParquetWriters.forSpecificRecord(Address.class))
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .build());

        env.execute();

        validateResults(folder, SpecificData.get(), data);
    }

    @Test
    void testWriteParquetAvroGeneric(@TempDir File folder) throws Exception {

        final Schema schema = Address.getClassSchema();

        final Collection<GenericRecord> data = new GenericTestDataCollection();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        DataStream<GenericRecord> stream =
                env.fromSource(
                        TestDataGenerators.fromDataWithSnapshotsLatch(
                                data, new GenericRecordAvroTypeInfo(schema)),
                        WatermarkStrategy.noWatermarks(),
                        "Test Source");

        stream.addSink(
                StreamingFileSink.forBulkFormat(
                                Path.fromLocalFile(folder),
                                AvroParquetWriters.forGenericRecord(schema))
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .build());

        env.execute();

        List<Address> expected =
                Arrays.asList(
                        new Address(1, "a", "b", "c", "12345"),
                        new Address(2, "x", "y", "z", "98765"));

        validateResults(folder, SpecificData.get(), expected);
    }

    @Test
    void testWriteParquetAvroReflect(@TempDir File folder) throws Exception {

        final List<Datum> data =
                Arrays.asList(new Datum("a", 1), new Datum("b", 2), new Datum("c", 3));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        DataStream<Datum> stream =
                env.fromSource(
                        TestDataGenerators.fromDataWithSnapshotsLatch(
                                data, TypeInformation.of(Datum.class)),
                        WatermarkStrategy.noWatermarks(),
                        "Test Source");

        stream.addSink(
                StreamingFileSink.forBulkFormat(
                                Path.fromLocalFile(folder),
                                AvroParquetWriters.forReflectRecord(Datum.class))
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .build());

        env.execute();

        validateResults(folder, ReflectData.get(), data);
    }

    // ------------------------------------------------------------------------

    private static <T> void validateResults(File folder, GenericData dataModel, List<T> expected)
            throws Exception {
        File[] buckets = folder.listFiles();
        assertThat(buckets).hasSize(1);

        File[] partFiles = buckets[0].listFiles();
        assertThat(partFiles).hasSize(2);

        for (File partFile : partFiles) {
            assertThat(partFile.length()).isGreaterThan(0);

            final List<T> fileContent = readParquetFile(partFile, dataModel);
            assertThat(fileContent).isEqualTo(expected);
        }
    }

    private static <T> List<T> readParquetFile(File file, GenericData dataModel)
            throws IOException {
        InputFile inFile =
                HadoopInputFile.fromPath(
                        new org.apache.hadoop.fs.Path(file.toURI()), new Configuration());

        ArrayList<T> results = new ArrayList<>();
        try (ParquetReader<T> reader =
                AvroParquetReader.<T>builder(inFile).withDataModel(dataModel).build()) {
            T next;
            while ((next = reader.read()) != null) {
                results.add(next);
            }
        }

        return results;
    }

    private static @ExtendWith(CTestJUnit5Extension.class) @CTestClass
    class GenericTestDataCollection extends AbstractCollection<GenericRecord>
            implements Serializable {

        @Override
        public Iterator<GenericRecord> iterator() {
            final GenericRecord rec1 = new GenericData.Record(Address.getClassSchema());
            rec1.put(0, 1);
            rec1.put(1, "a");
            rec1.put(2, "b");
            rec1.put(3, "c");
            rec1.put(4, "12345");

            final GenericRecord rec2 = new GenericData.Record(Address.getClassSchema());
            rec2.put(0, 2);
            rec2.put(1, "x");
            rec2.put(2, "y");
            rec2.put(3, "z");
            rec2.put(4, "98765");

            return Arrays.asList(rec1, rec2).iterator();
        }

        @Override
        public int size() {
            return 2;
        }
    }

    // ------------------------------------------------------------------------

}
