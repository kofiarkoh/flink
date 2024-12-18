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

package org.apache.flink.connector.file.sink.utils;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader.Decoder;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utilities for file sinks that writes a sequence of continues integers into files starting from 0.
 * The sinks expect multiple sources writing the same sequence onto the disk and the integers are
 * assigned to different buckets according to modulo.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class IntegerFileSinkTestDataUtils {

    /** Testing sink {@link Encoder} that writes integer with its binary representation. */
    public static class IntEncoder implements Encoder<Integer> {

        @Override
        public void encode(Integer element, OutputStream stream) throws IOException {
            stream.write(ByteBuffer.allocate(4).putInt(element).array());
            stream.flush();
        }
    }

    /** Testing sink {@link Decoder} that reads integer for compaction. */
    public static class IntDecoder implements Decoder<Integer> {

        private InputStream input;

        @Override
        public void open(InputStream input) throws IOException {
            this.input = input;
        }

        @Override
        public Integer decodeNext() throws IOException {
            byte[] bytes = new byte[4];
            int read = input.read(bytes);
            return read < 0 ? null : ByteBuffer.wrap(bytes).getInt();
        }

        @Override
        public void close() throws IOException {
            input.close();
            input = null;
        }
    }

    /** Testing {@link BucketAssigner} that assigns integers according to modulo. */
    public static class ModuloBucketAssigner implements BucketAssigner<Integer, String> {

        private final int numBuckets;

        public ModuloBucketAssigner(int numBuckets) {
            this.numBuckets = numBuckets;
        }

        @Override
        public String getBucketId(Integer element, Context context) {
            return Integer.toString(element % numBuckets);
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    /**
     * Verifies the files written by the sink contains the expected integer sequences. The integers
     * are partition into different buckets according to module, and each integer will be repeated
     * by <tt>numSources</tt> times.
     *
     * @param path The directory to check.
     * @param numRecords The total number of records.
     * @param numBuckets The number of buckets to assign.
     * @param numSources The parallelism of sources generating the sequences. Each integer will be
     *     repeat for <tt>numSources</tt> times.
     */
    public static void checkIntegerSequenceSinkOutput(
            String path, int numRecords, int numBuckets, int numSources) throws Exception {
        File dir = new File(path);
        String[] subDirNames = dir.list();
        assertThat(subDirNames).isNotNull();

        Arrays.sort(subDirNames, Comparator.comparingInt(Integer::parseInt));
        assertThat(subDirNames).hasSize(numBuckets);
        for (int i = 0; i < numBuckets; ++i) {
            assertThat(subDirNames[i]).isEqualTo(Integer.toString(i));

            // now check its content
            File bucketDir = new File(path, subDirNames[i]);
            assertThat(bucketDir)
                    .as(bucketDir.getAbsolutePath() + " Should be a existing directory")
                    .isDirectory();

            Map<Integer, Integer> counts = new HashMap<>();
            File[] files = bucketDir.listFiles(f -> !f.getName().startsWith("."));
            assertThat(files).isNotNull();

            for (File file : files) {
                assertThat(file).isFile();

                try (DataInputStream dataInputStream =
                        new DataInputStream(new FileInputStream(file))) {
                    while (true) {
                        int value = dataInputStream.readInt();
                        counts.compute(value, (k, v) -> v == null ? 1 : v + 1);
                    }
                } catch (EOFException e) {
                    // End the reading
                }
            }

            int expectedCount = numRecords / numBuckets + (i < numRecords % numBuckets ? 1 : 0);
            assertThat(counts).hasSize(expectedCount);

            for (int j = i; j < numRecords; j += numBuckets) {
                assertThat(counts.getOrDefault(j, 0).intValue())
                        .as(
                                "The record "
                                        + j
                                        + " should occur "
                                        + numSources
                                        + " times, "
                                        + " but only occurs "
                                        + counts.getOrDefault(j, 0)
                                        + "time")
                        .isEqualTo(numSources);
            }
        }
    }
}
