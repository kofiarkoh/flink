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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.GSBlobStorage;
import org.apache.flink.fs.gs.storage.MockBlobStorage;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.flink.fs.gs.TestUtils.RANDOM_SEED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link GSChecksumWriteChannel}. */
@ExtendWith(ParameterizedTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class GSChecksumWriteChannelTest {

    /* The sizes of each buffer of bytes used for writing. */
    @Parameter(value = 0)
    private int[] bufferSizes;

    /* The start positions in write buffers. */
    @Parameter(value = 1)
    private int[] writeStarts;

    /* The length of each write. */
    @Parameter(value = 2)
    private int[] writeLengths;

    @Parameter(value = 3)
    private String description;

    @Parameters(name = "{3}")
    private static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {
                        // a simple write of 64 bytes from the start of a 64-byte byte
                        new int[] {64}, new int[] {0}, new int[] {64}, "simple write"
                    },
                    {
                        // write 64 bytes from the start of a 64-byte buffer, then 128 bytes
                        // from the start of a 128-byte buffer, then 64 bytes from the start
                        // of a 64-byte buffer
                        new int[] {64, 128, 64},
                        new int[] {0, 0, 0},
                        new int[] {64, 128, 64},
                        "multiple write"
                    },
                    {
                        // write 32 bytes from position 16 of a 64-byte buffer,
                        // then 48 bytes from position 32 of a 128-byte buffer,
                        // then 1 byte from position 32 of a 64-byte buffer
                        new int[] {64, 128, 64},
                        new int[] {16, 32, 32},
                        new int[] {32, 48, 1},
                        "multiple partial writes"
                    },
                });
    }

    private byte[][] byteBuffers;

    private byte[] expectedWrittenBytes;

    private GSBlobIdentifier blobIdentifier;

    @BeforeEach
    void before() throws IOException {
        Random random = new Random();
        random.setSeed(RANDOM_SEED);

        blobIdentifier = new GSBlobIdentifier("foo", "bar");

        // initialize the byte buffers and determine what we expect to be written
        byteBuffers = new byte[bufferSizes.length][];
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {

            for (int i = 0; i < bufferSizes.length; i++) {

                int size = bufferSizes[i];
                byteBuffers[i] = new byte[size];
                random.nextBytes(byteBuffers[i]);

                int start = writeStarts[i];
                int length = writeLengths[i];
                stream.write(byteBuffers[i], start, length);
            }

            stream.flush();
            expectedWrittenBytes = stream.toByteArray();
        }
    }

    /**
     * Write each of the partial byte buffers and confirm we get the expected results, including a
     * valid checksum and the expected data in the storage.
     *
     * @throws IOException On storage failure.
     */
    @TestTemplate
    void shouldWriteProperly() throws IOException {

        MockBlobStorage blobStorage = new MockBlobStorage();
        GSBlobStorage.WriteChannel writeChannel = blobStorage.writeBlob(blobIdentifier);
        GSChecksumWriteChannel checksumWriteChannel =
                new GSChecksumWriteChannel(blobStorage, writeChannel, blobIdentifier);

        // write each partial buffer and validate the written count
        for (int i = 0; i < byteBuffers.length; i++) {
            int writtenCount =
                    checksumWriteChannel.write(byteBuffers[i], writeStarts[i], writeLengths[i]);
            assertThat(writtenCount).isEqualTo(writeLengths[i]);
        }

        // close the write, this also validates the checksum
        checksumWriteChannel.close();

        // read the value out of storage, the bytes should match
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(blobIdentifier);
        assertThat(blobValue.content).isEqualTo(expectedWrittenBytes);
    }

    /**
     * Simulate a checksum failure and confirm an exception is thrown.
     *
     * @throws IOException On checksum failure.
     */
    @TestTemplate
    void shouldThrowOnChecksumMismatch() throws IOException {

        MockBlobStorage blobStorage = new MockBlobStorage();
        blobStorage.forcedChecksum = "";
        GSBlobStorage.WriteChannel writeChannel = blobStorage.writeBlob(blobIdentifier);
        GSChecksumWriteChannel checksumWriteChannel =
                new GSChecksumWriteChannel(blobStorage, writeChannel, blobIdentifier);

        // write each partial buffer and validate the written count
        for (int i = 0; i < byteBuffers.length; i++) {

            int writtenCount =
                    checksumWriteChannel.write(byteBuffers[i], writeStarts[i], writeLengths[i]);
            assertThat(writtenCount).isEqualTo(writeLengths[i]);
        }

        // close the write, this also validates the checksum
        assertThatThrownBy(() -> checksumWriteChannel.close()).isInstanceOf(IOException.class);
    }
}
