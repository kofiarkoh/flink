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

package org.apache.flink.core.io;

import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Suite of tests for {@link PostVersionedIOReadableWritable}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PostVersionedIOReadableWritableTest {

    @Test
    void testReadVersioned() throws IOException {
        byte[] payload = "test-data".getBytes(StandardCharsets.UTF_8);
        byte[] serialized = serializeWithPostVersionedReadableWritable(payload);
        byte[] restored = restoreWithPostVersionedReadableWritable(serialized, payload.length);

        assertThat(restored).containsExactly(payload);
    }

    @Test
    void testReadNonVersioned() throws IOException {
        byte[] preVersionedPayload = new byte[] {0x00, 0x00, 0x02, 0x33};
        byte[] serialized = serializeWithNonVersionedReadableWritable(preVersionedPayload);
        byte[] restored =
                restoreWithPostVersionedReadableWritable(serialized, preVersionedPayload.length);

        assertThat(restored).containsExactly(preVersionedPayload);
    }

    @Test
    void testReadNonVersionedWithLongPayload() throws IOException {
        byte[] preVersionedPayload = "test-data".getBytes(StandardCharsets.UTF_8);
        byte[] serialized = serializeWithNonVersionedReadableWritable(preVersionedPayload);
        byte[] restored =
                restoreWithPostVersionedReadableWritable(serialized, preVersionedPayload.length);

        assertThat(restored).containsExactly(preVersionedPayload);
    }

    @Test
    void testReadNonVersionedWithShortPayload() throws IOException {
        byte[] preVersionedPayload = new byte[] {-15, -51};
        byte[] serialized = serializeWithNonVersionedReadableWritable(preVersionedPayload);
        byte[] restored =
                restoreWithPostVersionedReadableWritable(serialized, preVersionedPayload.length);

        assertThat(restored).containsExactly(preVersionedPayload);
    }

    @Test
    void testReadNonVersionedWithEmptyPayload() throws IOException {
        byte[] preVersionedPayload = new byte[0];
        byte[] serialized = serializeWithNonVersionedReadableWritable(preVersionedPayload);
        byte[] restored =
                restoreWithPostVersionedReadableWritable(serialized, preVersionedPayload.length);

        assertThat(restored).containsExactly(preVersionedPayload);
    }

    private byte[] serializeWithNonVersionedReadableWritable(byte[] payload) throws IOException {
        TestNonVersionedReadableWritable versionedReadableWritable =
                new TestNonVersionedReadableWritable(payload);

        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            versionedReadableWritable.write(new DataOutputViewStreamWrapper(out));
            serialized = out.toByteArray();
        }

        return serialized;
    }

    private byte[] serializeWithPostVersionedReadableWritable(byte[] payload) throws IOException {
        TestPostVersionedReadableWritable versionedReadableWritable =
                new TestPostVersionedReadableWritable(payload);

        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            versionedReadableWritable.write(new DataOutputViewStreamWrapper(out));
            serialized = out.toByteArray();
        }

        return serialized;
    }

    private byte[] restoreWithPostVersionedReadableWritable(byte[] serialized, int expectedLength)
            throws IOException {
        TestPostVersionedReadableWritable restoredVersionedReadableWritable =
                new TestPostVersionedReadableWritable(expectedLength);

        try (ByteArrayInputStreamWithPos in =
                new TestByteArrayInputStreamProducingOneByteAtATime(serialized)) {
            restoredVersionedReadableWritable.read(in);
        }

        return restoredVersionedReadableWritable.getData();
    }

    private static void assertEmpty(DataInputView in) {
        assertThatThrownBy(in::readByte).isInstanceOf(EOFException.class);
    }

    static class TestPostVersionedReadableWritable extends PostVersionedIOReadableWritable {

        private static final int VERSION = 1;
        private final byte[] data;

        TestPostVersionedReadableWritable(int len) {
            this.data = new byte[len];
        }

        TestPostVersionedReadableWritable(byte[] data) {
            this.data = data;
        }

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            super.write(out);
            out.write(data);
        }

        @Override
        protected void read(DataInputView in, boolean wasVersioned) throws IOException {
            in.readFully(data);
            assertEmpty(in);
        }

        public byte[] getData() {
            return data;
        }
    }

    static class TestNonVersionedReadableWritable implements IOReadableWritable {

        private final byte[] data;

        TestNonVersionedReadableWritable(byte[] data) {
            this.data = data;
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.write(data);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            in.readFully(data);
            assertEmpty(in);
        }
    }

    static class TestByteArrayInputStreamProducingOneByteAtATime
            extends ByteArrayInputStreamWithPos {

        public TestByteArrayInputStreamProducingOneByteAtATime(byte[] buf) {
            super(buf);
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return super.read(b, off, Math.min(len, 1));
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }
    }
}
