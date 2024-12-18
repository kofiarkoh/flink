/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class VersionedIOWriteableTest {

    @Test
    void testReadSameVersion() throws Exception {

        String payload = "test";

        TestWriteable testWriteable = new TestWriteable(1, payload);
        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            testWriteable.write(new DataOutputViewStreamWrapper(out));
            serialized = out.toByteArray();
        }

        testWriteable = new TestWriteable(1);
        try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
            testWriteable.read(new DataInputViewStreamWrapper(in));
        }

        assertThat(testWriteable.getData()).isEqualTo(payload);
    }

    @Test
    void testReadCompatibleVersion() throws Exception {

        String payload = "test";

        TestWriteable testWriteable = new TestWriteable(1, payload);
        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            testWriteable.write(new DataOutputViewStreamWrapper(out));
            serialized = out.toByteArray();
        }

        testWriteable =
                new TestWriteable(2) {
                    @Override
                    public int[] getCompatibleVersions() {
                        return new int[] {1, 2};
                    }
                };
        try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
            testWriteable.read(new DataInputViewStreamWrapper(in));
        }

        assertThat(testWriteable.getData()).isEqualTo(payload);
    }

    @Test
    void testReadMismatchVersion() throws Exception {

        String payload = "test";

        TestWriteable testWriteable = new TestWriteable(1, payload);
        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            testWriteable.write(new DataOutputViewStreamWrapper(out));
            serialized = out.toByteArray();
        }

        TestWriteable finalTestWriteable = new TestWriteable(2);
        assertThatThrownBy(
                        () -> {
                            try (ByteArrayInputStreamWithPos in =
                                    new ByteArrayInputStreamWithPos(serialized)) {
                                finalTestWriteable.read(new DataInputViewStreamWrapper(in));
                            }
                        })
                .isInstanceOf(VersionMismatchException.class);

        assertThat(finalTestWriteable.getData()).isNull();
    }

    static class TestWriteable extends VersionedIOReadableWritable {

        private final int version;
        private String data;

        public TestWriteable(int version) {
            this(version, null);
        }

        public TestWriteable(int version, String data) {
            this.version = version;
            this.data = data;
        }

        @Override
        public int getVersion() {
            return version;
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            super.write(out);
            out.writeUTF(data);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            super.read(in);
            this.data = in.readUTF();
        }

        public String getData() {
            return data;
        }
    }
}
