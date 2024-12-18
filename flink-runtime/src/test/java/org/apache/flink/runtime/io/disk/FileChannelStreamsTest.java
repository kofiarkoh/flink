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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.types.StringValue;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class FileChannelStreamsTest {

    @Test
    void testCloseAndDeleteOutputView() throws Exception {
        try (IOManager ioManager = new IOManagerAsync()) {
            MemoryManager memMan = MemoryManagerBuilder.newBuilder().build();
            List<MemorySegment> memory = new ArrayList<MemorySegment>();
            memMan.allocatePages(new DummyInvokable(), memory, 4);

            FileIOChannel.ID channel = ioManager.createChannel();
            BlockChannelWriter<MemorySegment> writer = ioManager.createBlockChannelWriter(channel);

            FileChannelOutputView out =
                    new FileChannelOutputView(writer, memMan, memory, memMan.getPageSize());
            new StringValue("Some test text").write(out);

            // close for the first time, make sure all memory returns
            out.close();
            assertThat(memMan.verifyEmpty()).isTrue();

            // close again, should not cause an exception
            out.close();

            // delete, make sure file is removed
            out.closeAndDelete();
            assertThat(new File(channel.getPath())).doesNotExist();
        }
    }

    @Test
    void testCloseAndDeleteInputView() throws Exception {
        try (IOManager ioManager = new IOManagerAsync()) {
            MemoryManager memMan = MemoryManagerBuilder.newBuilder().build();
            List<MemorySegment> memory = new ArrayList<MemorySegment>();
            memMan.allocatePages(new DummyInvokable(), memory, 4);

            FileIOChannel.ID channel = ioManager.createChannel();

            // add some test data
            try (FileWriter wrt = new FileWriter(channel.getPath())) {
                wrt.write("test data");
            }

            BlockChannelReader<MemorySegment> reader = ioManager.createBlockChannelReader(channel);
            FileChannelInputView in = new FileChannelInputView(reader, memMan, memory, 9);

            // read just something
            in.readInt();

            // close for the first time, make sure all memory returns
            in.close();
            assertThat(memMan.verifyEmpty()).isTrue();

            // close again, should not cause an exception
            in.close();

            // delete, make sure file is removed
            in.closeAndDelete();
            assertThat(new File(channel.getPath())).doesNotExist();
        }
    }
}
