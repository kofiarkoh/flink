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

package org.apache.flink.runtime.operators.resettable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.MutableObjectIteratorWrapper;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializer;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class SpillingResettableMutableObjectIteratorTest {

    private static final int NUM_TESTRECORDS = 50000;

    private static final int MEMORY_CAPACITY = 10 * 1024 * 1024;

    private IOManager ioman;

    private MemoryManager memman;

    private MutableObjectIterator<Record> reader;

    private final TypeSerializer<Record> serializer = RecordSerializer.get();

    @BeforeEach
    void startup() {
        // set up IO and memory manager
        this.memman = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_CAPACITY).build();
        this.ioman = new IOManagerAsync();

        // create test objects
        final ArrayList<Record> objects = new ArrayList<Record>(NUM_TESTRECORDS);

        for (int i = 0; i < NUM_TESTRECORDS; ++i) {
            Record tmp = new Record(new IntValue(i));
            objects.add(tmp);
        }
        this.reader = new MutableObjectIteratorWrapper(objects.iterator());
    }

    @AfterEach
    void shutdown() throws Exception {
        this.ioman.close();
        this.ioman = null;

        assertThat(this.memman.verifyEmpty())
                .withFailMessage(
                        "A memory leak has occurred: Not all memory was properly returned to the memory manager.")
                .isTrue();
        this.memman.shutdown();
        this.memman = null;
    }

    /**
     * Tests the resettable iterator with too little memory, so that the data has to be written to
     * disk.
     */
    @Test
    void testResettableIterator() {
        try {
            final AbstractInvokable memOwner = new DummyInvokable();

            // create the resettable Iterator
            SpillingResettableMutableObjectIterator<Record> iterator =
                    new SpillingResettableMutableObjectIterator<Record>(
                            this.reader, this.serializer, this.memman, this.ioman, 2, memOwner);

            // open the iterator
            iterator.open();

            // now test walking through the iterator
            int count = 0;
            Record target = new Record();
            while ((target = iterator.next(target)) != null) {
                assertThat(target.getField(0, IntValue.class).getValue())
                        .withFailMessage(
                                "In initial run, element %d does not match expected value!", count)
                        .isEqualTo(count++);
            }
            assertThat(count)
                    .withFailMessage("Too few elements were deserialized in initial run!")
                    .isEqualTo(NUM_TESTRECORDS);
            // test resetting the iterator a few times
            for (int j = 0; j < 10; ++j) {
                count = 0;
                iterator.reset();
                target = new Record();
                // now we should get the same results
                while ((target = iterator.next(target)) != null) {
                    assertThat(target.getField(0, IntValue.class).getValue())
                            .withFailMessage(
                                    "After reset nr. %d element %d does not match expected value!",
                                    j + 1, count)
                            .isEqualTo(count++);
                }
                assertThat(count)
                        .withFailMessage(
                                "Too few elements were deserialized after reset nr. %d!", j + 1)
                        .isEqualTo(NUM_TESTRECORDS);
            }
            // close the iterator
            iterator.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test encountered an exception.");
        }
    }

    /**
     * Tests the resettable iterator with enough memory so that all data is kept locally in memory.
     */
    @Test
    void testResettableIteratorInMemory() {
        try {
            final AbstractInvokable memOwner = new DummyInvokable();

            // create the resettable Iterator
            SpillingResettableMutableObjectIterator<Record> iterator =
                    new SpillingResettableMutableObjectIterator<Record>(
                            this.reader, this.serializer, this.memman, this.ioman, 20, memOwner);

            // open the iterator
            iterator.open();

            // now test walking through the iterator
            int count = 0;
            Record target = new Record();
            while ((target = iterator.next(target)) != null) {
                assertThat(target.getField(0, IntValue.class).getValue())
                        .withFailMessage(
                                "In initial run, element %d does not match expected value!", count)
                        .isEqualTo(count++);
            }
            assertThat(count)
                    .withFailMessage("Too few elements were deserialized in initial run!")
                    .isEqualTo(NUM_TESTRECORDS);
            // test resetting the iterator a few times
            for (int j = 0; j < 10; ++j) {
                count = 0;
                iterator.reset();
                target = new Record();
                // now we should get the same results
                while ((target = iterator.next(target)) != null) {
                    assertThat(target.getField(0, IntValue.class).getValue())
                            .withFailMessage(
                                    "After reset nr. %d element %d does not match expected value!",
                                    j + 1, count)
                            .isEqualTo(count++);
                }
                assertThat(count)
                        .withFailMessage(
                                "Too few elements were deserialized after reset nr. %d!", j + 1)
                        .isEqualTo(NUM_TESTRECORDS);
            }
            // close the iterator
            iterator.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test encountered an exception.");
        }
    }
}
