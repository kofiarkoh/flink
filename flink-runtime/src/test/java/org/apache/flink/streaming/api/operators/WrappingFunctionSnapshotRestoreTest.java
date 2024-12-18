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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.WrappingFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test snapshot state with {@link WrappingFunction}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class WrappingFunctionSnapshotRestoreTest {

    @Test
    void testSnapshotAndRestoreWrappedCheckpointedFunction() throws Exception {

        StreamMap<Integer, Integer> operator =
                new StreamMap<>(new WrappingTestFun(new WrappingTestFun(new InnerTestFun())));

        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(5, 12L));

        // snapshot and restore from scratch
        OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

        testHarness.close();

        InnerTestFun innerTestFun = new InnerTestFun();
        operator = new StreamMap<>(new WrappingTestFun(new WrappingTestFun(innerTestFun)));

        testHarness = new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        assertThat(innerTestFun.wasRestored).isTrue();
        testHarness.close();
    }

    @Test
    void testSnapshotAndRestoreWrappedListCheckpointed() throws Exception {

        StreamMap<Integer, Integer> operator =
                new StreamMap<>(new WrappingTestFun(new WrappingTestFun(new InnerTestFunList())));

        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(5, 12L));

        // snapshot and restore from scratch
        OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

        testHarness.close();

        InnerTestFunList innerTestFun = new InnerTestFunList();
        operator = new StreamMap<>(new WrappingTestFun(new WrappingTestFun(innerTestFun)));

        testHarness = new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        assertThat(innerTestFun.wasRestored).isTrue();
        testHarness.close();
    }

    static @ExtendWith(CTestJUnit5Extension.class) @CTestClass class WrappingTestFun
            extends WrappingFunction<MapFunction<Integer, Integer>>
            implements MapFunction<Integer, Integer> {

        private static final long serialVersionUID = 1L;

        public WrappingTestFun(MapFunction<Integer, Integer> wrappedFunction) {
            super(wrappedFunction);
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }
    }

    static @ExtendWith(CTestJUnit5Extension.class) @CTestClass class InnerTestFun
            extends AbstractRichFunction
            implements MapFunction<Integer, Integer>, CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        private ListState<Integer> serializableListState;
        private boolean wasRestored;

        public InnerTestFun() {
            wasRestored = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (!wasRestored) {
                serializableListState.add(42);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            serializableListState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "test-state", IntSerializer.INSTANCE));
            if (context.isRestored()) {
                Iterator<Integer> integers = serializableListState.get().iterator();
                int act = integers.next();
                assertThat(act).isEqualTo(42);
                assertThat(integers).isExhausted();
                wasRestored = true;
            }
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }
    }

    static @ExtendWith(CTestJUnit5Extension.class) @CTestClass class InnerTestFunList
            extends AbstractRichFunction
            implements MapFunction<Integer, Integer>, ListCheckpointed<Integer> {

        private static final long serialVersionUID = 1L;

        private boolean wasRestored;

        public InnerTestFunList() {
            wasRestored = false;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(42);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            assertThat(state).hasSize(1);
            int val = state.get(0);
            assertThat(val).isEqualTo(42);
            wasRestored = true;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }
    }
}
