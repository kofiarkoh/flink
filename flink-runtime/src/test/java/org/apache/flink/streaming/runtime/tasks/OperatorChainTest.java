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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.streaming.api.operators.StreamOperatorUtils.setupStreamOperator;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class test the {@link OperatorChain}.
 *
 * <p>It takes a different (simpler) approach at testing the operator chain than {@link
 * org.apache.flink.streaming.runtime.operators.StreamOperatorChainingTest}.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class OperatorChainTest {

    @Test
    void testPrepareCheckpointPreBarrier() throws Exception {
        final AtomicInteger intRef = new AtomicInteger();

        final OneInputStreamOperator<String, String> one = new ValidatingOperator(intRef, 0);
        final OneInputStreamOperator<String, String> two = new ValidatingOperator(intRef, 1);
        final OneInputStreamOperator<String, String> three = new ValidatingOperator(intRef, 2);

        final OperatorChain<?, ?> chain = setupOperatorChain(one, two, three);
        chain.prepareSnapshotPreBarrier(ValidatingOperator.CHECKPOINT_ID);

        assertThat(intRef).hasValue(3);
    }

    // ------------------------------------------------------------------------
    //  Operator Chain Setup Utils
    // ------------------------------------------------------------------------

    @SafeVarargs
    static <T, OP extends StreamOperator<T>> OperatorChain<T, OP> setupOperatorChain(
            OneInputStreamOperator<T, T>... operators) throws Exception {

        checkNotNull(operators);
        checkArgument(operators.length > 0);

        try (MockEnvironment env = MockEnvironment.builder().build()) {
            final StreamTask<?, ?> containingTask = new MockStreamTaskBuilder(env).build();

            final StreamConfig cfg = new StreamConfig(new Configuration());
            cfg.setOperatorID(new OperatorID());
            cfg.setStateKeySerializer(new StringSerializer());
            cfg.serializeAllConfigs();
            final List<StreamOperatorWrapper<?, ?>> operatorWrappers = new ArrayList<>();

            // initial output goes to nowhere
            @SuppressWarnings({"unchecked", "rawtypes"})
            WatermarkGaugeExposingOutput<StreamRecord<T>> lastWriter =
                    new BroadcastingOutputCollector<>(
                            new OutputWithChainingCheck[0], new SimpleCounter());

            // build the reverse operators array
            for (int i = 0; i < operators.length; i++) {
                int operatorIndex = operators.length - i - 1;
                OneInputStreamOperator<T, T> op = operators[operatorIndex];
                if (op instanceof AbstractStreamOperator) {
                    setupStreamOperator(
                            (AbstractStreamOperator<T>) op, containingTask, cfg, lastWriter);
                }
                lastWriter = new ChainingOutput<>(op, null, op.getMetricGroup(), null);

                ProcessingTimeService processingTimeService = null;
                if (op instanceof AbstractStreamOperator) {
                    processingTimeService =
                            ((AbstractStreamOperator) op).getProcessingTimeService();
                }
                operatorWrappers.add(
                        new StreamOperatorWrapper<>(
                                op,
                                Optional.ofNullable(processingTimeService),
                                containingTask.getMailboxExecutorFactory().createExecutor(i),
                                operatorIndex == 0));
            }

            @SuppressWarnings("unchecked")
            final StreamOperatorWrapper<T, OP> headOperatorWrapper =
                    (StreamOperatorWrapper<T, OP>)
                            operatorWrappers.get(operatorWrappers.size() - 1);

            return new RegularOperatorChain<>(
                    operatorWrappers,
                    new RecordWriterOutput<?>[0],
                    lastWriter,
                    headOperatorWrapper);
        }
    }

    // ------------------------------------------------------------------------
    //  Test Operator Implementations
    // ------------------------------------------------------------------------

    private static class ValidatingOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        private static final long serialVersionUID = 1L;

        static final long CHECKPOINT_ID = 5765167L;

        final AtomicInteger toUpdate;
        final int expected;

        public ValidatingOperator(AtomicInteger toUpdate, int expected) {
            this.toUpdate = toUpdate;
            this.expected = expected;
        }

        @Override
        public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
            assertThat(checkpointId).as("wrong checkpointId").isEqualTo(CHECKPOINT_ID);
            assertThat(toUpdate.getAndIncrement()).as("wrong order").isEqualTo(expected);
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorID getOperatorID() {
            return new OperatorID();
        }
    }
}
