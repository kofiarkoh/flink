/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackendParametersImpl;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.source.TestingSourceOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Helper @ExtendWith(CTestJUnit5Extension.class) @CTestClass class for testing {@link
 * SourceOperator}.
 */
@SuppressWarnings("serial")
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class SourceOperatorTestContext implements AutoCloseable {

    public static final int SUBTASK_INDEX = 1;
    public static final MockSourceSplit MOCK_SPLIT = new MockSourceSplit(1234, 10);

    private MockSourceReader mockSourceReader;
    private MockOperatorEventGateway mockGateway;
    private TestProcessingTimeService timeService;
    private SourceOperator<Integer, MockSourceSplit> operator;

    public SourceOperatorTestContext() throws Exception {
        this(false);
    }

    public SourceOperatorTestContext(boolean idle) throws Exception {
        this(idle, WatermarkStrategy.noWatermarks());
    }

    public SourceOperatorTestContext(boolean idle, WatermarkStrategy<Integer> watermarkStrategy)
            throws Exception {
        this(idle, false, watermarkStrategy, new MockOutput<>(new ArrayList<>()));
    }

    public SourceOperatorTestContext(
            boolean idle,
            boolean usePerSplitOutputs,
            WatermarkStrategy<Integer> watermarkStrategy,
            Output<StreamRecord<Integer>> output)
            throws Exception {
        mockSourceReader =
                new MockSourceReader(
                        idle
                                ? MockSourceReader.WaitingForSplits.WAIT_UNTIL_ALL_SPLITS_ASSIGNED
                                : MockSourceReader.WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS,
                        idle,
                        usePerSplitOutputs);
        mockGateway = new MockOperatorEventGateway();
        timeService = new TestProcessingTimeService();
        Environment env = getTestingEnvironment();
        operator =
                new TestingSourceOperator<>(
                        new StreamOperatorParameters<>(
                                new SourceOperatorStreamTask<Integer>(env),
                                new MockStreamConfig(new Configuration(), 1),
                                output,
                                () -> timeService,
                                null,
                                null),
                        mockSourceReader,
                        watermarkStrategy,
                        timeService,
                        mockGateway,
                        SUBTASK_INDEX,
                        5,
                        true);
        operator.initializeState(
                new StreamTaskStateInitializerImpl(env, new HashMapStateBackend()));
    }

    @Override
    public void close() throws Exception {
        operator.close();
        checkState(mockSourceReader.isClosed());
    }

    public TestProcessingTimeService getTimeService() {
        return timeService;
    }

    public SourceOperator<Integer, MockSourceSplit> getOperator() {
        return operator;
    }

    public MockOperatorEventGateway getGateway() {
        return mockGateway;
    }

    public MockSourceReader getSourceReader() {
        return mockSourceReader;
    }

    public StateInitializationContext createStateContext() throws Exception {
        return createStateContext(Collections.singletonList(MOCK_SPLIT));
    }

    public StateInitializationContext createStateContext(Collection<MockSourceSplit> initialSplits)
            throws Exception {

        List<byte[]> serializedSplits = new ArrayList<>();
        for (MockSourceSplit initialSplit : initialSplits) {
            serializedSplits.add(
                    SimpleVersionedSerialization.writeVersionAndSerialize(
                            new MockSourceSplitSerializer(), initialSplit));
        }

        // Crate the state context.
        OperatorStateStore operatorStateStore = createOperatorStateStore();
        StateInitializationContext stateContext =
                new StateInitializationContextImpl(null, operatorStateStore, null, null, null);

        // Update the context.
        stateContext
                .getOperatorStateStore()
                .getListState(SourceOperator.SPLITS_STATE_DESC)
                .update(serializedSplits);

        return stateContext;
    }

    private OperatorStateStore createOperatorStateStore() throws Exception {
        MockEnvironment env = new MockEnvironmentBuilder().build();
        final AbstractStateBackend abstractStateBackend = new HashMapStateBackend();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        return abstractStateBackend.createOperatorStateBackend(
                new OperatorStateBackendParametersImpl(
                        env, "test-operator", Collections.emptyList(), cancelStreamRegistry));
    }

    private Environment getTestingEnvironment() {
        return new StreamMockEnvironment(
                new Configuration(),
                new Configuration(),
                new ExecutionConfig(),
                1L,
                new MockInputSplitProvider(),
                1,
                new TestTaskStateManager());
    }
}
