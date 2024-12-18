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

package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.InternalOperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.FlatMapDriver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.TaskTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializerFactory;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Metrics related tests for batch task chains. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class ChainedOperatorsMetricTest extends TaskTestBase {

    private static final int MEMORY_MANAGER_SIZE = 1024 * 1024 * 3;

    private static final int NETWORK_BUFFER_SIZE = 1024;

    private static final TypeSerializerFactory<Record> serFact = RecordSerializerFactory.get();

    private final List<Record> outList = new ArrayList<>();

    private static final String HEAD_OPERATOR_NAME = "headoperator";
    private static final String CHAINED_OPERATOR_NAME = "chainedoperator";

    @Test
    void testOperatorIOMetricReuse() throws Exception {
        // environment
        initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);

        this.mockEnv =
                new MockEnvironmentBuilder()
                        .setTaskName(HEAD_OPERATOR_NAME)
                        .setManagedMemorySize(MEMORY_MANAGER_SIZE)
                        .setInputSplitProvider(this.inputSplitProvider)
                        .setBufferSize(NETWORK_BUFFER_SIZE)
                        .setMetricGroup(
                                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                                                NoOpMetricRegistry.INSTANCE,
                                                "host",
                                                ResourceID.generate())
                                        .addJob(new JobID(), "jobName")
                                        .addTask(createExecutionAttemptId(), "task"))
                        .build();

        final int keyCnt = 100;
        final int valCnt = 20;
        final int numRecords = keyCnt * valCnt;
        addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);
        addOutput(this.outList);

        // the chained operator
        addChainedOperator();

        // creates the head operator and assembles the chain
        registerTask(FlatMapDriver.class, DuplicatingFlatMapFunction.class);
        final BatchTask<FlatMapFunction<Record, Record>, Record> testTask =
                new BatchTask<>(this.mockEnv);

        testTask.invoke();

        assertThat(this.outList).hasSize(numRecords * 2 * 2);

        final TaskMetricGroup taskMetricGroup = mockEnv.getMetricGroup();

        // verify task-level metrics
        {
            final TaskIOMetricGroup ioMetricGroup = taskMetricGroup.getIOMetricGroup();
            final Counter numRecordsInCounter = ioMetricGroup.getNumRecordsInCounter();
            final Counter numRecordsOutCounter = ioMetricGroup.getNumRecordsOutCounter();

            assertThat(numRecordsInCounter.getCount()).isEqualTo(numRecords);
            assertThat(numRecordsOutCounter.getCount()).isEqualTo(numRecords * 2 * 2);
        }

        // verify head operator metrics
        {
            // this only returns the existing group and doesn't create a new one
            final OperatorMetricGroup operatorMetricGroup1 =
                    taskMetricGroup.getOrAddOperator(HEAD_OPERATOR_NAME);
            final OperatorIOMetricGroup ioMetricGroup = operatorMetricGroup1.getIOMetricGroup();
            final Counter numRecordsInCounter = ioMetricGroup.getNumRecordsInCounter();
            final Counter numRecordsOutCounter = ioMetricGroup.getNumRecordsOutCounter();

            assertThat(numRecordsInCounter.getCount()).isEqualTo(numRecords);
            assertThat(numRecordsOutCounter.getCount()).isEqualTo(numRecords * 2);
        }

        // verify chained operator metrics
        {
            // this only returns the existing group and doesn't create a new one
            final InternalOperatorMetricGroup operatorMetricGroup1 =
                    taskMetricGroup.getOrAddOperator(CHAINED_OPERATOR_NAME);
            final InternalOperatorIOMetricGroup ioMetricGroup =
                    operatorMetricGroup1.getIOMetricGroup();
            final Counter numRecordsInCounter = ioMetricGroup.getNumRecordsInCounter();
            final Counter numRecordsOutCounter = ioMetricGroup.getNumRecordsOutCounter();

            assertThat(numRecordsInCounter.getCount()).isEqualTo(numRecords * 2);
            assertThat(numRecordsOutCounter.getCount()).isEqualTo(numRecords * 2 * 2);
        }
    }

    private void addChainedOperator() {
        final TaskConfig chainedConfig = new TaskConfig(new Configuration());

        // input
        chainedConfig.addInputToGroup(0);
        chainedConfig.setInputSerializer(serFact, 0);

        // output
        chainedConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
        chainedConfig.setOutputSerializer(serFact);

        // driver
        chainedConfig.setDriverStrategy(DriverStrategy.FLAT_MAP);

        // udf
        chainedConfig.setStubWrapper(new UserCodeClassWrapper<>(DuplicatingFlatMapFunction.class));

        getTaskConfig()
                .addChainedTask(ChainedFlatMapDriver.class, chainedConfig, CHAINED_OPERATOR_NAME);
    }

    /** Simple {@link FlatMapFunction} that duplicates the input. */
    public static class DuplicatingFlatMapFunction extends RichFlatMapFunction<Record, Record> {

        private static final long serialVersionUID = -1152068682935346164L;

        @Override
        public void flatMap(final Record value, final Collector<Record> out) throws Exception {
            out.collect(value);
            out.collect(value);
        }
    }
}
