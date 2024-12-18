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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.consumer.IteratorWrappingTestSingleInputGate;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializerFactory;
import org.apache.flink.types.Record;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

public abstract @ExtendWith(CTestJUnit5Extension.class) @CTestClass class TaskTestBase {

    @TempDir protected java.nio.file.Path tempFolder;

    protected long memorySize = 0;

    protected MockInputSplitProvider inputSplitProvider;

    protected MockEnvironment mockEnv;

    protected void initEnvironment(long memorySize, int bufferSize) {
        this.memorySize = memorySize;
        this.inputSplitProvider = new MockInputSplitProvider();
        this.mockEnv =
                new MockEnvironmentBuilder()
                        .setTaskName("mock task")
                        .setManagedMemorySize(this.memorySize)
                        .setInputSplitProvider(this.inputSplitProvider)
                        .setBufferSize(bufferSize)
                        .build();
    }

    protected IteratorWrappingTestSingleInputGate<Record> addInput(
            MutableObjectIterator<Record> input, int groupId) {
        return addInput(input, groupId, true);
    }

    protected IteratorWrappingTestSingleInputGate<Record> addInput(
            MutableObjectIterator<Record> input, int groupId, boolean read) {
        final IteratorWrappingTestSingleInputGate<Record> reader = this.mockEnv.addInput(input);
        TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());
        conf.addInputToGroup(groupId);
        conf.setInputSerializer(RecordSerializerFactory.get(), groupId);

        if (read) {
            reader.notifyNonEmpty();
        }

        return reader;
    }

    protected void addOutput(List<Record> output) {
        this.mockEnv.addOutput(output);
        TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());
        conf.addOutputShipStrategy(ShipStrategyType.FORWARD);
        conf.setOutputSerializer(RecordSerializerFactory.get());
    }

    protected TaskConfig getTaskConfig() {
        return new TaskConfig(this.mockEnv.getTaskConfiguration());
    }

    protected Configuration getConfiguration() {
        return this.mockEnv.getTaskConfiguration();
    }

    protected void registerTask(
            @SuppressWarnings("rawtypes") Class<? extends Driver> driver,
            Class<? extends RichFunction> stubClass) {

        final TaskConfig config = new TaskConfig(this.mockEnv.getTaskConfiguration());
        config.setDriver(driver);
        config.setStubWrapper(new UserCodeClassWrapper<>(stubClass));
    }

    protected void registerFileOutputTask(
            Class<? extends FileOutputFormat<Record>> stubClass,
            String outPath,
            Configuration formatParams) {

        registerFileOutputTask(
                InstantiationUtil.instantiate(stubClass, FileOutputFormat.class),
                outPath,
                formatParams);
    }

    protected void registerFileOutputTask(
            FileOutputFormat<Record> outputFormat, String outPath, Configuration formatParams) {

        outputFormat.setOutputFilePath(new Path(outPath));
        outputFormat.setWriteMode(WriteMode.OVERWRITE);

        OperatorID operatorID = new OperatorID();
        new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader())
                .addOutputFormat(operatorID, outputFormat)
                .addParameters(operatorID, formatParams)
                .write(new TaskConfig(this.mockEnv.getTaskConfiguration()));
    }

    protected void registerFileInputTask(
            AbstractInvokable inTask,
            Class<? extends DelimitedInputFormat<Record>> stubClass,
            String inPath,
            String delimiter) {

        DelimitedInputFormat<Record> format;
        try {
            format = stubClass.newInstance();
        } catch (Throwable t) {
            throw new RuntimeException("Could not instantiate test input format.", t);
        }

        format.setFilePath(inPath);
        format.setDelimiter(delimiter);

        OperatorID operatorID = new OperatorID();
        new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader())
                .addInputFormat(operatorID, format)
                .write(new TaskConfig(this.mockEnv.getTaskConfiguration()));

        this.inputSplitProvider.addInputSplits(inPath, 5);
    }

    protected MemoryManager getMemoryManager() {
        return this.mockEnv.getMemoryManager();
    }

    @AfterEach
    void shutdown() throws Exception {
        mockEnv.close();
    }
}
