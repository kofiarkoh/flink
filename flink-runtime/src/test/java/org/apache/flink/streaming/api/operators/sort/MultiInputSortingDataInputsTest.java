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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.sort.MultiInputSortingDataInput.SelectableSortingInputs;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MultiInputSortingDataInput}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class MultiInputSortingDataInputsTest {

    @Test
    void passThroughThenSortedInput() throws Exception {
        twoInputOrderTest(1, 0);
    }

    @Test
    void sortedThenPassThroughInput() throws Exception {
        twoInputOrderTest(0, 1);
    }

    @SuppressWarnings("unchecked")
    void twoInputOrderTest(int preferredIndex, int sortedIndex) throws Exception {
        CollectingDataOutput<Object> collectingDataOutput = new CollectingDataOutput<>();

        List<StreamElement> sortedInputElements =
                Arrays.asList(
                        new StreamRecord<>(1, 3),
                        new StreamRecord<>(1, 1),
                        new StreamRecord<>(2, 1),
                        new StreamRecord<>(2, 3),
                        new StreamRecord<>(1, 2),
                        new StreamRecord<>(2, 2),
                        Watermark.MAX_WATERMARK);
        CollectionDataInput<Integer> sortedInput =
                new CollectionDataInput<>(sortedInputElements, sortedIndex);

        List<StreamElement> preferredInputElements =
                Arrays.asList(
                        new StreamRecord<>(99, 3), new StreamRecord<>(99, 1), new Watermark(99L));
        CollectionDataInput<Integer> preferredInput =
                new CollectionDataInput<>(preferredInputElements, preferredIndex);

        KeySelector<Integer, Integer> keySelector = value -> value;

        try (MockEnvironment environment = MockEnvironment.builder().build()) {
            SelectableSortingInputs selectableSortingInputs =
                    MultiInputSortingDataInput.wrapInputs(
                            new DummyInvokable(),
                            new StreamTaskInput[] {sortedInput},
                            new KeySelector[] {keySelector},
                            new TypeSerializer[] {new IntSerializer()},
                            new IntSerializer(),
                            new StreamTaskInput[] {preferredInput},
                            environment.getMemoryManager(),
                            environment.getIOManager(),
                            true,
                            1.0,
                            new Configuration(),
                            new ExecutionConfig());

            StreamTaskInput<?>[] sortingDataInputs = selectableSortingInputs.getSortedInputs();
            StreamTaskInput<?>[] preferredDataInputs =
                    selectableSortingInputs.getPassThroughInputs();

            try (StreamTaskInput<Object> preferredTaskInput =
                            (StreamTaskInput<Object>) preferredDataInputs[0];
                    StreamTaskInput<Object> sortedTaskInput =
                            (StreamTaskInput<Object>) sortingDataInputs[0]) {

                MultipleInputSelectionHandler selectionHandler =
                        new MultipleInputSelectionHandler(
                                selectableSortingInputs.getInputSelectable(), 2);

                @SuppressWarnings("rawtypes")
                StreamOneInputProcessor[] inputProcessors = new StreamOneInputProcessor[2];
                inputProcessors[preferredIndex] =
                        new StreamOneInputProcessor<>(
                                preferredTaskInput, collectingDataOutput, new DummyOperatorChain());

                inputProcessors[sortedIndex] =
                        new StreamOneInputProcessor<>(
                                sortedTaskInput, collectingDataOutput, new DummyOperatorChain());

                StreamMultipleInputProcessor processor =
                        new StreamMultipleInputProcessor(selectionHandler, inputProcessors);

                DataInputStatus inputStatus;
                do {
                    inputStatus = processor.processInput();
                } while (inputStatus != DataInputStatus.END_OF_INPUT);
            }
        }

        assertThat(collectingDataOutput.events)
                .containsExactly(
                        new StreamRecord<>(99, 3),
                        new StreamRecord<>(99, 1),
                        // max watermark from the preferred input
                        new Watermark(99L),
                        new StreamRecord<>(1, 1),
                        new StreamRecord<>(1, 2),
                        new StreamRecord<>(1, 3),
                        new StreamRecord<>(2, 1),
                        new StreamRecord<>(2, 2),
                        new StreamRecord<>(2, 3),
                        // max watermark from the sorted input
                        Watermark.MAX_WATERMARK);
    }

    @Test
    @SuppressWarnings("unchecked")
    void simpleFixedLengthKeySorting() throws Exception {
        CollectingDataOutput<Object> collectingDataOutput = new CollectingDataOutput<>();
        List<StreamElement> elements =
                Arrays.asList(
                        new StreamRecord<>(1, 3),
                        new StreamRecord<>(1, 1),
                        new StreamRecord<>(2, 1),
                        new StreamRecord<>(2, 3),
                        new StreamRecord<>(1, 2),
                        new StreamRecord<>(2, 2),
                        Watermark.MAX_WATERMARK);
        CollectionDataInput<Integer> dataInput1 = new CollectionDataInput<>(elements, 0);
        CollectionDataInput<Integer> dataInput2 = new CollectionDataInput<>(elements, 1);
        KeySelector<Integer, Integer> keySelector = value -> value;
        try (MockEnvironment environment = MockEnvironment.builder().build()) {
            SelectableSortingInputs selectableSortingInputs =
                    MultiInputSortingDataInput.wrapInputs(
                            new DummyInvokable(),
                            new StreamTaskInput[] {dataInput1, dataInput2},
                            new KeySelector[] {keySelector, keySelector},
                            new TypeSerializer[] {new IntSerializer(), new IntSerializer()},
                            new IntSerializer(),
                            new StreamTaskInput[0],
                            environment.getMemoryManager(),
                            environment.getIOManager(),
                            true,
                            1.0,
                            new Configuration(),
                            new ExecutionConfig());

            StreamTaskInput<?>[] sortingDataInputs = selectableSortingInputs.getSortedInputs();
            try (StreamTaskInput<Object> input1 = (StreamTaskInput<Object>) sortingDataInputs[0];
                    StreamTaskInput<Object> input2 =
                            (StreamTaskInput<Object>) sortingDataInputs[1]) {

                MultipleInputSelectionHandler selectionHandler =
                        new MultipleInputSelectionHandler(
                                selectableSortingInputs.getInputSelectable(), 2);
                StreamMultipleInputProcessor processor =
                        new StreamMultipleInputProcessor(
                                selectionHandler,
                                new StreamOneInputProcessor[] {
                                    new StreamOneInputProcessor<>(
                                            input1, collectingDataOutput, new DummyOperatorChain()),
                                    new StreamOneInputProcessor<>(
                                            input2, collectingDataOutput, new DummyOperatorChain())
                                });

                DataInputStatus inputStatus;
                do {
                    inputStatus = processor.processInput();
                } while (inputStatus != DataInputStatus.END_OF_INPUT);
            }
        }

        assertThat(collectingDataOutput.events)
                .containsExactly(
                        new StreamRecord<>(1, 1),
                        new StreamRecord<>(1, 1),
                        new StreamRecord<>(1, 2),
                        new StreamRecord<>(1, 2),
                        new StreamRecord<>(1, 3),
                        new StreamRecord<>(1, 3),
                        new StreamRecord<>(2, 1),
                        new StreamRecord<>(2, 1),
                        new StreamRecord<>(2, 2),
                        new StreamRecord<>(2, 2),
                        new StreamRecord<>(2, 3),
                        // max watermark from one of the inputs
                        Watermark.MAX_WATERMARK,
                        new StreamRecord<>(2, 3),
                        // max watermark from the other input
                        Watermark.MAX_WATERMARK);
    }

    @Test
    @SuppressWarnings("unchecked")
    void watermarkPropagation() throws Exception {
        CollectingDataOutput<Object> collectingDataOutput = new CollectingDataOutput<>();
        List<StreamElement> elements1 =
                Arrays.asList(
                        new StreamRecord<>(2, 3),
                        new Watermark(3),
                        new StreamRecord<>(3, 3),
                        new Watermark(7));
        List<StreamElement> elements2 =
                Arrays.asList(
                        new StreamRecord<>(0, 3),
                        new Watermark(1),
                        new StreamRecord<>(1, 3),
                        new Watermark(3));
        CollectionDataInput<Integer> dataInput1 = new CollectionDataInput<>(elements1, 0);
        CollectionDataInput<Integer> dataInput2 = new CollectionDataInput<>(elements2, 1);
        KeySelector<Integer, Integer> keySelector = value -> value;
        try (MockEnvironment environment = MockEnvironment.builder().build()) {
            SelectableSortingInputs selectableSortingInputs =
                    MultiInputSortingDataInput.wrapInputs(
                            new DummyInvokable(),
                            new StreamTaskInput[] {dataInput1, dataInput2},
                            new KeySelector[] {keySelector, keySelector},
                            new TypeSerializer[] {new IntSerializer(), new IntSerializer()},
                            new IntSerializer(),
                            new StreamTaskInput[0],
                            environment.getMemoryManager(),
                            environment.getIOManager(),
                            true,
                            1.0,
                            new Configuration(),
                            new ExecutionConfig());

            StreamTaskInput<?>[] sortingDataInputs = selectableSortingInputs.getSortedInputs();
            try (StreamTaskInput<Object> input1 = (StreamTaskInput<Object>) sortingDataInputs[0];
                    StreamTaskInput<Object> input2 =
                            (StreamTaskInput<Object>) sortingDataInputs[1]) {

                MultipleInputSelectionHandler selectionHandler =
                        new MultipleInputSelectionHandler(
                                selectableSortingInputs.getInputSelectable(), 2);
                StreamMultipleInputProcessor processor =
                        new StreamMultipleInputProcessor(
                                selectionHandler,
                                new StreamOneInputProcessor[] {
                                    new StreamOneInputProcessor<>(
                                            input1, collectingDataOutput, new DummyOperatorChain()),
                                    new StreamOneInputProcessor<>(
                                            input2, collectingDataOutput, new DummyOperatorChain())
                                });

                DataInputStatus inputStatus;
                do {
                    inputStatus = processor.processInput();
                } while (inputStatus != DataInputStatus.END_OF_INPUT);
            }
        }

        assertThat(collectingDataOutput.events)
                .containsExactly(
                        new StreamRecord<>(0, 3),
                        new StreamRecord<>(1, 3),
                        // watermark from the second input
                        new Watermark(3),
                        new StreamRecord<>(2, 3),
                        new StreamRecord<>(3, 3),
                        // watermark from the first input
                        new Watermark(7));
    }

    private static class DummyOperatorChain implements BoundedMultiInput {
        @Override
        public void endInput(int inputId) {}
    }
}
