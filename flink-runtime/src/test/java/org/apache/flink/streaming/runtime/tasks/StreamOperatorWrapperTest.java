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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StreamOperatorWrapper}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StreamOperatorWrapperTest {

    private static SystemProcessingTimeService timerService;

    private static final int numOperators = 3;

    private List<StreamOperatorWrapper<?, ?>> operatorWrappers;

    private ConcurrentLinkedQueue<Object> output;

    private volatile StreamTask<?, ?> containingTask;

    @BeforeAll
    static void startTimeService() {
        CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();
        timerService = new SystemProcessingTimeService(errorFuture::complete);
    }

    @AfterAll
    static void shutdownTimeService() {
        timerService.shutdownService();
    }

    @BeforeEach
    void setup() throws Exception {
        this.operatorWrappers = new ArrayList<>();
        this.output = new ConcurrentLinkedQueue<>();

        try (MockEnvironment env = MockEnvironment.builder().build()) {
            this.containingTask = new MockStreamTaskBuilder(env).build();

            // initialize operator wrappers
            for (int i = 0; i < numOperators; i++) {
                MailboxExecutor mailboxExecutor =
                        containingTask.getMailboxExecutorFactory().createExecutor(i);

                TimerMailController timerMailController =
                        new TimerMailController(containingTask, mailboxExecutor);
                ProcessingTimeServiceImpl processingTimeService =
                        new ProcessingTimeServiceImpl(
                                timerService, timerMailController::wrapCallback);

                TestOneInputStreamOperator streamOperator =
                        new TestOneInputStreamOperator(
                                "Operator" + i,
                                output,
                                processingTimeService,
                                mailboxExecutor,
                                timerMailController);

                StreamOperatorWrapper<?, ?> operatorWrapper =
                        new StreamOperatorWrapper<>(
                                streamOperator,
                                Optional.ofNullable(streamOperator.getProcessingTimeService()),
                                mailboxExecutor,
                                i == 0);
                operatorWrappers.add(operatorWrapper);
            }

            StreamOperatorWrapper<?, ?> previous = null;
            for (StreamOperatorWrapper<?, ?> current : operatorWrappers) {
                if (previous != null) {
                    previous.setNext(current);
                }
                current.setPrevious(previous);
                previous = current;
            }
        }
    }

    @AfterEach
    void teardown() throws Exception {
        containingTask.cleanUpInternal();
    }

    @Test
    void testFinish() throws Exception {
        output.clear();
        operatorWrappers.get(0).finish(containingTask.getActionExecutor(), StopMode.DRAIN);

        List<Object> expected = new ArrayList<>();
        for (int i = 0; i < operatorWrappers.size(); i++) {
            String prefix = "[" + "Operator" + i + "]";
            Collections.addAll(
                    expected,
                    prefix + ": End of input",
                    prefix + ": Timer that was in mailbox before closing operator",
                    prefix + ": Bye",
                    prefix + ": Mail to put in mailbox when finishing operator");
        }

        assertThat(output)
                .as("Output was not correct.")
                .containsExactlyElementsOf(expected.subList(2, expected.size()));
    }

    @Test
    void testFinishingOperatorWithException() {
        AbstractStreamOperator<Void> streamOperator =
                new AbstractStreamOperator<Void>() {
                    @Override
                    public void finish() throws Exception {
                        throw new Exception("test exception at finishing");
                    }
                };

        StreamOperatorWrapper<?, ?> operatorWrapper =
                new StreamOperatorWrapper<>(
                        streamOperator,
                        Optional.ofNullable(streamOperator.getProcessingTimeService()),
                        containingTask
                                .getMailboxExecutorFactory()
                                .createExecutor(Integer.MAX_VALUE - 1),
                        true);

        assertThatThrownBy(
                        () ->
                                operatorWrapper.finish(
                                        containingTask.getActionExecutor(), StopMode.DRAIN))
                .hasMessageContaining("test exception at finishing");
    }

    @Test
    void testReadIterator() {
        // traverse operators in forward order
        Iterator<StreamOperatorWrapper<?, ?>> it =
                new StreamOperatorWrapper.ReadIterator(operatorWrappers.get(0), false);
        for (int i = 0; i < operatorWrappers.size(); i++) {
            assertThat(it).hasNext();

            StreamOperatorWrapper<?, ?> next = it.next();
            assertThat(next).isNotNull();

            TestOneInputStreamOperator operator = getStreamOperatorFromWrapper(next);
            assertThat(operator.getName()).isEqualTo("Operator" + i);
        }
        assertThat(it).isExhausted();

        // traverse operators in reverse order
        it =
                new StreamOperatorWrapper.ReadIterator(
                        operatorWrappers.get(operatorWrappers.size() - 1), true);
        for (int i = operatorWrappers.size() - 1; i >= 0; i--) {
            assertThat(it).hasNext();

            StreamOperatorWrapper<?, ?> next = it.next();
            assertThat(next).isNotNull();

            TestOneInputStreamOperator operator = getStreamOperatorFromWrapper(next);
            assertThat(operator.getName()).isEqualTo("Operator" + i);
        }
        assertThat(it).isExhausted();
    }

    private TestOneInputStreamOperator getStreamOperatorFromWrapper(
            StreamOperatorWrapper<?, ?> operatorWrapper) {
        return (TestOneInputStreamOperator)
                Objects.requireNonNull(operatorWrapper.getStreamOperator());
    }

    private static class TimerMailController {

        private final StreamTask<?, ?> containingTask;

        private final MailboxExecutor mailboxExecutor;

        private final ConcurrentHashMap<ProcessingTimeCallback, OneShotLatch> puttingLatches;

        private final ConcurrentHashMap<ProcessingTimeCallback, OneShotLatch> inMailboxLatches;

        TimerMailController(StreamTask<?, ?> containingTask, MailboxExecutor mailboxExecutor) {
            this.containingTask = containingTask;
            this.mailboxExecutor = mailboxExecutor;

            this.puttingLatches = new ConcurrentHashMap<>();
            this.inMailboxLatches = new ConcurrentHashMap<>();
        }

        OneShotLatch getPuttingLatch(ProcessingTimeCallback callback) {
            return puttingLatches.get(callback);
        }

        OneShotLatch getInMailboxLatch(ProcessingTimeCallback callback) {
            return inMailboxLatches.get(callback);
        }

        ProcessingTimeCallback wrapCallback(ProcessingTimeCallback callback) {
            puttingLatches.put(callback, new OneShotLatch());
            inMailboxLatches.put(callback, new OneShotLatch());

            return timestamp -> {
                puttingLatches.get(callback).trigger();
                containingTask
                        .deferCallbackToMailbox(mailboxExecutor, callback)
                        .onProcessingTime(timestamp);
                inMailboxLatches.get(callback).trigger();
            };
        }
    }

    private static class TestOneInputStreamOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String>, BoundedOneInput {

        private static final long serialVersionUID = 1L;

        private final String name;

        private final ConcurrentLinkedQueue<Object> output;

        private final ProcessingTimeService processingTimeService;

        private final MailboxExecutor mailboxExecutor;

        private final TimerMailController timerMailController;

        TestOneInputStreamOperator(
                String name,
                ConcurrentLinkedQueue<Object> output,
                ProcessingTimeService processingTimeService,
                MailboxExecutor mailboxExecutor,
                TimerMailController timerMailController) {

            this.name = name;
            this.output = output;
            this.processingTimeService = processingTimeService;
            this.mailboxExecutor = mailboxExecutor;
            this.timerMailController = timerMailController;

            processingTimeService.registerTimer(
                    Long.MAX_VALUE, t2 -> output.add("[" + name + "]: Timer not triggered"));
            super.setProcessingTimeService(processingTimeService);
        }

        public String getName() {
            return name;
        }

        @Override
        public void processElement(StreamRecord<String> element) {}

        @Override
        public void endInput() throws InterruptedException {
            output.add("[" + name + "]: End of input");

            ProcessingTimeCallback callback =
                    t1 ->
                            output.add(
                                    "["
                                            + name
                                            + "]: Timer that was in mailbox before closing operator");
            processingTimeService.registerTimer(0, callback);
            timerMailController.getInMailboxLatch(callback).await();
        }

        @Override
        public void finish() throws Exception {
            ProcessingTimeCallback callback =
                    t1 ->
                            output.add(
                                    "["
                                            + name
                                            + "]: Timer to put in mailbox when finishing operator");
            assertThat((Future<?>) processingTimeService.registerTimer(0, callback)).isNotNull();
            assertThat(timerMailController.getPuttingLatch(callback)).isNull();

            mailboxExecutor.execute(
                    () ->
                            output.add(
                                    "["
                                            + name
                                            + "]: Mail to put in mailbox when finishing operator"),
                    "");

            output.add("[" + name + "]: Bye");
        }
    }
}
