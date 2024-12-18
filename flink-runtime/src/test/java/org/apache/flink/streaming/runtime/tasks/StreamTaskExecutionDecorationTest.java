/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link StreamTask} {@link StreamTaskActionExecutor decorates execution} of actions
 * that potentially needs to be synchronized.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StreamTaskExecutionDecorationTest {
    private CountingStreamTaskActionExecutor decorator;
    private StreamTask<Object, StreamOperator<Object>> task;
    private TaskMailboxImpl mailbox;

    @Test
    void testAbortCheckpointOnBarrierIsDecorated() throws Exception {
        task.abortCheckpointOnBarrier(
                1,
                new CheckpointException(
                        CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
        assertThat(decorator.wasCalled()).as("execution decorator was not called").isTrue();
    }

    @Test
    void testTriggerCheckpointOnBarrierIsDecorated() throws Exception {
        task.triggerCheckpointOnBarrier(
                new CheckpointMetaData(1, 2),
                new CheckpointOptions(
                        CheckpointType.CHECKPOINT,
                        new CheckpointStorageLocationReference(new byte[] {1})),
                null);
        assertThat(decorator.wasCalled()).as("execution decorator was not called").isTrue();
    }

    @Test
    void testTriggerCheckpointAsyncIsDecorated() {
        task.triggerCheckpointAsync(
                new CheckpointMetaData(1, 2),
                new CheckpointOptions(
                        CheckpointType.CHECKPOINT,
                        new CheckpointStorageLocationReference(new byte[] {1})));
        assertThat(mailbox.hasMail()).as("mailbox is empty").isTrue();
        assertThat(decorator.wasCalled())
                .as("execution decorator was called preliminary")
                .isFalse();
        mailbox.drain()
                .forEach(
                        m -> {
                            try {
                                m.run();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        assertThat(decorator.wasCalled()).as("execution decorator was not called").isTrue();
    }

    @Test
    void testMailboxExecutorIsDecorated() throws Exception {
        task.mailboxProcessor
                .getMainMailboxExecutor()
                .execute(() -> task.mailboxProcessor.allActionsCompleted(), "");
        task.mailboxProcessor.runMailboxLoop();
        assertThat(decorator.wasCalled()).as("execution decorator was not called").isTrue();
    }

    @BeforeEach
    void before() throws Exception {
        mailbox = new TaskMailboxImpl();
        decorator = new CountingStreamTaskActionExecutor();
        task =
                new StreamTask<Object, StreamOperator<Object>>(
                        new DeclineDummyEnvironment(),
                        null,
                        FatalExitExceptionHandler.INSTANCE,
                        decorator,
                        mailbox) {
                    @Override
                    protected void init() {}

                    @Override
                    protected void processInput(MailboxDefaultAction.Controller controller) {}
                };
        task.operatorChain = new RegularOperatorChain<>(task, new NonRecordWriter<>());
    }

    @AfterEach
    void after() {
        decorator = null;
        task = null;
    }

    static class CountingStreamTaskActionExecutor
            extends StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor {
        private final AtomicInteger calls = new AtomicInteger(0);

        CountingStreamTaskActionExecutor() {
            super(new Object());
        }

        int getCallCount() {
            return calls.get();
        }

        boolean wasCalled() {
            return getCallCount() > 0;
        }

        @Override
        public void run(RunnableWithException runnable) throws Exception {
            calls.incrementAndGet();
            runnable.run();
        }

        @Override
        public <E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) throws E {
            calls.incrementAndGet();
            runnable.run();
        }

        @Override
        public <R> R call(Callable<R> callable) throws Exception {
            calls.incrementAndGet();
            return callable.call();
        }
    }

    private static final class DeclineDummyEnvironment extends DummyEnvironment {

        DeclineDummyEnvironment() {
            super("test", 1, 0);
        }

        @Override
        public void declineCheckpoint(long checkpointId, CheckpointException cause) {}
    }
}
