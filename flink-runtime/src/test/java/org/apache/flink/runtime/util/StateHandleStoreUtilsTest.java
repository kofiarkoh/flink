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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.function.RunnableWithException;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code StateHandleStoreUtilsTest} tests the utility classes collected in {@link
 * StateHandleStoreUtils}.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StateHandleStoreUtilsTest {

    @Test
    void testSerializationAndDeserialization() throws Exception {
        final TestingLongStateHandleHelper.LongStateHandle original =
                new TestingLongStateHandleHelper.LongStateHandle(42L);
        byte[] serializedData = StateHandleStoreUtils.serializeOrDiscard(original);

        final TestingLongStateHandleHelper.LongStateHandle deserializedInstance =
                StateHandleStoreUtils.deserialize(serializedData);
        assertThat(deserializedInstance.getStateSize()).isEqualTo(original.getStateSize());
        assertThat(deserializedInstance.getValue()).isEqualTo(original.getValue());
    }

    @Test
    void testSerializeOrDiscardFailureHandling() throws Exception {
        final AtomicBoolean discardCalled = new AtomicBoolean(false);
        final StateObject original =
                new FailingSerializationStateObject(() -> discardCalled.set(true));

        assertThatThrownBy(() -> StateHandleStoreUtils.serializeOrDiscard(original))
                .withFailMessage("An IOException is expected to be thrown.")
                .isInstanceOf(IOException.class);

        assertThat(discardCalled).isTrue();
    }

    @Test
    void testSerializationOrDiscardWithDiscardFailure() throws Exception {
        final Exception discardException =
                new IllegalStateException(
                        "Expected IllegalStateException that should be suppressed.");
        final StateObject original =
                new FailingSerializationStateObject(
                        () -> {
                            throw discardException;
                        });

        assertThatThrownBy(() -> StateHandleStoreUtils.serializeOrDiscard(original))
                .withFailMessage("An IOException is expected to be thrown.")
                .isInstanceOf(IOException.class)
                .satisfies(
                        e -> {
                            assertThat(e.getSuppressed()).hasSize(1);
                            assertThat(e.getSuppressed()[0]).isEqualTo(discardException);
                        });
    }

    private static class FailingSerializationStateObject implements StateObject {

        private static final long serialVersionUID = 6382458109061973983L;
        private final RunnableWithException discardStateRunnable;

        public FailingSerializationStateObject(RunnableWithException discardStateRunnable) {
            this.discardStateRunnable = discardStateRunnable;
        }

        private void writeObject(ObjectOutputStream outputStream) throws IOException {
            throw new IOException("Expected IOException to test serialization error.");
        }

        @Override
        public void discardState() throws Exception {
            discardStateRunnable.run();
        }

        @Override
        public long getStateSize() {
            return 0;
        }
    }
}
