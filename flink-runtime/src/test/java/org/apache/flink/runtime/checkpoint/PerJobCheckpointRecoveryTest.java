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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.util.concurrent.Executors;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests related to {@link PerJobCheckpointRecoveryFactory}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PerJobCheckpointRecoveryTest {

    @Test
    void testFactoryWithoutCheckpointStoreRecovery() throws Exception {
        final TestingCompletedCheckpointStore store =
                TestingCompletedCheckpointStore
                        .createStoreWithShutdownCheckAndNoCompletedCheckpoints(
                                new CompletableFuture<>());
        final CheckpointRecoveryFactory factory =
                PerJobCheckpointRecoveryFactory.withoutCheckpointStoreRecovery(
                        maxCheckpoints -> store);
        final JobID firstJobId = new JobID();
        assertThat(
                        factory.createRecoveredCompletedCheckpointStore(
                                firstJobId,
                                1,
                                SharedStateRegistry.DEFAULT_FACTORY,
                                Executors.directExecutor(),
                                RecoveryClaimMode.DEFAULT))
                .isSameAs(store);
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(
                        () ->
                                factory.createRecoveredCompletedCheckpointStore(
                                        firstJobId,
                                        1,
                                        SharedStateRegistry.DEFAULT_FACTORY,
                                        Executors.directExecutor(),
                                        RecoveryClaimMode.DEFAULT));

        final JobID secondJobId = new JobID();
        assertThat(
                        factory.createRecoveredCompletedCheckpointStore(
                                secondJobId,
                                1,
                                SharedStateRegistry.DEFAULT_FACTORY,
                                Executors.directExecutor(),
                                RecoveryClaimMode.DEFAULT))
                .isSameAs(store);
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(
                        () ->
                                factory.createRecoveredCompletedCheckpointStore(
                                        secondJobId,
                                        1,
                                        SharedStateRegistry.DEFAULT_FACTORY,
                                        Executors.directExecutor(),
                                        RecoveryClaimMode.DEFAULT));
    }
}
