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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TestingTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.TestingAvailabilityNotifier;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TieredStorageConsumerClient}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TieredStorageConsumerClientTest {

    private static final TieredStoragePartitionId DEFAULT_PARTITION_ID =
            TieredStorageIdMappingUtils.convertId(new ResultPartitionID());

    private static final TieredStorageSubpartitionId DEFAULT_SUBPARTITION_ID =
            new TieredStorageSubpartitionId(0);

    private static final TieredStorageInputChannelId DEFAULT_INPUT_CHANNEL_ID =
            new TieredStorageInputChannelId(0);

    private static final ResultSubpartitionIndexSet DEFAULT_SUBPARTITION_ID_SET =
            new ResultSubpartitionIndexSet(0);

    @Test
    void testStart() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TestingTierConsumerAgent tierConsumerAgent =
                new TestingTierConsumerAgent.Builder()
                        .setStartNotifier(() -> future.complete(null))
                        .build();
        TieredStorageConsumerClient consumerClient =
                createTieredStorageConsumerClient(tierConsumerAgent);
        consumerClient.start();
        assertThat(future).isDone();
    }

    @Test
    void testGetNextBuffer() throws IOException {
        Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer(0);
        TestingTierConsumerAgent tierConsumerAgent =
                new TestingTierConsumerAgent.Builder().setBufferSupplier(() -> buffer).build();
        TieredStorageConsumerClient consumerClient =
                createTieredStorageConsumerClient(tierConsumerAgent);
        assertThat(consumerClient.getNextBuffer(DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID))
                .hasValue(buffer);
    }

    @Test
    void testRegisterAvailabilityNotifier() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TestingTierConsumerAgent tierConsumerAgent =
                new TestingTierConsumerAgent.Builder()
                        .setAvailabilityNotifierRegistrationRunnable(() -> future.complete(null))
                        .build();
        TieredStorageConsumerClient consumerClient =
                createTieredStorageConsumerClient(tierConsumerAgent);
        consumerClient.registerAvailabilityNotifier(
                new TestingAvailabilityNotifier.Builder().build());
        assertThat(future).isDone();
    }

    @Test
    void testUpdateTierShuffleDescriptor() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TestingTierConsumerAgent tierConsumerAgent =
                new TestingTierConsumerAgent.Builder()
                        .setUpdateTierShuffleDescriptorRunnable(() -> future.complete(null))
                        .build();
        assertThat(future).isNotDone();
        TieredStorageConsumerClient consumerClient =
                createTieredStorageConsumerClient(tierConsumerAgent);
        consumerClient.updateTierShuffleDescriptors(
                DEFAULT_PARTITION_ID,
                DEFAULT_INPUT_CHANNEL_ID,
                DEFAULT_SUBPARTITION_ID,
                Collections.singletonList(
                        new TierShuffleDescriptor() {
                            private static final long serialVersionUID = 1L;
                        }));
        assertThat(future).isDone();
    }

    @Test
    void testClose() throws IOException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TestingTierConsumerAgent tierConsumerAgent =
                new TestingTierConsumerAgent.Builder()
                        .setCloseNotifier(() -> future.complete(null))
                        .build();
        TieredStorageConsumerClient consumerClient =
                createTieredStorageConsumerClient(tierConsumerAgent);
        consumerClient.close();
        assertThat(future).isDone();
    }

    private TieredStorageConsumerClient createTieredStorageConsumerClient(
            TierConsumerAgent tierConsumerAgent) {
        TierShuffleDescriptor emptyTierShuffleDescriptor =
                new TierShuffleDescriptor() {
                    private static final long serialVersionUID = 1L;
                };
        return new TieredStorageConsumerClient(
                Collections.singletonList(
                        new TestingTierFactory.Builder()
                                .setTierConsumerAgentSupplier(
                                        (tieredStorageConsumerSpecs, nettyService) ->
                                                tierConsumerAgent)
                                .build()),
                Collections.singletonList(
                        new TieredStorageConsumerSpec(
                                0,
                                DEFAULT_PARTITION_ID,
                                DEFAULT_INPUT_CHANNEL_ID,
                                DEFAULT_SUBPARTITION_ID_SET)),
                Collections.singletonList(Collections.singletonList(emptyTierShuffleDescriptor)),
                new TestingTieredStorageNettyService.Builder().build());
    }
}
