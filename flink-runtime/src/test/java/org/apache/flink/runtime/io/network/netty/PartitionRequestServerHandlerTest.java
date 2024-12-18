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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ResumeConsumption;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionRequestServerHandler}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PartitionRequestServerHandlerTest {

    @Test
    void testResumeConsumption() {
        final InputChannelID inputChannelID = new InputChannelID();
        final PartitionRequestQueue partitionRequestQueue = new PartitionRequestQueue();
        final TestViewReader testViewReader =
                new TestViewReader(inputChannelID, 2, partitionRequestQueue);
        final PartitionRequestServerHandler serverHandler =
                new PartitionRequestServerHandler(
                        new ResultPartitionManager(),
                        new TaskEventDispatcher(),
                        partitionRequestQueue);
        final EmbeddedChannel channel = new EmbeddedChannel(serverHandler);
        partitionRequestQueue.notifyReaderCreated(testViewReader);

        // Write the message of resume consumption to server
        channel.writeInbound(new ResumeConsumption(inputChannelID));
        channel.runPendingTasks();

        assertThat(testViewReader.consumptionResumed).isTrue();
    }

    @Test
    void testAcknowledgeAllRecordsProcessed() throws IOException {
        InputChannelID inputChannelID = new InputChannelID();

        ResultPartition resultPartition =
                PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);

        // Creates the netty network handler stack.
        PartitionRequestQueue partitionRequestQueue = new PartitionRequestQueue();
        final PartitionRequestServerHandler serverHandler =
                new PartitionRequestServerHandler(
                        new ResultPartitionManager(),
                        new TaskEventDispatcher(),
                        partitionRequestQueue);
        final EmbeddedChannel channel = new EmbeddedChannel(serverHandler, partitionRequestQueue);

        // Creates and registers the view to netty.
        NetworkSequenceViewReader viewReader =
                new CreditBasedSequenceNumberingViewReader(
                        inputChannelID, 2, partitionRequestQueue);
        viewReader.notifySubpartitionsCreated(resultPartition, new ResultSubpartitionIndexSet(0));
        partitionRequestQueue.notifyReaderCreated(viewReader);

        // Write the message to acknowledge all records are processed to server
        resultPartition.notifyEndOfData(StopMode.DRAIN);
        CompletableFuture<Void> allRecordsProcessedFuture =
                resultPartition.getAllDataProcessedFuture();
        assertThat(allRecordsProcessedFuture).isNotDone();
        channel.writeInbound(new NettyMessage.AckAllUserRecordsProcessed(inputChannelID));
        channel.runPendingTasks();

        assertThat(allRecordsProcessedFuture).isDone().isNotCompletedExceptionally();
    }

    @Test
    public void testNewBufferSize() {
        final InputChannelID inputChannelID = new InputChannelID();
        final PartitionRequestQueue partitionRequestQueue = new PartitionRequestQueue();
        final TestViewReader testViewReader =
                new TestViewReader(inputChannelID, 2, partitionRequestQueue);
        final PartitionRequestServerHandler serverHandler =
                new PartitionRequestServerHandler(
                        new ResultPartitionManager(),
                        new TaskEventDispatcher(),
                        partitionRequestQueue);
        final EmbeddedChannel channel = new EmbeddedChannel(serverHandler);
        partitionRequestQueue.notifyReaderCreated(testViewReader);

        // Write the message of new buffer size to server
        channel.writeInbound(new NettyMessage.NewBufferSize(666, inputChannelID));
        channel.runPendingTasks();

        assertThat(testViewReader.bufferSize).isEqualTo(666);
    }

    @Test
    void testReceivingNewBufferSizeBeforeReaderIsCreated() {
        final InputChannelID inputChannelID = new InputChannelID();
        final PartitionRequestQueue partitionRequestQueue = new PartitionRequestQueue();
        final TestViewReader testViewReader =
                new TestViewReader(inputChannelID, 2, partitionRequestQueue);
        final PartitionRequestServerHandler serverHandler =
                new PartitionRequestServerHandler(
                        new ResultPartitionManager(),
                        new TaskEventDispatcher(),
                        partitionRequestQueue);
        final EmbeddedChannel channel = new EmbeddedChannel(serverHandler);

        // Write the message of new buffer size to server without prepared reader.
        channel.writeInbound(new NettyMessage.NewBufferSize(666, inputChannelID));
        channel.runPendingTasks();

        // If error happens outbound messages would be not empty.
        assertThat(channel.outboundMessages())
                .withFailMessage(channel.outboundMessages().toString())
                .isEmpty();

        // New buffer size should be silently ignored because it is possible situation.
        assertThat(testViewReader.bufferSize).isEqualTo(-1);
    }

    private static class TestViewReader extends CreditBasedSequenceNumberingViewReader {
        private boolean consumptionResumed = false;
        private int bufferSize = -1;

        TestViewReader(
                InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {
            super(receiverId, initialCredit, requestQueue);
        }

        @Override
        public void resumeConsumption() {
            consumptionResumed = true;
        }

        @Override
        public void notifyNewBufferSize(int newBufferSize) {
            bufferSize = newBufferSize;
        }
    }
}
