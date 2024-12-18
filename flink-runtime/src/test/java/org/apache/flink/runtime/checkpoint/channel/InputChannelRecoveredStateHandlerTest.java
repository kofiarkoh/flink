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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Test of different implementation of {@link InputChannelRecoveredStateHandler}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class InputChannelRecoveredStateHandlerTest extends RecoveredChannelStateHandlerTest {
    private static final int preAllocatedSegments = 3;
    private NetworkBufferPool networkBufferPool;
    private SingleInputGate inputGate;
    private InputChannelRecoveredStateHandler icsHandler;
    private InputChannelInfo channelInfo;

    @BeforeEach
    void setUp() {
        // given: Segment provider with defined number of allocated segments.
        networkBufferPool = new NetworkBufferPool(preAllocatedSegments, 1024);

        // and: Configured input gate with recovered channel.
        inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(networkBufferPool)
                        .build();

        icsHandler = buildInputChannelStateHandler(inputGate);

        channelInfo = new InputChannelInfo(0, 0);
    }

    private InputChannelRecoveredStateHandler buildInputChannelStateHandler(
            SingleInputGate inputGate) {
        return new InputChannelRecoveredStateHandler(
                new InputGate[] {inputGate},
                new InflightDataRescalingDescriptor(
                        new InflightDataRescalingDescriptor
                                        .InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {1},
                                    RescaleMappings.identity(1, 1),
                                    new HashSet<>(),
                                    InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor
                                            .MappingType.IDENTITY)
                        }));
    }

    @Test
    void testRecycleBufferBeforeRecoverWasCalled() throws Exception {
        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHandler.getBuffer(channelInfo);

        // and: Recycle buffer outside.
        bufferWithContext.buffer.close();

        // Close the gate for flushing the cached recycled buffers to the segment provider.
        inputGate.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(preAllocatedSegments);
    }

    @Test
    void testRecycleBufferAfterRecoverWasCalled() throws Exception {
        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHandler.getBuffer(channelInfo);

        // and: Recycle buffer outside.
        icsHandler.recover(channelInfo, 0, bufferWithContext);

        // Close the gate for flushing the cached recycled buffers to the segment provider.
        inputGate.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(preAllocatedSegments);
    }
}
