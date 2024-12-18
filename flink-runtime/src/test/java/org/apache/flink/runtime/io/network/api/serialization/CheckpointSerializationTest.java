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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the {@link EventSerializer} functionality for serializing {@link CheckpointBarrier
 * checkpoint barriers}.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CheckpointSerializationTest {

    private static final byte[] STORAGE_LOCATION_REF =
            new byte[] {15, 52, 52, 11, 0, 0, 0, 0, -1, -23, -19, 35};

    @Test
    void testSuspendingCheckpointBarrierSerialization() throws Exception {
        CheckpointOptions suspendSavepointToSerialize =
                new CheckpointOptions(
                        SavepointType.suspend(SavepointFormatType.CANONICAL),
                        new CheckpointStorageLocationReference(STORAGE_LOCATION_REF));
        testCheckpointBarrierSerialization(suspendSavepointToSerialize);
    }

    @Test
    void testSavepointBarrierSerialization() throws Exception {
        CheckpointOptions savepointToSerialize =
                new CheckpointOptions(
                        SavepointType.savepoint(SavepointFormatType.CANONICAL),
                        new CheckpointStorageLocationReference(STORAGE_LOCATION_REF));
        testCheckpointBarrierSerialization(savepointToSerialize);
    }

    @Test
    void testCheckpointBarrierSerialization() throws Exception {
        CheckpointOptions checkpointToSerialize =
                new CheckpointOptions(
                        CheckpointType.CHECKPOINT,
                        new CheckpointStorageLocationReference(STORAGE_LOCATION_REF));
        testCheckpointBarrierSerialization(checkpointToSerialize);
    }

    @Test
    void testFullCheckpointBarrierSerialization() throws Exception {
        CheckpointOptions checkpointToSerialize =
                new CheckpointOptions(
                        CheckpointType.FULL_CHECKPOINT,
                        new CheckpointStorageLocationReference(STORAGE_LOCATION_REF));
        testCheckpointBarrierSerialization(checkpointToSerialize);
    }

    @Test
    void testCheckpointWithDefaultLocationSerialization() throws Exception {
        CheckpointOptions checkpointToSerialize =
                CheckpointOptions.forCheckpointWithDefaultLocation();
        testCheckpointBarrierSerialization(checkpointToSerialize);
    }

    private static void testCheckpointBarrierSerialization(CheckpointOptions options)
            throws IOException {
        final long checkpointId = Integer.MAX_VALUE + 123123L;
        final long timestamp = Integer.MAX_VALUE + 1228L;

        final CheckpointBarrier barrierBeforeSerialization =
                new CheckpointBarrier(checkpointId, timestamp, options);
        final CheckpointBarrier barrierAfterDeserialization =
                serializeAndDeserializeCheckpointBarrier(barrierBeforeSerialization);

        assertThat(barrierAfterDeserialization).isEqualTo(barrierBeforeSerialization);
    }

    private static CheckpointBarrier serializeAndDeserializeCheckpointBarrier(
            final CheckpointBarrier barrierUnderTest) throws IOException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final ByteBuffer serialized = EventSerializer.toSerializedEvent(barrierUnderTest);
        final CheckpointBarrier deserialized =
                (CheckpointBarrier) EventSerializer.fromSerializedEvent(serialized, cl);
        assertThat(serialized.hasRemaining()).isFalse();
        return deserialized;
    }
}
