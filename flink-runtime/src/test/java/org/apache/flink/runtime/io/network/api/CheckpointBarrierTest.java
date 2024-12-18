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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link CheckpointBarrier} type. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CheckpointBarrierTest {

    /**
     * Test serialization of the checkpoint barrier. The checkpoint barrier does not support its own
     * serialization, in order to be immutable.
     */
    @Test
    void testSerialization() {
        long id = Integer.MAX_VALUE + 123123L;
        long timestamp = Integer.MAX_VALUE + 1228L;

        CheckpointOptions options = CheckpointOptions.forCheckpointWithDefaultLocation();
        CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp, options);

        assertThatThrownBy(() -> barrier.write(new DataOutputSerializer(1024)))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> barrier.read(new DataInputDeserializer(new byte[32])))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
