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

import org.apache.flink.core.execution.SavepointFormatType;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class RestoredCheckpointStatsTest {

    /** Tests simple access to restore properties. */
    @Test
    void testSimpleAccess() {
        long checkpointId = Integer.MAX_VALUE + 1L;
        CheckpointProperties props =
                new CheckpointProperties(
                        true,
                        SavepointType.savepoint(SavepointFormatType.CANONICAL),
                        false,
                        false,
                        true,
                        false,
                        true,
                        false);
        long restoreTimestamp = Integer.MAX_VALUE + 1L;
        String externalPath = "external-path";

        RestoredCheckpointStats restored =
                new RestoredCheckpointStats(
                        checkpointId, props, restoreTimestamp, externalPath, 42);

        assertThat(restored.getCheckpointId()).isEqualTo(checkpointId);
        assertThat(restored.getProperties()).isEqualTo(props);
        assertThat(restored.getRestoreTimestamp()).isEqualTo(restoreTimestamp);
        assertThat(restored.getExternalPath()).isEqualTo(externalPath);
        assertThat(restored.getStateSize()).isEqualTo(42);
    }
}
