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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TieredStorageIdMappingUtils}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TieredStorageIdMappingUtilsTest {

    @Test
    void testConvertDataSetId() {
        IntermediateDataSetID dataSetID = new IntermediateDataSetID();
        TieredStorageTopicId topicId = TieredStorageIdMappingUtils.convertId(dataSetID);
        IntermediateDataSetID convertedDataSetID = TieredStorageIdMappingUtils.convertId(topicId);
        assertThat(dataSetID).isEqualTo(convertedDataSetID);
    }

    @Test
    void testConvertResultPartitionId() {
        ResultPartitionID resultPartitionID = new ResultPartitionID();
        TieredStoragePartitionId tieredStoragePartitionId =
                TieredStorageIdMappingUtils.convertId(resultPartitionID);
        ResultPartitionID convertedResultPartitionID =
                TieredStorageIdMappingUtils.convertId(tieredStoragePartitionId);
        assertThat(resultPartitionID).isEqualTo(convertedResultPartitionID);
    }

    @Test
    void testConvertSubpartitionId() {
        int subpartitionId = 2;
        TieredStorageSubpartitionId tieredStorageSubpartitionId =
                TieredStorageIdMappingUtils.convertId(subpartitionId);
        int convertedSubpartitionId =
                TieredStorageIdMappingUtils.convertId(tieredStorageSubpartitionId);
        assertThat(subpartitionId).isEqualTo(convertedSubpartitionId);
    }
}
