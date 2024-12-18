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

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Utilities for testing collecting mechanism. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class CollectTestUtils {

    public static <T> List<byte[]> toBytesList(List<T> values, TypeSerializer<T> serializer) {
        List<byte[]> ret = new ArrayList<>();
        for (T value : values) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
            try {
                serializer.serialize(value, wrapper);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ret.add(baos.toByteArray());
        }
        return ret;
    }

    public static <T> void assertResponseEquals(
            CollectCoordinationResponse response,
            String version,
            long lastCheckpointedOffset,
            List<T> expected,
            TypeSerializer<T> serializer)
            throws IOException {
        assertThat(response.getVersion()).isEqualTo(version);
        assertThat(response.getLastCheckpointedOffset()).isEqualTo(lastCheckpointedOffset);
        List<T> results = response.getResults(serializer);
        assertThat(results).isEqualTo(expected);
    }

    public static <T> void assertAccumulatorResult(
            Tuple2<Long, CollectCoordinationResponse> accResults,
            long expectedOffset,
            String expectedVersion,
            long expectedLastCheckpointedOffset,
            List<T> expectedResults,
            TypeSerializer<T> serializer)
            throws Exception {
        long offset = accResults.f0;
        CollectCoordinationResponse response = accResults.f1;
        List<T> actualResults = response.getResults(serializer);

        assertThat(offset).isEqualTo(expectedOffset);
        assertThat(response.getVersion()).isEqualTo(expectedVersion);
        assertThat(response.getLastCheckpointedOffset()).isEqualTo(expectedLastCheckpointedOffset);
        assertThat(actualResults).isEqualTo(expectedResults);
    }
}
