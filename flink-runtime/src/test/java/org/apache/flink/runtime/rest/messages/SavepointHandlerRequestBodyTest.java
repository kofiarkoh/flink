/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.stop.StopWithSavepointRequestBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the savepoint request bodies. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class SavepointHandlerRequestBodyTest {

    @Test
    void testSavepointRequestCanBeParsedFromEmptyObject() throws JsonProcessingException {
        final SavepointTriggerRequestBody defaultParseResult =
                getDefaultParseResult(SavepointTriggerRequestBody.class);

        assertThat(defaultParseResult.isCancelJob()).isFalse();

        assertThat(defaultParseResult.getTargetDirectory()).isNotPresent();
    }

    @Test
    void testStopWithSavepointRequestCanBeParsedFromEmptyObject() throws JsonProcessingException {
        final StopWithSavepointRequestBody defaultParseResult =
                getDefaultParseResult(StopWithSavepointRequestBody.class);

        assertThat(defaultParseResult.shouldDrain()).isFalse();

        assertThat(defaultParseResult.getTargetDirectory()).isNotPresent();
    }

    private static <T> T getDefaultParseResult(Class<T> clazz) throws JsonProcessingException {
        final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        return mapper.readValue("{}", clazz);
    }
}
