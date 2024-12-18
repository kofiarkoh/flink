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

package org.apache.flink.runtime.rest.messages.json;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JobResultDeserializer}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class JobResultDeserializerTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(JobResult.class, new JobResultDeserializer());

        objectMapper = JacksonMapperFactory.createObjectMapper();
        objectMapper.registerModule(simpleModule);
    }

    @Test
    void testDeserialization() throws Exception {
        final JobResult jobResult =
                objectMapper.readValue(
                        "{\n"
                                + "\t\"id\": \"1bb5e8c7df49938733b7c6a73678de6a\",\n"
                                + "\t\"accumulator-results\": {},\n"
                                + "\t\"net-runtime\": 0,\n"
                                + "\t\"unknownfield\": \"foobar\"\n"
                                + "}",
                        JobResult.class);

        assertThat(jobResult.getJobId())
                .isEqualTo(JobID.fromHexString("1bb5e8c7df49938733b7c6a73678de6a"));
        assertThat(jobResult.getNetRuntime()).isZero();
        assertThat(jobResult.getAccumulatorResults().size()).isZero();
        assertThat(jobResult.getSerializedThrowable()).isNotPresent();
    }

    @Test
    void testInvalidType() {
        assertThatThrownBy(
                        () ->
                                objectMapper.readValue(
                                        "{\n"
                                                + "\t\"id\": \"1bb5e8c7df49938733b7c6a73678de6a\",\n"
                                                + "\t\"net-runtime\": \"invalid\"\n"
                                                + "}",
                                        JobResult.class))
                .isInstanceOf(JsonMappingException.class)
                .hasMessageContaining("Expected token VALUE_NUMBER_INT (was VALUE_STRING)");
    }

    @Test
    void testIncompleteJobResult() throws Exception {
        assertThatThrownBy(
                        () ->
                                objectMapper.readValue(
                                        "{\n"
                                                + "\t\"id\": \"1bb5e8c7df49938733b7c6a73678de6a\"\n"
                                                + "}",
                                        JobResult.class))
                .isInstanceOf(JsonMappingException.class)
                .hasMessageContaining("Could not deserialize JobResult");
    }
}
