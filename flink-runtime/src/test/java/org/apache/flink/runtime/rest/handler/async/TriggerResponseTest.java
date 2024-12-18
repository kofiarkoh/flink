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

package org.apache.flink.runtime.rest.handler.async;

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Marshalling test for {@link TriggerResponse}. */
@ExtendWith(NoOpTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TriggerResponseTest extends RestResponseMarshallingTestBase<TriggerResponse> {

    @Override
    protected Class<TriggerResponse> getTestResponseClass() {
        return TriggerResponse.class;
    }

    @Override
    protected TriggerResponse getTestResponseInstance() throws Exception {
        return new TriggerResponse(new TriggerId());
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            TriggerResponse expected, TriggerResponse actual) {
        assertThat(actual.getTriggerId()).isEqualTo(expected.getTriggerId());
    }
}
