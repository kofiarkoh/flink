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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.streaming.api.functions.sink.legacy.OutputFormatSinkFunction;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/** Tests for {@link OutputFormatSinkFunction}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class OutputFormatSinkFunctionTest {

    @Test
    void setRuntimeContext() {
        RuntimeContext mockRuntimeContext = Mockito.mock(RuntimeContext.class);

        // Make sure setRuntimeContext of the rich output format is called
        RichOutputFormat<?> mockRichOutputFormat = Mockito.mock(RichOutputFormat.class);
        new OutputFormatSinkFunction<>(mockRichOutputFormat).setRuntimeContext(mockRuntimeContext);
        Mockito.verify(mockRichOutputFormat, Mockito.times(1))
                .setRuntimeContext(Mockito.eq(mockRuntimeContext));

        // Make sure setRuntimeContext work well when output format is not RichOutputFormat
        OutputFormat<?> mockOutputFormat = Mockito.mock(OutputFormat.class);
        new OutputFormatSinkFunction<>(mockOutputFormat).setRuntimeContext(mockRuntimeContext);
    }
}
