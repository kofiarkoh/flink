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

package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.api.common.typeutils.TypeInformationTestBase;
import org.apache.flink.connector.file.sink.FileSinkCommittableSerializer;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test for {@link CompactorRequestTypeInfo}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CompactorRequestTypeInfoTest extends TypeInformationTestBase<CompactorRequestTypeInfo> {

    @Override
    protected CompactorRequestTypeInfo[] getTestData() {
        return new CompactorRequestTypeInfo[] {
            new CompactorRequestTypeInfo(
                    () ->
                            new FileSinkCommittableSerializer(
                                    new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                                            FileSinkTestUtils.TestPendingFileRecoverable::new),
                                    new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                                            FileSinkTestUtils.TestInProgressFileRecoverable::new)))
        };
    }
}
