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

package org.apache.flink.fs.s3.common.writer;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the {@link
 * RecoverableMultiPartUploadImpl#createIncompletePartObjectNamePrefix(String)}.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class IncompletePartPrefixTest {

    @Test
    void nullObjectNameShouldThroughException() {
        assertThatThrownBy(
                        () ->
                                RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix(
                                        null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void emptyInitialNameShouldSucceed() {
        String objectNamePrefix =
                RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix("");

        assertThat(objectNamePrefix).isEqualTo("_tmp_");
    }

    @Test
    void nameWithoutSlashShouldSucceed() {
        String objectNamePrefix =
                RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix(
                        "no_slash_path");

        assertThat(objectNamePrefix).isEqualTo("_no_slash_path_tmp_");
    }

    @Test
    void nameWithOnlySlashShouldSucceed() {
        String objectNamePrefix =
                RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix("/");

        assertThat(objectNamePrefix).isEqualTo("/_tmp_");
    }

    @Test
    void normalPathShouldSucceed() {
        String objectNamePrefix =
                RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix(
                        "/root/home/test-file");

        assertThat(objectNamePrefix).isEqualTo("/root/home/_test-file_tmp_");
    }
}
