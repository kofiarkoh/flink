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

package org.apache.flink.fs.s3presto;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.testutils.s3.S3TestCredentials;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.MAX_CONCURRENT_UPLOADS;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.PART_UPLOAD_MIN_SIZE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link org.apache.flink.core.fs.RecoverableWriter} of the Presto S3 FS. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PrestoS3RecoverableWriterTest {

    // ----------------------- S3 general configuration -----------------------

    private static final long PART_UPLOAD_MIN_SIZE_VALUE = 7L << 20;
    private static final int MAX_CONCURRENT_UPLOADS_VALUE = 2;

    // ----------------------- Test Specific configuration -----------------------

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    // ----------------------- Test Lifecycle -----------------------

    @BeforeAll
    static void checkCredentialsAndSetup() throws IOException {
        // check whether credentials exist
        S3TestCredentials.assumeCredentialsAvailable();

        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        conf.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
        conf.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());

        conf.set(PART_UPLOAD_MIN_SIZE, PART_UPLOAD_MIN_SIZE_VALUE);
        conf.set(MAX_CONCURRENT_UPLOADS, MAX_CONCURRENT_UPLOADS_VALUE);

        final String defaultTmpDir = conf.get(CoreOptions.TMP_DIRS) + "s3_tmp_dir";
        conf.set(CoreOptions.TMP_DIRS, defaultTmpDir);

        FileSystem.initialize(conf);
    }

    @AfterAll
    static void cleanUp() throws IOException {
        FileSystem.initialize(new Configuration());
    }

    // ----------------------- Tests -----------------------

    @Test
    void requestingRecoverableWriterShouldThroughException() throws Exception {
        URI s3Uri = URI.create(S3TestCredentials.getTestBucketUri());
        FlinkS3FileSystem fileSystem = (FlinkS3FileSystem) FileSystem.get(s3Uri);
        assertThatThrownBy(() -> fileSystem.createRecoverableWriter())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
