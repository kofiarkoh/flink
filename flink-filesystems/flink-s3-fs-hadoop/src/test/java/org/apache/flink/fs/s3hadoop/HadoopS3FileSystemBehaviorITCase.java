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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemBehaviorTestSuite;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.s3.S3TestCredentials;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.UUID;

/**
 * An implementation of the {@link FileSystemBehaviorTestSuite} for the s3a-based S3 file system.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class HadoopS3FileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void checkCredentialsAndSetup() {
        // check whether credentials exist
        S3TestCredentials.assumeCredentialsAvailable();

        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        conf.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
        conf.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());
        FileSystem.initialize(conf, null);
    }

    @AfterAll
    static void clearFsConfig() throws IOException {
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected Path getBasePath() throws Exception {
        return new Path(S3TestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }

    @Override
    protected FileSystemKind getFileSystemKind() {
        return FileSystemKind.OBJECT_STORE;
    }
}
