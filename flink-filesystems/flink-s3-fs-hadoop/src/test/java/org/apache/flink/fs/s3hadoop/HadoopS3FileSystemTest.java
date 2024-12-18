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
import org.apache.flink.runtime.util.HadoopConfigLoader;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the S3 file system support via Hadoop's {@link
 * org.apache.hadoop.fs.s3a.S3AFileSystem}.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class HadoopS3FileSystemTest {

    @Test
    void testShadingOfAwsCredProviderConfig() {
        final Configuration conf = new Configuration();
        conf.setString(
                "fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.ContainerCredentialsProvider");

        HadoopConfigLoader configLoader = S3FileSystemFactory.createHadoopConfigLoader();
        configLoader.setFlinkConfig(conf);

        org.apache.hadoop.conf.Configuration hadoopConfig = configLoader.getOrLoadHadoopConfig();
        assertThat(hadoopConfig.get("fs.s3a.aws.credentials.provider"))
                .isEqualTo("com.amazonaws.auth.ContainerCredentialsProvider");
    }

    // ------------------------------------------------------------------------
    //  These tests check that the S3FileSystemFactory properly forwards
    // various patterns of keys for credentials.
    // ------------------------------------------------------------------------

    /** Test forwarding of standard Hadoop-style credential keys. */
    @Test
    void testConfigKeysForwardingHadoopStyle() {
        Configuration conf = new Configuration();
        conf.setString("fs.s3a.access.key", "test_access_key");
        conf.setString("fs.s3a.secret.key", "test_secret_key");

        checkHadoopAccessKeys(conf, "test_access_key", "test_secret_key");
    }

    /** Test forwarding of shortened Hadoop-style credential keys. */
    @Test
    void testConfigKeysForwardingShortHadoopStyle() {
        Configuration conf = new Configuration();
        conf.setString("s3.access.key", "my_key_a");
        conf.setString("s3.secret.key", "my_key_b");

        checkHadoopAccessKeys(conf, "my_key_a", "my_key_b");
    }

    /** Test forwarding of shortened Presto-style credential keys. */
    @Test
    void testConfigKeysForwardingPrestoStyle() {
        Configuration conf = new Configuration();
        conf.setString("s3.access-key", "clé d'accès");
        conf.setString("s3.secret-key", "clef secrète");
        checkHadoopAccessKeys(conf, "clé d'accès", "clef secrète");
    }

    private static void checkHadoopAccessKeys(
            Configuration flinkConf, String accessKey, String secretKey) {
        HadoopConfigLoader configLoader = S3FileSystemFactory.createHadoopConfigLoader();
        configLoader.setFlinkConfig(flinkConf);

        org.apache.hadoop.conf.Configuration hadoopConf = configLoader.getOrLoadHadoopConfig();

        assertThat(hadoopConf.get("fs.s3a.access.key", null)).isEqualTo(accessKey);
        assertThat(hadoopConf.get("fs.s3a.secret.key", null)).isEqualTo(secretKey);
    }
}
