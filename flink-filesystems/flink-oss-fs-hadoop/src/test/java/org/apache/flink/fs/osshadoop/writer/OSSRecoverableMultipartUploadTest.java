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

package org.apache.flink.fs.osshadoop.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RefCountedBufferingFileStream;
import org.apache.flink.fs.osshadoop.OSSAccessor;
import org.apache.flink.fs.osshadoop.OSSTestUtils;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.testutils.oss.OSSTestCredentials;

import com.aliyun.oss.model.PartETag;
import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link OSSRecoverableMultipartUpload}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class OSSRecoverableMultipartUploadTest {

    private static Path basePath;

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    private FileSystem fs;

    private static final String TEST_OBJECT_NAME_PREFIX = "TEST-OBJECT-";

    private Path objectPath;

    private String uploadId;

    private List<PartETag> completeParts;

    private OSSAccessor ossAccessor;

    private OSSRecoverableMultipartUpload uploader;

    @TempDir File temporaryFolder;

    @BeforeEach
    void before() throws IOException {
        OSSTestCredentials.assumeCredentialsAvailable();

        final Configuration conf = new Configuration();
        conf.setString("fs.oss.endpoint", OSSTestCredentials.getOSSEndpoint());
        conf.setString("fs.oss.accessKeyId", OSSTestCredentials.getOSSAccessKey());
        conf.setString("fs.oss.accessKeySecret", OSSTestCredentials.getOSSSecretKey());
        FileSystem.initialize(conf);

        basePath = new Path(OSSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        fs = basePath.getFileSystem();

        objectPath = new Path(basePath + "/" + TEST_OBJECT_NAME_PREFIX + UUID.randomUUID());

        ossAccessor =
                new OSSAccessor(
                        (AliyunOSSFileSystem) ((HadoopFileSystem) fs).getHadoopFileSystem());

        uploadId = ossAccessor.startMultipartUpload(ossAccessor.pathToObject(objectPath));

        completeParts = new ArrayList<>();

        uploader =
                new OSSRecoverableMultipartUpload(
                        ossAccessor.pathToObject(objectPath),
                        Executors.newCachedThreadPool(),
                        ossAccessor,
                        null,
                        uploadId,
                        completeParts,
                        0L);
    }

    @Test
    void testUploadSinglePart() throws IOException {
        final byte[] part = OSSTestUtils.bytesOf("hello world", 1024 * 1024);

        OSSTestUtils.uploadPart(uploader, temporaryFolder, part);

        uploader.getRecoverable(null);

        ossAccessor.completeMultipartUpload(
                ossAccessor.pathToObject(objectPath), uploadId, completeParts);

        OSSTestUtils.objectContentEquals(fs, objectPath, part);
    }

    @Test
    void testUploadIncompletePart() throws IOException {
        final byte[] part = OSSTestUtils.bytesOf("hello world", 1024 * 1024);

        RefCountedBufferingFileStream partFile = OSSTestUtils.writeData(temporaryFolder, part);

        partFile.close();

        OSSRecoverable ossRecoverable = uploader.getRecoverable(partFile);

        OSSTestUtils.objectContentEquals(
                fs, ossAccessor.objectToPath(ossRecoverable.getLastPartObject()), part);
    }

    @Test
    void testMultipartAndIncompletePart() throws IOException {
        final byte[] firstCompletePart = OSSTestUtils.bytesOf("hello world", 1024 * 1024);
        final byte[] secondCompletePart = OSSTestUtils.bytesOf("hello again", 1024 * 1024);
        final byte[] thirdIncompletePart = OSSTestUtils.bytesOf("!!!", 1024);

        OSSTestUtils.uploadPart(uploader, temporaryFolder, firstCompletePart);
        OSSTestUtils.uploadPart(uploader, temporaryFolder, secondCompletePart);

        RefCountedBufferingFileStream partFile =
                OSSTestUtils.writeData(temporaryFolder, thirdIncompletePart);

        partFile.close();

        OSSRecoverable ossRecoverable = uploader.getRecoverable(partFile);

        assertThat(ossRecoverable.getPartETags()).hasSize(2);
        assertThat(ossRecoverable.getLastPartObject()).isNotNull();
        assertThat(ossRecoverable.getLastPartObjectLength()).isEqualTo(1026);
        assertThat(ossRecoverable.getNumBytesInParts()).isEqualTo(2 * 1024 * 1024 + 20);
        assertThat(ossRecoverable.getUploadId()).isEqualTo(uploadId);
        assertThat(ossRecoverable.getObjectName()).isEqualTo(ossAccessor.pathToObject(objectPath));

        ossAccessor.completeMultipartUpload(
                ossAccessor.pathToObject(objectPath), uploadId, completeParts);

        OSSTestUtils.objectContentEquals(fs, objectPath, firstCompletePart, secondCompletePart);

        OSSTestUtils.objectContentEquals(
                fs,
                ossAccessor.objectToPath(ossRecoverable.getLastPartObject()),
                thirdIncompletePart);
    }

    @Test
    void testRecoverableReflectsTheLatestPartialObject() throws IOException {
        final byte[] incompletePartOne = OSSTestUtils.bytesOf("AB", 1024);
        final byte[] incompletePartTwo = OSSTestUtils.bytesOf("ABC", 1024);

        RefCountedBufferingFileStream partFile =
                OSSTestUtils.writeData(temporaryFolder, incompletePartOne);
        partFile.close();

        OSSRecoverable recoverableOne = uploader.getRecoverable(partFile);

        partFile = OSSTestUtils.writeData(temporaryFolder, incompletePartTwo);
        partFile.close();
        OSSRecoverable recoverableTwo = uploader.getRecoverable(partFile);

        assertThat(recoverableOne.getLastPartObject())
                .isNotEqualTo(recoverableTwo.getLastPartObject());
    }

    @Test
    void testUploadingNonClosedFileAsCompleteShouldThroughException() throws IOException {
        final byte[] incompletePart = OSSTestUtils.bytesOf("!!!", 1024);

        RefCountedBufferingFileStream partFile =
                OSSTestUtils.writeData(temporaryFolder, incompletePart);

        assertThatThrownBy(() -> uploader.uploadPart(partFile))
                .isInstanceOf(IllegalStateException.class);
    }

    @AfterEach
    void after() throws IOException {
        try {
            if (fs != null) {
                fs.delete(basePath, true);
            }
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }
}
