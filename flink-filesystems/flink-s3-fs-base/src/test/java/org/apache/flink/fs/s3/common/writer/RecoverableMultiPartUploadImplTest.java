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

import org.apache.flink.core.fs.RefCountedBufferingFileStream;
import org.apache.flink.core.fs.RefCountedFileWithStream;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.MathUtils;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartResult;
import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link RecoverableMultiPartUploadImpl}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class RecoverableMultiPartUploadImplTest {

    private static final int BUFFER_SIZE = 10;

    private static final String TEST_OBJECT_NAME = "TEST-OBJECT";

    @TempDir File temporaryFolder;

    private StubMultiPartUploader stubMultiPartUploader;

    private RecoverableMultiPartUploadImpl multiPartUploadUnderTest;

    @BeforeEach
    void before() throws IOException {
        stubMultiPartUploader = new StubMultiPartUploader();
        multiPartUploadUnderTest =
                RecoverableMultiPartUploadImpl.newUpload(
                        stubMultiPartUploader, new MainThreadExecutor(), TEST_OBJECT_NAME);
    }

    @Test
    void singlePartUploadShouldBeIncluded() throws IOException {
        final byte[] part = bytesOf("hello world");

        uploadPart(part);

        assertThatHasMultiPartUploadWithPart(stubMultiPartUploader, part, 1);
    }

    @Test
    void incompletePartShouldBeUploadedAsIndividualObject() throws IOException {
        final byte[] incompletePart = bytesOf("Hi!");

        uploadObject(incompletePart);

        assertThatHasUploadedObject(stubMultiPartUploader, incompletePart);
    }

    @Test
    void multiplePartAndObjectUploadsShouldBeIncluded() throws IOException {
        final byte[] firstCompletePart = bytesOf("hello world");
        final byte[] secondCompletePart = bytesOf("hello again");
        final byte[] thirdIncompletePart = bytesOf("!!!");

        uploadPart(firstCompletePart);
        uploadPart(secondCompletePart);
        uploadObject(thirdIncompletePart);

        assertThatHasMultiPartUploadWithPart(stubMultiPartUploader, firstCompletePart, 1);
        assertThatHasMultiPartUploadWithPart(stubMultiPartUploader, secondCompletePart, 2);
        assertThatHasUploadedObject(stubMultiPartUploader, thirdIncompletePart);
    }

    @Test
    void multiplePartAndObjectUploadsShouldBeReflectedInRecoverable() throws IOException {
        final byte[] firstCompletePart = bytesOf("hello world");
        final byte[] secondCompletePart = bytesOf("hello again");
        final byte[] thirdIncompletePart = bytesOf("!!!");

        uploadPart(firstCompletePart);
        uploadPart(secondCompletePart);

        final S3Recoverable recoverable = uploadObject(thirdIncompletePart);

        assertThatIsEqualTo(
                recoverable, thirdIncompletePart, firstCompletePart, secondCompletePart);
    }

    @Test
    void s3RecoverableReflectsTheLatestPartialObject() throws IOException {
        final byte[] incompletePartOne = bytesOf("AB");
        final byte[] incompletePartTwo = bytesOf("ABC");

        S3Recoverable recoverableOne = uploadObject(incompletePartOne);
        S3Recoverable recoverableTwo = uploadObject(incompletePartTwo);

        assertThat(recoverableTwo.incompleteObjectName())
                .isNotEqualTo(recoverableOne.incompleteObjectName());
    }

    @Test
    void uploadingNonClosedFileAsCompleteShouldThroughException() throws IOException {
        final byte[] incompletePart = bytesOf("!!!");

        final RefCountedBufferingFileStream incompletePartFile = writeContent(incompletePart);

        assertThatThrownBy(() -> multiPartUploadUnderTest.uploadPart(incompletePartFile))
                .isInstanceOf(IllegalStateException.class);
    }

    private static void assertThatHasMultiPartUploadWithPart(
            StubMultiPartUploader actual, byte[] content, int partNo) {
        TestUploadPartResult expectedCompletePart =
                createUploadPartResult(TEST_OBJECT_NAME, partNo, content);

        assertThat(actual.getCompletePartsUploaded()).contains(expectedCompletePart);
    }

    private static void assertThatHasUploadedObject(StubMultiPartUploader actual, byte[] content) {
        TestPutObjectResult expectedIncompletePart =
                createPutObjectResult(TEST_OBJECT_NAME, content);

        assertThat(actual.getIncompletePartsUploaded()).contains(expectedIncompletePart);
    }

    private static void assertThatIsEqualTo(
            S3Recoverable actualRecoverable, byte[] incompletePart, byte[]... completeParts) {
        S3Recoverable expectedRecoverable = createS3Recoverable(incompletePart, completeParts);

        assertThat(actualRecoverable.getObjectName())
                .isEqualTo(expectedRecoverable.getObjectName());
        assertThat(actualRecoverable.uploadId()).isEqualTo(expectedRecoverable.uploadId());
        assertThat(actualRecoverable.numBytesInParts())
                .isEqualTo(expectedRecoverable.numBytesInParts());
        assertThat(actualRecoverable.incompleteObjectLength())
                .isEqualTo(expectedRecoverable.incompleteObjectLength());

        assertThat(actualRecoverable.parts().stream().map(PartETag::getETag).toArray())
                .isEqualTo(expectedRecoverable.parts().stream().map(PartETag::getETag).toArray());
    }

    // ---------------------------------- Test Methods -------------------------------------------

    private static byte[] bytesOf(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private static S3Recoverable createS3Recoverable(
            byte[] incompletePart, byte[]... completeParts) {
        final List<PartETag> eTags = new ArrayList<>();

        int index = 1;
        long bytesInPart = 0L;
        for (byte[] part : completeParts) {
            eTags.add(new PartETag(index, createETag(TEST_OBJECT_NAME, index)));
            bytesInPart += part.length;
            index++;
        }

        return new S3Recoverable(
                TEST_OBJECT_NAME,
                createMPUploadId(TEST_OBJECT_NAME),
                eTags,
                bytesInPart,
                "IGNORED-DUE-TO-RANDOMNESS",
                (long) incompletePart.length);
    }

    private static RecoverableMultiPartUploadImplTest.TestPutObjectResult createPutObjectResult(
            String key, byte[] content) {
        final RecoverableMultiPartUploadImplTest.TestPutObjectResult result =
                new RecoverableMultiPartUploadImplTest.TestPutObjectResult();
        result.setETag(createETag(key, -1));
        result.setContent(content);
        return result;
    }

    private static RecoverableMultiPartUploadImplTest.TestUploadPartResult createUploadPartResult(
            String key, int number, byte[] payload) {
        final RecoverableMultiPartUploadImplTest.TestUploadPartResult result =
                new RecoverableMultiPartUploadImplTest.TestUploadPartResult();
        result.setETag(createETag(key, number));
        result.setPartNumber(number);
        result.setContent(payload);
        return result;
    }

    private static String createMPUploadId(String key) {
        return "MPU-" + key;
    }

    private static String createETag(String key, int partNo) {
        return "ETAG-" + key + '-' + partNo;
    }

    private S3Recoverable uploadObject(byte[] content) throws IOException {
        final RefCountedBufferingFileStream incompletePartFile = writeContent(content);
        incompletePartFile.flush();

        // as in the production code, we assume that a file containing
        // a in-progress part is flushed but not closed before being passed
        // to the uploader.

        return multiPartUploadUnderTest.snapshotAndGetRecoverable(incompletePartFile);
    }

    private void uploadPart(final byte[] content) throws IOException {
        RefCountedBufferingFileStream partFile = writeContent(content);

        // as in the production code, we assume that a file containing
        // a completed part is closed before being passed to the uploader.

        partFile.close();

        multiPartUploadUnderTest.uploadPart(partFile);
    }

    private RefCountedBufferingFileStream writeContent(byte[] content) throws IOException {
        final File newFile = new File(temporaryFolder, ".tmp_" + UUID.randomUUID());
        final OutputStream out =
                Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

        final RefCountedBufferingFileStream testStream =
                new RefCountedBufferingFileStream(
                        RefCountedFileWithStream.newFile(newFile, out), BUFFER_SIZE);

        testStream.write(content, 0, content.length);
        return testStream;
    }

    // ---------------------------------- Test Classes -------------------------------------------

    /** A simple executor that executes the runnable on the main thread. */
    private static class MainThreadExecutor implements Executor {

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    /**
     * A {@link S3AccessHelper} that simulates uploading part files to S3 by simply putting complete
     * and incomplete part files in lists for further validation.
     */
    private static class StubMultiPartUploader implements S3AccessHelper {

        private final List<RecoverableMultiPartUploadImplTest.TestUploadPartResult>
                completePartsUploaded = new ArrayList<>();
        private final List<RecoverableMultiPartUploadImplTest.TestPutObjectResult>
                incompletePartsUploaded = new ArrayList<>();

        List<RecoverableMultiPartUploadImplTest.TestUploadPartResult> getCompletePartsUploaded() {
            return completePartsUploaded;
        }

        List<RecoverableMultiPartUploadImplTest.TestPutObjectResult> getIncompletePartsUploaded() {
            return incompletePartsUploaded;
        }

        @Override
        public String startMultiPartUpload(String key) throws IOException {
            return createMPUploadId(key);
        }

        @Override
        public UploadPartResult uploadPart(
                String key, String uploadId, int partNumber, File inputFile, long length)
                throws IOException {
            final byte[] content =
                    getFileContentBytes(inputFile, MathUtils.checkedDownCast(length));
            return storeAndGetUploadPartResult(key, partNumber, content);
        }

        @Override
        public PutObjectResult putObject(String key, File inputFile) throws IOException {
            final byte[] content =
                    getFileContentBytes(inputFile, MathUtils.checkedDownCast(inputFile.length()));
            return storeAndGetPutObjectResult(key, content);
        }

        @Override
        public boolean deleteObject(String key) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getObject(String key, File targetLocation) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompleteMultipartUploadResult commitMultiPartUpload(
                String key,
                String uploadId,
                List<PartETag> partETags,
                long length,
                AtomicInteger errorCount)
                throws IOException {
            return null;
        }

        @Override
        public ObjectMetadata getObjectMetadata(String key) throws IOException {
            throw new UnsupportedOperationException();
        }

        private byte[] getFileContentBytes(File file, int length) throws IOException {
            final byte[] content = new byte[length];
            IOUtils.readFully(new FileInputStream(file), content, 0, length);
            return content;
        }

        private RecoverableMultiPartUploadImplTest.TestUploadPartResult storeAndGetUploadPartResult(
                String key, int number, byte[] payload) {
            final RecoverableMultiPartUploadImplTest.TestUploadPartResult result =
                    createUploadPartResult(key, number, payload);
            completePartsUploaded.add(result);
            return result;
        }

        private RecoverableMultiPartUploadImplTest.TestPutObjectResult storeAndGetPutObjectResult(
                String key, byte[] payload) {
            final RecoverableMultiPartUploadImplTest.TestPutObjectResult result =
                    createPutObjectResult(key, payload);
            incompletePartsUploaded.add(result);
            return result;
        }
    }

    /** A {@link PutObjectResult} that also contains the actual content of the uploaded part. */
    private static class TestPutObjectResult extends PutObjectResult {
        private static final long serialVersionUID = 1L;

        private byte[] content;

        void setContent(byte[] payload) {
            this.content = payload;
        }

        public byte[] getContent() {
            return content;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final TestPutObjectResult that = (TestPutObjectResult) o;
            // we ignore the etag as it contains randomness
            return Arrays.equals(getContent(), that.getContent());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(getContent());
        }

        @Override
        public String toString() {
            return '{' + " eTag=" + getETag() + ", payload=" + Arrays.toString(content) + '}';
        }
    }

    /** A {@link UploadPartResult} that also contains the actual content of the uploaded part. */
    private static class TestUploadPartResult extends UploadPartResult {

        private static final long serialVersionUID = 1L;

        private byte[] content;

        void setContent(byte[] content) {
            this.content = content;
        }

        public byte[] getContent() {
            return content;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final TestUploadPartResult that = (TestUploadPartResult) o;
            return getETag().equals(that.getETag())
                    && getPartNumber() == that.getPartNumber()
                    && Arrays.equals(content, that.content);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hash(getETag(), getPartNumber()) + Arrays.hashCode(getContent());
        }

        @Override
        public String toString() {
            return '{'
                    + "etag="
                    + getETag()
                    + ", partNo="
                    + getPartNumber()
                    + ", content="
                    + Arrays.toString(content)
                    + '}';
        }
    }
}
