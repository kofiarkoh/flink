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
 * limitations under the License
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.concurrent.FutureUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for using {@link BlobCacheSizeTracker} to track the size of BLOBs in {@link
 * PermanentBlobCache}. When new BLOBs are intended to be stored and the size limit exceeds, {@link
 * BlobCacheSizeTracker} will provide excess BLOBs for {@link PermanentBlobCache} to delete.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PermanentBlobCacheSizeLimitTest {

    private static final Random RANDOM = new Random();

    private static final BlobKey.BlobType BLOB_TYPE = BlobKey.BlobType.PERMANENT_BLOB;
    private static final int BLOB_SIZE = 10_000;
    // The size limit is the size of 2 blobs
    private static final int MAX_NUM_OF_ACCEPTED_BLOBS = 2;
    private static final int TOTAL_NUM_OF_BLOBS = 3;

    @TempDir Path tempDir;

    @Test
    void testTrackSizeLimitAndDeleteExcessSequentially() throws Exception {
        final Configuration config = new Configuration();

        try (BlobServer server = TestingBlobUtils.createServer(tempDir, config);
                BlobCacheService cache =
                        initBlobCacheServiceWithSizeLimit(
                                config, new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            // Put the BLOBs into the blob server
            final BlobInfo[] blobs = putBlobsIntoBlobServer(server);

            // The cache retrieves the BLOBs from the server sequentially
            for (int i = 0; i < TOTAL_NUM_OF_BLOBS; i++) {

                // Retrieve the BLOB from the blob server
                readFileAndVerifyContent(cache, blobs[i].jobId, blobs[i].blobKey, blobs[i].data);

                // Retrieve the location of BLOBs from the blob cache
                blobs[i].blobFile = getFile(cache, blobs[i].jobId, blobs[i].blobKey);
                assertThat(blobs[i].blobFile).exists();
            }

            // Since the size limit of the blob cache is the size of 2 BLOBs,
            // the first BLOB is removed and the second BLOB remains
            assertThat(blobs[0].blobFile).doesNotExist();
            assertThat(blobs[1].blobFile).exists();

            // Retrieve the second BLOB once again,
            // make the third BLOB to be the least recently used
            readFileAndVerifyContent(cache, blobs[1].jobId, blobs[1].blobKey, blobs[1].data);

            // Then retrieve the first BLOB again, make sure the third BLOB is replaced
            blobs[0].blobKey = put(server, blobs[0].jobId, blobs[0].data, BLOB_TYPE);
            readFileAndVerifyContent(cache, blobs[0].jobId, blobs[0].blobKey, blobs[0].data);
            blobs[0].blobFile = getFile(cache, blobs[0].jobId, blobs[0].blobKey);

            assertThat(blobs[0].blobFile).exists();
            assertThat(blobs[1].blobFile).exists();
            assertThat(blobs[2].blobFile).doesNotExist();
        }
    }

    @Test
    void testTrackSizeLimitAndDeleteExcessConcurrently() throws Exception {

        final ExecutorService executor = Executors.newFixedThreadPool(TOTAL_NUM_OF_BLOBS);
        final Configuration config = new Configuration();

        try (BlobServer server = TestingBlobUtils.createServer(tempDir, config);
                BlobCacheService cache =
                        initBlobCacheServiceWithSizeLimit(
                                config, new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            // Put the BLOBs into the blob server
            final BlobInfo[] blobs = putBlobsIntoBlobServer(server);

            final List<CompletableFuture<Void>> futures = new ArrayList<>(TOTAL_NUM_OF_BLOBS);

            // The blob cache retrieves the BLOB from the server concurrently
            for (int i = 0; i < TOTAL_NUM_OF_BLOBS; i++) {
                int idx = i;
                CompletableFuture<Void> future =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        // Retrieve the BLOB from the blob server
                                        readFileAndVerifyContent(
                                                cache,
                                                blobs[idx].jobId,
                                                blobs[idx].blobKey,
                                                blobs[idx].data);

                                        // Retrieve the location of BLOBs from the blob cache
                                        blobs[idx].blobFile =
                                                getFile(
                                                        cache,
                                                        blobs[idx].jobId,
                                                        blobs[idx].blobKey);

                                        return null;
                                    } catch (IOException e) {
                                        throw new CompletionException(e);
                                    }
                                },
                                executor);

                futures.add(future);
            }

            final CompletableFuture<Void> conjunctFuture = FutureUtils.waitForAll(futures);
            conjunctFuture.get();

            // Check how many BLOBs exist in the blob cache
            int exists = 0, nonExists = 0;
            for (int i = 0; i < TOTAL_NUM_OF_BLOBS; i++) {
                if (blobs[i].blobFile.exists()) {
                    exists++;
                } else {
                    nonExists++;
                }
            }
            assertThat(exists).isEqualTo(MAX_NUM_OF_ACCEPTED_BLOBS);
            assertThat(nonExists).isEqualTo(TOTAL_NUM_OF_BLOBS - MAX_NUM_OF_ACCEPTED_BLOBS);

        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * {@link BlobInfo} contains all the information related to a BLOB (for the test purpose only).
     */
    private static class BlobInfo {
        private final JobID jobId;
        private final byte[] data;
        private BlobKey blobKey;
        private File blobFile;

        private BlobInfo() {
            this.jobId = new JobID();

            this.data = new byte[BLOB_SIZE];
            RANDOM.nextBytes(this.data);
        }
    }

    private BlobCacheService initBlobCacheServiceWithSizeLimit(
            Configuration config, @Nullable final InetSocketAddress serverAddress)
            throws IOException {

        final PermanentBlobCache permanentBlobCache =
                new PermanentBlobCache(
                        config,
                        tempDir.resolve("permanent_cache").toFile(),
                        new VoidBlobStore(),
                        serverAddress,
                        new BlobCacheSizeTracker(MAX_NUM_OF_ACCEPTED_BLOBS * BLOB_SIZE));

        final TransientBlobCache transientBlobCache =
                new TransientBlobCache(
                        config, tempDir.resolve("transient_cache").toFile(), serverAddress);

        return new BlobCacheService(permanentBlobCache, transientBlobCache);
    }

    private static BlobInfo[] putBlobsIntoBlobServer(BlobServer server) throws IOException {
        // Initialize the information of BLOBs
        BlobInfo[] blobs = new BlobInfo[TOTAL_NUM_OF_BLOBS];

        // Put all the BLOBs into the blob server one by one
        for (int i = 0; i < TOTAL_NUM_OF_BLOBS; i++) {
            blobs[i] = new BlobInfo();

            // Put the BLOB into the blob server
            blobs[i].blobKey = put(server, blobs[i].jobId, blobs[i].data, BLOB_TYPE);
            assertThat(blobs[i].blobKey).isNotNull();
        }

        return blobs;
    }

    private static void readFileAndVerifyContent(
            BlobService blobService, JobID jobId, BlobKey blobKey, byte[] expected)
            throws IOException {

        assertThat(jobId).isNotNull();
        assertThat(blobKey).isNotNull().isInstanceOf(PermanentBlobKey.class);

        byte[] target =
                blobService.getPermanentBlobService().readFile(jobId, (PermanentBlobKey) blobKey);
        assertThat(target).isEqualTo(expected);
    }

    private static File getFile(BlobCacheService blobCacheService, JobID jobId, BlobKey blobKey)
            throws IOException {
        return blobCacheService.getPermanentBlobService().getStorageLocation(jobId, blobKey);
    }
}
