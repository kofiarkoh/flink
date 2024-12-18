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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.types.Either;
import org.apache.flink.util.SerializedValue;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Tests {@link ExecutionGraph} deployment when offloading job and task information into the BLOB
 * server.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class DefaultExecutionGraphDeploymentWithBlobServerTest
        extends DefaultExecutionGraphDeploymentTest {

    @TempDir Path temporaryFolder;

    private Set<byte[]> seenHashes =
            Collections.newSetFromMap(new ConcurrentHashMap<byte[], Boolean>());

    protected BlobServer blobServer = null;

    @BeforeEach
    void setupBlobServer() throws IOException {
        Configuration config = new Configuration();
        // always offload the serialized job and task information
        config.set(BlobServerOptions.OFFLOAD_MINSIZE, 0);
        blobServer =
                new AssertBlobServer(
                        config, TempDirUtils.newFolder(temporaryFolder), new VoidBlobStore());
        blobWriter = blobServer;
        blobCache = blobServer;

        seenHashes.clear();
        blobServer.start();
    }

    @AfterEach
    void shutdownBlobServer() throws IOException {
        if (blobServer != null) {
            blobServer.close();
        }
    }

    @Override
    protected void checkJobOffloaded(DefaultExecutionGraph eg) throws Exception {
        TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> serializedJobInformation =
                eg.getTaskDeploymentDescriptorFactory().getSerializedJobInformation();

        assertThat(serializedJobInformation).isInstanceOf(TaskDeploymentDescriptor.Offloaded.class);
        PermanentBlobKey blobKey =
                ((TaskDeploymentDescriptor.Offloaded<JobInformation>) serializedJobInformation)
                        .serializedValueKey;
        assertThatNoException().isThrownBy(() -> blobServer.getFile(eg.getJobID(), blobKey));
    }

    @Override
    protected void checkTaskOffloaded(ExecutionGraph eg, JobVertexID jobVertexId) throws Exception {
        Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInformationOrBlobKey =
                eg.getJobVertex(jobVertexId).getTaskInformationOrBlobKey();

        assertThat(taskInformationOrBlobKey.isRight()).isTrue();

        // must not throw:
        blobServer.getFile(eg.getJobID(), taskInformationOrBlobKey.right());
    }

    private class AssertBlobServer extends BlobServer {
        public AssertBlobServer(Configuration config, File storageDir, BlobStore blobStore)
                throws IOException {
            super(config, storageDir, blobStore);
        }

        @Override
        public PermanentBlobKey putPermanent(JobID jobId, byte[] value) throws IOException {
            PermanentBlobKey key = super.putPermanent(jobId, value);
            // verify that we do not upload the same content more than once
            assertThat(seenHashes.add(key.getHash())).isTrue();
            return key;
        }
    }
}
