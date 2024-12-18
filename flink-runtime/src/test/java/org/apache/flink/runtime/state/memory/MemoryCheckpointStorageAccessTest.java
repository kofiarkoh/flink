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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFileCheckpointStorageAccessTestBase;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory.MemoryCheckpointOutputStream;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link MemoryBackendCheckpointStorageAccess}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class MemoryCheckpointStorageAccessTest extends AbstractFileCheckpointStorageAccessTestBase {

    private static final int DEFAULT_MAX_STATE_SIZE =
            JobManagerCheckpointStorage.DEFAULT_MAX_STATE_SIZE;

    // ------------------------------------------------------------------------
    //  General Fs-based checkpoint storage tests, inherited
    // ------------------------------------------------------------------------

    @Override
    protected CheckpointStorageAccess createCheckpointStorage(
            Path checkpointDir, boolean createCheckpointSubDir) throws Exception {
        return new MemoryBackendCheckpointStorageAccess(
                new JobID(), checkpointDir, null, createCheckpointSubDir, DEFAULT_MAX_STATE_SIZE);
    }

    @Override
    protected CheckpointStorageAccess createCheckpointStorageWithSavepointDir(
            Path checkpointDir, Path savepointDir, boolean createCheckpointSubDir)
            throws Exception {
        return new MemoryBackendCheckpointStorageAccess(
                new JobID(),
                checkpointDir,
                savepointDir,
                createCheckpointSubDir,
                DEFAULT_MAX_STATE_SIZE);
    }

    // ------------------------------------------------------------------------
    //  MemoryCheckpointStorage-specific tests
    // ------------------------------------------------------------------------

    @Test
    void testParametrizationDefault() throws Exception {
        final JobID jid = new JobID();

        JobManagerCheckpointStorage jobManagerCheckpointStorage = new JobManagerCheckpointStorage();

        MemoryBackendCheckpointStorageAccess storage =
                (MemoryBackendCheckpointStorageAccess)
                        jobManagerCheckpointStorage.createCheckpointStorage(jid);

        assertThat(storage.supportsHighlyAvailableStorage()).isFalse();
        assertThat(storage.hasDefaultSavepointLocation()).isFalse();
        assertThat(storage.getDefaultSavepointDirectory()).isNull();
        assertThat(storage.getMaxStateSize())
                .isEqualTo(JobManagerCheckpointStorage.DEFAULT_MAX_STATE_SIZE);
    }

    @Test
    void testParametrizationDirectories() throws Exception {
        final JobID jid = new JobID();
        final String checkpointPath = TempDirUtils.newFolder(tmp).toURI().toString();

        JobManagerCheckpointStorage jobManagerCheckpointStorage =
                new JobManagerCheckpointStorage(checkpointPath);

        MemoryBackendCheckpointStorageAccess storage =
                (MemoryBackendCheckpointStorageAccess)
                        jobManagerCheckpointStorage.createCheckpointStorage(jid);

        assertThat(storage.supportsHighlyAvailableStorage()).isTrue();
    }

    @Test
    void testParametrizationStateSize() throws Exception {
        final int maxSize = 17;

        JobManagerCheckpointStorage jobManagerCheckpointStorage =
                new JobManagerCheckpointStorage(maxSize);
        MemoryBackendCheckpointStorageAccess storage =
                (MemoryBackendCheckpointStorageAccess)
                        jobManagerCheckpointStorage.createCheckpointStorage(new JobID());

        assertThat(storage.getMaxStateSize()).isEqualTo(maxSize);
    }

    @Test
    void testNonPersistentCheckpointLocation() throws Exception {
        MemoryBackendCheckpointStorageAccess storage =
                new MemoryBackendCheckpointStorageAccess(
                        new JobID(), null, null, true, DEFAULT_MAX_STATE_SIZE);

        CheckpointStorageLocation location = storage.initializeLocationForCheckpoint(9);

        CheckpointMetadataOutputStream stream = location.createMetadataOutputStream();
        stream.write(99);

        CompletedCheckpointStorageLocation completed = stream.closeAndFinalizeCheckpoint();
        StreamStateHandle handle = completed.getMetadataHandle();
        assertThat(handle).isInstanceOf(ByteStreamStateHandle.class);

        // the reference is not valid in that case
        assertThatThrownBy(() -> storage.resolveCheckpoint(completed.getExternalPointer()))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testLocationReference() throws Exception {
        // non persistent memory state backend for checkpoint
        {
            MemoryBackendCheckpointStorageAccess storage =
                    new MemoryBackendCheckpointStorageAccess(
                            new JobID(), null, null, true, DEFAULT_MAX_STATE_SIZE);
            CheckpointStorageLocation location = storage.initializeLocationForCheckpoint(42);
            assertThat(location.getLocationReference().isDefaultReference()).isTrue();
        }

        // non persistent memory state backend for checkpoint
        {
            MemoryBackendCheckpointStorageAccess storage =
                    new MemoryBackendCheckpointStorageAccess(
                            new JobID(), randomTempPath(), null, true, DEFAULT_MAX_STATE_SIZE);
            CheckpointStorageLocation location = storage.initializeLocationForCheckpoint(42);
            assertThat(location.getLocationReference().isDefaultReference()).isTrue();
        }

        // memory state backend for savepoint
        {
            MemoryBackendCheckpointStorageAccess storage =
                    new MemoryBackendCheckpointStorageAccess(
                            new JobID(), null, null, true, DEFAULT_MAX_STATE_SIZE);
            CheckpointStorageLocation location =
                    storage.initializeLocationForSavepoint(1337, randomTempPath().toString());
            assertThat(location.getLocationReference().isDefaultReference()).isTrue();
        }
    }

    @Test
    void testTaskOwnedStateStream() throws Exception {
        final List<String> state = Arrays.asList("Flopsy", "Mopsy", "Cotton Tail", "Peter");

        final MemoryBackendCheckpointStorageAccess storage =
                new MemoryBackendCheckpointStorageAccess(
                        new JobID(), null, null, true, DEFAULT_MAX_STATE_SIZE);

        StreamStateHandle stateHandle;

        try (CheckpointStateOutputStream stream = storage.createTaskOwnedStateStream()) {
            assertThat(stream).isInstanceOf(MemoryCheckpointOutputStream.class);

            new ObjectOutputStream(stream).writeObject(state);
            stateHandle = stream.closeAndGetHandle();
        }

        try (ObjectInputStream in = new ObjectInputStream(stateHandle.openInputStream())) {
            assertThat(in.readObject()).isEqualTo(state);
        }
    }

    /**
     * This test checks that the expected mkdirs action for checkpoint storage, only called when
     * initializeLocationForCheckpoint.
     */
    @Test
    void testStorageLocationMkdirs() throws Exception {
        MemoryBackendCheckpointStorageAccess storage =
                new MemoryBackendCheckpointStorageAccess(
                        new JobID(),
                        new Path(randomTempPath(), "chk"),
                        null,
                        true,
                        DEFAULT_MAX_STATE_SIZE);

        File baseDir = new File(storage.getCheckpointsDirectory().getPath());
        assertThat(baseDir).doesNotExist();

        // mkdirs only be called when initializeLocationForCheckpoint
        storage.initializeLocationForCheckpoint(177L);
        assertThat(baseDir).exists();
    }
}
