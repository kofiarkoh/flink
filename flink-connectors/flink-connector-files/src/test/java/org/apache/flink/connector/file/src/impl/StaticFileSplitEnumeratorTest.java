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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.SimpleSplitAssigner;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.fs.Path;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link ContinuousFileSplitEnumerator}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StaticFileSplitEnumeratorTest {

    // this is no JUnit temporary folder, because we don't create actual files, we just
    // need some random file path.
    private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"));

    private static long splitId = 1L;

    @Test
    void testCheckpointNoSplitRequested() throws Exception {
        final TestingSplitEnumeratorContext<FileSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        final FileSourceSplit split = createRandomSplit();
        final StaticFileSplitEnumerator enumerator = createEnumerator(context, split);

        final PendingSplitsCheckpoint<FileSourceSplit> checkpoint = enumerator.snapshotState(1L);

        assertThat(checkpoint.getSplits()).contains(split);
    }

    @Test
    void testSplitRequestForRegisteredReader() throws Exception {
        final TestingSplitEnumeratorContext<FileSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        final FileSourceSplit split = createRandomSplit();
        final StaticFileSplitEnumerator enumerator = createEnumerator(context, split);

        context.registerReader(3, "somehost");
        enumerator.addReader(3);
        enumerator.handleSplitRequest(3, "somehost");

        assertThat(enumerator.snapshotState(1L).getSplits()).isEmpty();
        assertThat(context.getSplitAssignments().get(3).getAssignedSplits()).contains(split);
    }

    @Test
    void testSplitRequestForNonRegisteredReader() throws Exception {
        final TestingSplitEnumeratorContext<FileSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        final FileSourceSplit split = createRandomSplit();
        final StaticFileSplitEnumerator enumerator = createEnumerator(context, split);

        enumerator.handleSplitRequest(3, "somehost");

        assertThat(context.getSplitAssignments()).doesNotContainKey(3);
        assertThat(enumerator.snapshotState(1L).getSplits()).contains(split);
    }

    @Test
    void testNoMoreSplits() throws Exception {
        final TestingSplitEnumeratorContext<FileSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        final FileSourceSplit split = createRandomSplit();
        final StaticFileSplitEnumerator enumerator = createEnumerator(context, split);

        // first split assignment
        context.registerReader(1, "somehost");
        enumerator.addReader(1);
        enumerator.handleSplitRequest(1, "somehost");

        // second request has no more split
        enumerator.handleSplitRequest(1, "somehost");

        assertThat(context.getSplitAssignments().get(1).getAssignedSplits()).contains(split);
        assertThat(context.getSplitAssignments().get(1).hasReceivedNoMoreSplitsSignal()).isTrue();
    }

    // ------------------------------------------------------------------------
    //  test setup helpers
    // ------------------------------------------------------------------------

    private static FileSourceSplit createRandomSplit() {
        return new FileSourceSplit(
                String.valueOf(splitId++),
                Path.fromLocalFile(new File(TMP_DIR, "foo")),
                0L,
                0L,
                0L,
                0L);
    }

    private static StaticFileSplitEnumerator createEnumerator(
            final SplitEnumeratorContext<FileSourceSplit> context,
            final FileSourceSplit... splits) {

        return new StaticFileSplitEnumerator(
                context, new SimpleSplitAssigner(Arrays.asList(splits)));
    }
}
