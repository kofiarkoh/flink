/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.util.AbstractAutoCloseableRegistry;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link SafetyNetCloseableRegistry}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class SafetyNetCloseableRegistryTest
        extends AbstractAutoCloseableRegistryTest<
                Closeable,
                WrappingProxyCloseable<? extends Closeable>,
                SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef> {

    @TempDir public File tmpFolder;

    @Override
    protected void registerCloseable(final Closeable closeable) throws IOException {
        final WrappingProxyCloseable<Closeable> wrappingProxyCloseable =
                new WrappingProxyCloseable<Closeable>() {

                    @Override
                    public void close() throws IOException {
                        closeable.close();
                    }

                    @Override
                    public Closeable getWrappedDelegate() {
                        return closeable;
                    }
                };
        closeableRegistry.registerCloseable(wrappingProxyCloseable);
    }

    @Override
    protected AbstractAutoCloseableRegistry<
                    Closeable,
                    WrappingProxyCloseable<? extends Closeable>,
                    SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef,
                    IOException>
            createRegistry() {
        // SafetyNetCloseableRegistry has a global reaper thread to reclaim leaking resources,
        // in normal cases, that thread will be interrupted in closing of last active registry
        // and then shutdown in background. But in testing codes, some assertions need leaking
        // resources reclaimed, so we override reaper thread to join itself on interrupt. Thus,
        // after close of last active registry, we can assert post-close-invariants safely.
        return new SafetyNetCloseableRegistry(JoinOnInterruptReaperThread::new);
    }

    @Override
    protected AbstractAutoCloseableRegistryTest.ProducerThread<
                    Closeable,
                    WrappingProxyCloseable<? extends Closeable>,
                    SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef>
            createProducerThread(
                    AbstractAutoCloseableRegistry<
                                    Closeable,
                                    WrappingProxyCloseable<? extends Closeable>,
                                    SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef,
                                    IOException>
                            registry,
                    AtomicInteger unclosedCounter,
                    int maxStreams) {

        return new AbstractAutoCloseableRegistryTest.ProducerThread<
                Closeable,
                WrappingProxyCloseable<? extends Closeable>,
                SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef>(
                registry, unclosedCounter, maxStreams) {

            int count = 0;

            @Override
            protected void createAndRegisterStream() throws IOException {
                String debug = Thread.currentThread().getName() + " " + count;
                TestStream testStream = new TestStream(refCount);

                // this method automatically registers the stream with the given registry.
                @SuppressWarnings("unused")
                ClosingFSDataInputStream pis =
                        ClosingFSDataInputStream.wrapSafe(
                                testStream,
                                (SafetyNetCloseableRegistry) registry,
                                debug); // reference dies here
                ++count;
            }
        };
    }

    @AfterEach
    void tearDown() {
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isFalse();
    }

    @Test
    void testCorrectScopesForSafetyNet() throws Exception {
        CheckedThread t1 =
                new CheckedThread() {

                    @Override
                    public void go() throws Exception {
                        FileSystem fs1 = FileSystem.getLocalFileSystem();
                        // ensure no safety net in place
                        assertThat(fs1).isNotInstanceOf(SafetyNetWrapperFileSystem.class);
                        FileSystemSafetyNet.initializeSafetyNetForThread();
                        fs1 = FileSystem.getLocalFileSystem();
                        // ensure safety net is in place now
                        assertThat(fs1).isInstanceOf(SafetyNetWrapperFileSystem.class);

                        Path tmp =
                                new Path(
                                        newFolder(tmpFolder, "junit").toURI().toString(),
                                        "test_file");

                        try (FSDataOutputStream stream =
                                fs1.create(tmp, FileSystem.WriteMode.NO_OVERWRITE)) {
                            CheckedThread t2 =
                                    new CheckedThread() {
                                        @Override
                                        public void go() {
                                            FileSystem fs2 = FileSystem.getLocalFileSystem();
                                            // ensure the safety net does not leak here
                                            assertThat(fs2)
                                                    .isNotInstanceOf(
                                                            SafetyNetWrapperFileSystem.class);
                                            FileSystemSafetyNet.initializeSafetyNetForThread();
                                            fs2 = FileSystem.getLocalFileSystem();
                                            // ensure we can bring another safety net in place
                                            assertThat(fs2)
                                                    .isInstanceOf(SafetyNetWrapperFileSystem.class);
                                            FileSystemSafetyNet
                                                    .closeSafetyNetAndGuardedResourcesForThread();
                                            fs2 = FileSystem.getLocalFileSystem();
                                            // and that we can remove it again
                                            assertThat(fs2)
                                                    .isNotInstanceOf(
                                                            SafetyNetWrapperFileSystem.class);
                                        }
                                    };

                            t2.start();
                            t2.sync();

                            // ensure stream is still open and was never closed by any
                            // interferences
                            stream.write(42);
                            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

                            // ensure leaking stream was closed
                            assertThatThrownBy(() -> stream.write(43))
                                    .isInstanceOf(IOException.class);
                            fs1 = FileSystem.getLocalFileSystem();
                            // ensure safety net was removed
                            assertThat(fs1).isNotInstanceOf(SafetyNetWrapperFileSystem.class);
                        } finally {
                            fs1.delete(tmp, false);
                        }
                    }
                };

        t1.start();
        t1.sync();
    }

    @Test
    void testSafetyNetClose() throws Exception {
        setup(20);
        startThreads();

        joinThreads();

        for (int i = 0; i < 5 && unclosedCounter.get() > 0; ++i) {
            System.gc();
            Thread.sleep(50);
        }

        assertThat(unclosedCounter).hasValue(0);
        closeableRegistry.close();
    }

    @Test
    void testReaperThreadSpawnAndStop() throws Exception {
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isFalse();

        try (SafetyNetCloseableRegistry ignored = new SafetyNetCloseableRegistry()) {
            assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isTrue();

            try (SafetyNetCloseableRegistry ignored2 = new SafetyNetCloseableRegistry()) {
                assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isTrue();
            }
            assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isTrue();
        }
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isFalse();
    }

    /**
     * Test whether failure to start thread in {@link SafetyNetCloseableRegistry} constructor can
     * lead to failure of subsequent state check.
     */
    @Test
    void testReaperThreadStartFailed() throws Exception {

        try {
            new SafetyNetCloseableRegistry(OutOfMemoryReaperThread::new);
        } catch (java.lang.OutOfMemoryError error) {
        }
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isFalse();

        // the OOM error will not lead to failure of subsequent constructor call.
        SafetyNetCloseableRegistry closeableRegistry = new SafetyNetCloseableRegistry();
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isTrue();

        closeableRegistry.close();
    }

    private static class JoinOnInterruptReaperThread
            extends SafetyNetCloseableRegistry.CloseableReaperThread {
        @Override
        public void interrupt() {
            super.interrupt();
            try {
                join();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        private static File newFolder(File root, String... subDirs) throws IOException {
            String subFolder = String.join("/", subDirs);
            File result = new File(root, subFolder);
            if (!result.mkdirs()) {
                throw new IOException("Couldn't create folders " + root);
            }
            return result;
        }
    }

    private static class OutOfMemoryReaperThread
            extends SafetyNetCloseableRegistry.CloseableReaperThread {

        @Override
        public synchronized void start() {
            throw new java.lang.OutOfMemoryError();
        }

        private static File newFolder(File root, String... subDirs) throws IOException {
            String subFolder = String.join("/", subDirs);
            File result = new File(root, subFolder);
            if (!result.mkdirs()) {
                throw new IOException("Couldn't create folders " + root);
            }
            return result;
        }
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }
}
