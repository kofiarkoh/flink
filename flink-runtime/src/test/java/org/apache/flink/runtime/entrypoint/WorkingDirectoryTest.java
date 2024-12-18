/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.TestLoggerExtension;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link WorkingDirectory}. */
@ExtendWith(TestLoggerExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class WorkingDirectoryTest {

    @Test
    public void testTmpDirectoryIsCleanedUp(@TempDir File directory) throws IOException {
        final WorkingDirectory workingDirectory = WorkingDirectory.create(directory);

        File tmpDirectory = workingDirectory.getTmpDirectory();
        assertThat(tmpDirectory).isEmptyDirectory();

        final File tmpFile = new File(tmpDirectory, "foobar");
        Files.createFile(tmpFile.toPath());
        assertThat(tmpFile).exists();

        final WorkingDirectory newWorkingDirectory = WorkingDirectory.create(directory);

        tmpDirectory = newWorkingDirectory.getTmpDirectory();

        assertThat(tmpDirectory.list()).isEmpty();
    }
}
