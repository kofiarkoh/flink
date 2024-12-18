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

import org.apache.flink.core.fs.FileSystemFactory;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.fail;

/** This test validates that the S3 file system registers both under s3:// and s3a://. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class HadoopS3FileSystemsSchemesTest {

    @Test
    void testS3Factory() {
        testFactory("s3");
    }

    @Test
    void testS3AFactory() {
        testFactory("s3a");
    }

    private static void testFactory(String scheme) {
        ServiceLoader<FileSystemFactory> serviceLoader =
                ServiceLoader.load(FileSystemFactory.class);
        for (FileSystemFactory fs : serviceLoader) {
            if (scheme.equals(fs.getScheme())) {
                // found the matching scheme
                return;
            }
        }

        fail("No factory available for scheme " + scheme);
    }
}
