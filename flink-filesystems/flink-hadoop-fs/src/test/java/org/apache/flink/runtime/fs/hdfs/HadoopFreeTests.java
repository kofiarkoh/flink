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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.UnsupportedFileSystemSchemeException;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A class with tests that require to be run in a Hadoop-free environment, to test proper error
 * handling when no Hadoop classes are available.
 *
 * <p>This class must be dynamically loaded in a Hadoop-free class loader.
 */
// this class is only instantiated via reflection
@SuppressWarnings("unused")
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class HadoopFreeTests {

    public static void test() throws Exception {
        // make sure no Hadoop FS classes are in the classpath
        assertThatThrownBy(() -> Class.forName("org.apache.hadoop.fs.FileSystem"))
                .describedAs("Cannot run test when Hadoop classes are in the classpath")
                .isInstanceOf(ClassNotFoundException.class);

        assertThatThrownBy(() -> Class.forName("org.apache.hadoop.conf.Configuration"))
                .describedAs("Cannot run test when Hadoop classes are in the classpath")
                .isInstanceOf(ClassNotFoundException.class);

        // this method should complete without a linkage error
        final HadoopFsFactory factory = new HadoopFsFactory();

        // this method should also complete without a linkage error
        factory.configure(new Configuration());

        assertThatThrownBy(() -> factory.create(new URI("hdfs://somehost:9000/root/dir")))
                .isInstanceOf(UnsupportedFileSystemSchemeException.class);
    }
}
