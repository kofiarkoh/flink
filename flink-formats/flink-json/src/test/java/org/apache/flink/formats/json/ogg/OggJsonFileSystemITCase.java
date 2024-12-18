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

package org.apache.flink.formats.json.ogg;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/** Test Filesystem connector with OGG Json. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class OggJsonFileSystemITCase extends StreamingTestBase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static final List<String> EXPECTED =
            Arrays.asList(
                    "+I[101, SCOOTER, Small 2-wheel scooter, 3.14]",
                    "+I[102, CAR BATTERY, 12V car battery, 8.1]",
                    "+I[103, 12-PACK DRILL BITS, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                    "+I[104, HAMMER, 12oz carpenter's hammer, 0.75]",
                    "+I[105, HAMMER, 14oz carpenter's hammer, 0.875]",
                    "+I[106, HAMMER, 16oz carpenter's hammer, 1.0]",
                    "+I[107, ROCKS, box of assorted rocks, 5.3]",
                    "+I[108, JACKET, water resistent black wind breaker, 0.1]",
                    "+I[109, SPARE TIRE, 24 inch spare tire, 22.2]",
                    "-D[106, HAMMER, 16oz carpenter's hammer, 1.0]", // -U
                    "+I[106, HAMMER, 18oz carpenter hammer, 1.0]", // +U
                    "-D[107, ROCKS, box of assorted rocks, 5.3]", // -U
                    "+I[107, ROCKS, box of assorted rocks, 5.1]", // +U
                    "+I[110, JACKET, water resistent white wind breaker, 0.2]",
                    "+I[111, SCOOTER, Big 2-wheel scooter , 5.18]",
                    "-D[110, JACKET, water resistent white wind breaker, 0.2]", // -U
                    "+I[110, JACKET, new water resistent white wind breaker, 0.5]", // +U
                    "-D[111, SCOOTER, Big 2-wheel scooter , 5.18]", // -U
                    "+I[111, SCOOTER, Big 2-wheel scooter , 5.17]", // +U
                    "-D[111, SCOOTER, Big 2-wheel scooter , 5.17]");

    private File source;
    private File sink;

    private static byte[] readBytes(String resource) throws IOException {
        final URL url = OggJsonSerDeSchemaTest.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllBytes(path);
    }

    private void prepareTables(boolean isPartition, Path tempSourceDir, Path tempSinkDir)
            throws IOException {
        byte[] bytes = readBytes("ogg-data.txt");
        source = tempSourceDir.toFile();
        File file;
        if (isPartition) {
            File partition = new File(source, "p=1");
            partition.mkdirs();
            file = new File(partition, "my_file");
        } else {
            file = new File(source, "my_file");
        }
        file.createNewFile();
        Files.write(file.toPath(), bytes);

        sink = tempSinkDir.toFile();

        env().setParallelism(1);
    }

    private void createTable(boolean isSink, String path, boolean isPartition) {
        tEnv().executeSql(
                        format("create table %s (", isSink ? "sink" : "source")
                                + "id int, name string,"
                                + (isSink ? "upper_name string," : "")
                                + " description string, weight float"
                                + (isPartition ? ", p int) partitioned by (p) " : ")")
                                + " with ("
                                + "'connector'='filesystem',"
                                + "'format'='ogg-json',"
                                + format("'path'='%s'", path)
                                + ")");
    }

    @Test
    void testNonPartition(@TempDir Path tempSourceDir, @TempDir Path tempSinkDir) throws Exception {
        prepareTables(true, tempSourceDir, tempSinkDir);
        createTable(false, source.toURI().toString(), false);
        createTable(true, sink.toURI().toString(), false);

        tEnv().executeSql(
                        "insert into sink select id,name,UPPER(name),description,weight from source")
                .await();
        CloseableIterator<Row> iter =
                tEnv().executeSql("select id,upper_name,description,weight from sink").collect();

        List<String> results =
                CollectionUtil.iteratorToList(iter).stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());
        iter.close();

        assertThat(results).isEqualTo(EXPECTED);
    }

    @Test
    void testPartition(@TempDir Path tempSourceDir, @TempDir Path tempSinkDir) throws Exception {
        prepareTables(true, tempSourceDir, tempSinkDir);
        createTable(false, source.toURI().toString(), true);
        createTable(true, sink.toURI().toString(), true);

        tEnv().executeSql(
                        "insert into sink select id,name,UPPER(name),description,weight,p from source")
                .await();
        CloseableIterator<Row> iter =
                tEnv().executeSql("select id,upper_name,description,weight,p from sink").collect();
        List<Row> list = CollectionUtil.iteratorToList(iter);
        iter.close();

        List<String> results =
                list.stream()
                        .map(row -> Row.project(row, new int[] {0, 1, 2, 3}))
                        .map(Row::toString)
                        .collect(Collectors.toList());

        assertThat(results).isEqualTo(EXPECTED);

        // check partition value
        for (Row row : list) {
            assertThat(row.getField(4)).isEqualTo(1);
        }
    }
}
