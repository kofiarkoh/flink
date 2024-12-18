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

package org.apache.flink.connectors.hive;
import edu.illinois.CTestJUnit5Extension;

import org.junit.jupiter.api.extension.ExtendWith;

import edu.illinois.CTestClass;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.read.HiveCompactReaderFactory;
import org.apache.flink.connectors.hive.write.HiveWriterFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assume.assumeTrue;

/**
 * Sometimes users only add hive connector deps on client side but forget to add them on JM/TM. This
 * test is to make sure users get a clear message when that happens.
 */
@RunWith(Parameterized.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class HiveDeserializeExceptionTest {

    @Parameterized.Parameters(name = "{1}")
    public static Object[] parameters() {
        HiveWriterFactory writerFactory =
                new HiveWriterFactory(
                        new JobConf(),
                        HiveIgnoreKeyTextOutputFormat.class,
                        new SerDeInfo(),
                        ResolvedSchema.of(),
                        new String[0],
                        new Properties(),
                        HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion()),
                        false);

        HiveCompactReaderFactory compactReaderFactory =
                new HiveCompactReaderFactory(
                        new StorageDescriptor(),
                        new Properties(),
                        new JobConf(),
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder().build(),
                                        null,
                                        new ArrayList<>(),
                                        Collections.emptyMap()),
                                ResolvedSchema.of()),
                        HiveShimLoader.getHiveVersion(),
                        RowType.of(DataTypes.INT().getLogicalType()),
                        false);

        ResolvedSchema resolvedSchema = ResolvedSchema.of(Column.physical("i", DataTypes.INT()));
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                                null,
                                Collections.emptyList(),
                                Collections.emptyMap()),
                        resolvedSchema);
        HiveSourceBuilder builder =
                new HiveSourceBuilder(
                        new JobConf(),
                        new Configuration(),
                        new ObjectPath("default", "foo"),
                        HiveShimLoader.getHiveVersion(),
                        resolvedCatalogTable);
        builder.setPartitions(
                Collections.singletonList(
                        new HiveTablePartition(new StorageDescriptor(), new Properties())));

        HiveSource<RowData> hiveSource = builder.buildWithDefaultBulkFormat();

        return new Object[][] {
            new Object[] {writerFactory, writerFactory.getClass().getSimpleName()},
            new Object[] {compactReaderFactory, compactReaderFactory.getClass().getSimpleName()},
            new Object[] {hiveSource, hiveSource.getClass().getSimpleName()}
        };
    }

    @Parameterized.Parameter public Object object;

    @Parameterized.Parameter(1)
    public String name;

    @Test
    public void test() throws Exception {
        ClassLoader parentLoader = object.getClass().getClassLoader().getParent();
        assumeTrue(parentLoader != null);
        byte[] bytes = InstantiationUtil.serializeObject(object);
        assertThatThrownBy(() -> InstantiationUtil.deserializeObject(bytes, parentLoader))
                .isInstanceOf(ClassNotFoundException.class);
    }
}
