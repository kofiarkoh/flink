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

package org.apache.flink.table.sources;

import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.legacy.sources.TableSource;
import org.apache.flink.table.types.utils.TypeConversions;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for {@link CsvTableSource}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CsvTableSourceTest extends TableSourceTestBase {

    @Override
    protected TableSource<?> createTableSource(TableSchema requestedSchema) {
        CsvTableSource.Builder builder =
                CsvTableSource.builder().path("ignored").fieldDelimiter("|");

        requestedSchema
                .getTableColumns()
                .forEach(
                        column ->
                                builder.field(
                                        column.getName(),
                                        TypeConversions.fromDataTypeToLegacyInfo(
                                                column.getType())));

        return builder.build();
    }
}
