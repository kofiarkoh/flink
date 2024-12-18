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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.legacy.factories.TableSinkFactory;
import org.apache.flink.table.legacy.factories.TableSourceFactory;
import org.apache.flink.table.legacy.sinks.TableSink;
import org.apache.flink.table.legacy.sources.TableSource;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A table factory implementation for Hive catalog. */
public class HiveTableFactory implements TableSourceFactory, TableSinkFactory {

    @Override
    public Map<String, String> requiredContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> supportedProperties() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableSource createTableSource(TableSourceFactory.Context context) {
        CatalogTable table = checkNotNull(context.getTable());

        boolean isHiveTable = HiveCatalog.isHiveTable(table.getOptions());

        // we don't support temporary hive tables yet
        if (isHiveTable && !context.isTemporary()) {
            throw new UnsupportedOperationException(
                    "Legacy TableSource for Hive is deprecated. Hive table source should be created by HiveDynamicTableFactory.");
        } else {
            return TableFactoryUtil.findAndCreateTableSource(context);
        }
    }

    @Override
    public TableSink createTableSink(TableSinkFactory.Context context) {
        CatalogTable table = checkNotNull(context.getTable());

        boolean isHiveTable = HiveCatalog.isHiveTable(table.getOptions());

        // we don't support temporary hive tables yet
        if (isHiveTable && !context.isTemporary()) {
            throw new UnsupportedOperationException(
                    "Legacy TableSink for Hive is deprecated. Hive table sink should be created by HiveDynamicTableFactory.");
        } else {
            return TableFactoryUtil.findAndCreateTableSink(context);
        }
    }
}
