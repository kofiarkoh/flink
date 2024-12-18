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

package org.apache.flink.table.catalog.hive.client;
import edu.illinois.CTestJUnit5Extension;

import org.junit.jupiter.api.extension.ExtendWith;

import edu.illinois.CTestClass;

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.legacy.api.constraints.UniqueConstraint;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shim for Hive version 2.1.0. */
public class HiveShimV210 extends HiveShimV201 {

    protected final boolean hasFollowingStatsTask = false;

    @Override
    public void alterPartition(
            IMetaStoreClient client, String databaseName, String tableName, Partition partition)
            throws InvalidOperationException, MetaException, TException {
        String errorMsg = "Failed to alter partition for table %s in database %s";
        try {
            Method method =
                    client.getClass()
                            .getMethod(
                                    "alter_partition",
                                    String.class,
                                    String.class,
                                    Partition.class,
                                    EnvironmentContext.class);
            method.invoke(client, databaseName, tableName, partition, null);
        } catch (InvocationTargetException ite) {
            Throwable targetEx = ite.getTargetException();
            if (targetEx instanceof TException) {
                throw (TException) targetEx;
            } else {
                throw new CatalogException(
                        String.format(errorMsg, tableName, databaseName), targetEx);
            }
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new CatalogException(String.format(errorMsg, tableName, databaseName), e);
        }
    }

    @Override
    public Optional<UniqueConstraint> getPrimaryKey(
            IMetaStoreClient client, String dbName, String tableName, byte requiredTrait) {
        try {
            Class requestClz =
                    Class.forName("org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest");
            Object request =
                    requestClz
                            .getDeclaredConstructor(String.class, String.class)
                            .newInstance(dbName, tableName);
            List<?> constraints =
                    (List<?>)
                            HiveReflectionUtils.invokeMethod(
                                    client.getClass(),
                                    client,
                                    "getPrimaryKeys",
                                    new Class[] {requestClz},
                                    new Object[] {request});
            if (constraints.isEmpty()) {
                return Optional.empty();
            }
            Class constraintClz =
                    Class.forName("org.apache.hadoop.hive.metastore.api.SQLPrimaryKey");
            Method colNameMethod = constraintClz.getDeclaredMethod("getColumn_name");
            Method isEnableMethod = constraintClz.getDeclaredMethod("isEnable_cstr");
            Method isValidateMethod = constraintClz.getDeclaredMethod("isValidate_cstr");
            Method isRelyMethod = constraintClz.getDeclaredMethod("isRely_cstr");
            List<String> colNames = new ArrayList<>();
            for (Object constraint : constraints) {
                // check whether a constraint satisfies all the traits the caller specified
                boolean satisfy =
                        !HiveTableUtil.requireEnableConstraint(requiredTrait)
                                || (boolean) isEnableMethod.invoke(constraint);
                if (satisfy) {
                    satisfy =
                            !HiveTableUtil.requireValidateConstraint(requiredTrait)
                                    || (boolean) isValidateMethod.invoke(constraint);
                }
                if (satisfy) {
                    satisfy =
                            !HiveTableUtil.requireRelyConstraint(requiredTrait)
                                    || (boolean) isRelyMethod.invoke(constraint);
                }
                if (satisfy) {
                    colNames.add((String) colNameMethod.invoke(constraint));
                } else {
                    return Optional.empty();
                }
            }
            // all pk constraints should have the same name, so let's use the name of the first one
            String pkName =
                    (String)
                            HiveReflectionUtils.invokeMethod(
                                    constraintClz, constraints.get(0), "getPk_name", null, null);
            return Optional.of(UniqueConstraint.primaryKey(pkName, colNames));
        } catch (Throwable t) {
            if (t instanceof InvocationTargetException) {
                t = t.getCause();
            }
            if (t instanceof TApplicationException
                    && t.getMessage() != null
                    && t.getMessage().contains("Invalid method name")) {
                return Optional.empty();
            }
            throw new CatalogException("Failed to get PrimaryKey constraints", t);
        }
    }

    @Override
    public void createTableWithConstraints(
            IMetaStoreClient client,
            Table table,
            Configuration conf,
            UniqueConstraint pk,
            List<Byte> pkTraits,
            List<String> notNullCols,
            List<Byte> nnTraits) {
        if (!notNullCols.isEmpty()) {
            throw new UnsupportedOperationException(
                    "NOT NULL constraints not supported until 3.0.0");
        }
        try {
            List<Object> hivePKs = createHivePKs(table, pk, pkTraits);
            // createTableWithConstraints takes PK and FK lists
            HiveReflectionUtils.invokeMethod(
                    client.getClass(),
                    client,
                    "createTableWithConstraints",
                    new Class[] {Table.class, List.class, List.class},
                    new Object[] {table, hivePKs, Collections.emptyList()});
        } catch (Exception e) {
            throw new CatalogException("Failed to create Hive table with constraints", e);
        }
    }

    List<Object> createHivePKs(Table table, UniqueConstraint pk, List<Byte> traits)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException,
                    NoSuchMethodException, InvocationTargetException {
        List<Object> res = new ArrayList<>();
        if (pk != null) {
            Class pkClz = Class.forName("org.apache.hadoop.hive.metastore.api.SQLPrimaryKey");
            // PK constructor takes dbName, tableName, colName, keySeq, pkName, enable, validate,
            // rely
            Constructor constructor =
                    pkClz.getConstructor(
                            String.class,
                            String.class,
                            String.class,
                            int.class,
                            String.class,
                            boolean.class,
                            boolean.class,
                            boolean.class);
            int seq = 1;
            Preconditions.checkArgument(
                    pk.getColumns().size() == traits.size(),
                    "Number of PK columns and traits mismatch");
            for (int i = 0; i < pk.getColumns().size(); i++) {
                String col = pk.getColumns().get(i);
                byte trait = traits.get(i);
                boolean enable = HiveTableUtil.requireEnableConstraint(trait);
                boolean validate = HiveTableUtil.requireValidateConstraint(trait);
                boolean rely = HiveTableUtil.requireRelyConstraint(trait);
                Object hivePK =
                        constructor.newInstance(
                                table.getDbName(),
                                table.getTableName(),
                                col,
                                seq++,
                                pk.getName(),
                                enable,
                                validate,
                                rely);
                res.add(hivePK);
            }
        }
        return res;
    }

    @Override
    public void loadTable(
            Hive hive, Path loadPath, String tableName, boolean replace, boolean isSrcLocal) {
        try {
            Class hiveClass = Hive.class;
            Method loadTableMethod =
                    hiveClass.getDeclaredMethod(
                            "loadTable",
                            Path.class,
                            String.class,
                            boolean.class,
                            boolean.class,
                            boolean.class,
                            boolean.class,
                            boolean.class);
            loadTableMethod.invoke(
                    hive,
                    loadPath,
                    tableName,
                    replace,
                    isSrcLocal,
                    isSkewedStoreAsSubdir,
                    isAcid,
                    hasFollowingStatsTask);
        } catch (Exception e) {
            throw new FlinkHiveException("Failed to load table", e);
        }
    }

    @Override
    public void loadPartition(
            Hive hive,
            Path loadPath,
            String tableName,
            Map<String, String> partSpec,
            boolean isSkewedStoreAsSubdir,
            boolean replace,
            boolean isSrcLocal) {
        try {
            Class hiveClass = Hive.class;
            Method loadPartitionMethod =
                    hiveClass.getDeclaredMethod(
                            "loadPartition",
                            Path.class,
                            String.class,
                            Map.class,
                            boolean.class,
                            boolean.class,
                            boolean.class,
                            boolean.class,
                            boolean.class,
                            boolean.class);
            loadPartitionMethod.invoke(
                    hive,
                    loadPath,
                    tableName,
                    partSpec,
                    replace,
                    inheritTableSpecs,
                    isSkewedStoreAsSubdir,
                    isSrcLocal,
                    isAcid,
                    hasFollowingStatsTask);
        } catch (Exception e) {
            throw new FlinkHiveException("Failed to load partition", e);
        }
    }
}
