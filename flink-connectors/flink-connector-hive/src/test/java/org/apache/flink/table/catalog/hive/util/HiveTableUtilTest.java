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

package org.apache.flink.table.catalog.hive.util;
import edu.illinois.CTestJUnit5Extension;

import org.junit.jupiter.api.extension.ExtendWith;

import edu.illinois.CTestClass;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for HiveTableUtil. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class HiveTableUtilTest {

    private static final HiveShim hiveShim =
            HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());

    @Test
    public void testMakePartitionFilter() {
        List<String> partColNames = Arrays.asList("p1", "p2", "p3");
        ResolvedExpression p1Ref = new FieldReferenceExpression("p1", DataTypes.INT(), 0, 2);
        ResolvedExpression p2Ref = new FieldReferenceExpression("p2", DataTypes.STRING(), 0, 3);
        ResolvedExpression p3Ref = new FieldReferenceExpression("p3", DataTypes.DOUBLE(), 0, 4);
        ResolvedExpression p1Exp =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(p1Ref, valueLiteral(1)),
                        DataTypes.BOOLEAN());
        ResolvedExpression p2Exp =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(p2Ref, valueLiteral("a", DataTypes.STRING().notNull())),
                        DataTypes.BOOLEAN());
        ResolvedExpression p3Exp =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(p3Ref, valueLiteral(1.1)),
                        DataTypes.BOOLEAN());
        Optional<String> filter =
                HiveTableUtil.makePartitionFilter(2, partColNames, Arrays.asList(p1Exp), hiveShim);
        assertThat(filter.orElse(null)).isEqualTo("(p1 = 1)");

        filter =
                HiveTableUtil.makePartitionFilter(
                        2, partColNames, Arrays.asList(p1Exp, p3Exp), hiveShim);
        assertThat(filter.orElse(null)).isEqualTo("(p1 = 1) and (p3 = 1.1)");

        filter =
                HiveTableUtil.makePartitionFilter(
                        2,
                        partColNames,
                        Arrays.asList(
                                p2Exp,
                                CallExpression.permanent(
                                        BuiltInFunctionDefinitions.OR,
                                        Arrays.asList(p1Exp, p3Exp),
                                        DataTypes.BOOLEAN())),
                        hiveShim);
        assertThat(filter.orElse(null)).isEqualTo("(p2 = 'a') and ((p1 = 1) or (p3 = 1.1))");
    }
}
