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

package org.apache.flink.table.functions.hive;
import edu.illinois.CTestJUnit5Extension;

import org.junit.jupiter.api.extension.ExtendWith;

import edu.illinois.CTestClass;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.functions.hive.conversion.IdentityConversion;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A ScalarFunction implementation that calls Hive's {@link UDF}. */
@Internal
public class HiveSimpleUDF extends HiveScalarFunction<UDF> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveSimpleUDF.class);

    private final HiveShim hiveShim;

    private transient Method method;
    private transient GenericUDFUtils.ConversionHelper conversionHelper;
    private transient HiveObjectConversion[] conversions;
    private transient boolean allIdentityConverter;

    public HiveSimpleUDF(HiveFunctionWrapper<UDF> hiveFunctionWrapper, HiveShim hiveShim) {
        super(hiveFunctionWrapper);
        this.hiveShim = hiveShim;
        LOG.info("Creating HiveSimpleUDF from '{}'", this.hiveFunctionWrapper.getUDFClassName());
    }

    @Override
    public void openInternal() {
        LOG.info("Opening HiveSimpleUDF as '{}'", hiveFunctionWrapper.getUDFClassName());

        function = hiveFunctionWrapper.createFunction();

        List<TypeInfo> typeInfos = new ArrayList<>();

        for (int i = 0; i < arguments.size(); i++) {
            typeInfos.add(HiveTypeUtil.toHiveTypeInfo(arguments.getDataType(i), false));
        }

        try {
            method = function.getResolver().getEvalMethod(typeInfos);
            returnInspector =
                    ObjectInspectorFactory.getReflectionObjectInspector(
                            method.getGenericReturnType(),
                            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            ObjectInspector[] argInspectors = new ObjectInspector[typeInfos.size()];

            for (int i = 0; i < arguments.size(); i++) {
                argInspectors[i] =
                        TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfos.get(i));
            }

            conversionHelper = new GenericUDFUtils.ConversionHelper(method, argInspectors);
            conversions = new HiveObjectConversion[argInspectors.length];
            for (int i = 0; i < argInspectors.length; i++) {
                conversions[i] =
                        HiveInspectors.getConversion(
                                argInspectors[i],
                                arguments.getDataType(i).getLogicalType(),
                                hiveShim);
            }

            allIdentityConverter =
                    Arrays.stream(conversions).allMatch(conv -> conv instanceof IdentityConversion);
        } catch (Exception e) {
            throw new FlinkHiveUDFException(
                    String.format(
                            "Failed to open HiveSimpleUDF from %s",
                            hiveFunctionWrapper.getUDFClassName()),
                    e);
        }
    }

    @Override
    public Object evalInternal(Object[] args) {
        checkArgument(args.length == conversions.length);

        if (!allIdentityConverter) {
            for (int i = 0; i < args.length; i++) {
                args[i] = conversions[i].toHiveObject(args[i]);
            }
        }

        try {
            Object result =
                    FunctionRegistry.invoke(
                            method, function, conversionHelper.convertIfNecessary(args));
            return HiveInspectors.toFlinkObject(returnInspector, result, hiveShim);
        } catch (HiveException e) {
            throw new FlinkHiveUDFException(e);
        }
    }

    @Override
    public DataType inferReturnType() throws UDFArgumentException {
        List<TypeInfo> argTypeInfo = new ArrayList<>();
        for (int i = 0; i < arguments.size(); i++) {
            argTypeInfo.add(HiveTypeUtil.toHiveTypeInfo(arguments.getDataType(i), false));
        }

        Method evalMethod =
                hiveFunctionWrapper.createFunction().getResolver().getEvalMethod(argTypeInfo);
        return HiveTypeUtil.toFlinkType(
                ObjectInspectorFactory.getReflectionObjectInspector(
                        evalMethod.getGenericReturnType(),
                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
    }
}
