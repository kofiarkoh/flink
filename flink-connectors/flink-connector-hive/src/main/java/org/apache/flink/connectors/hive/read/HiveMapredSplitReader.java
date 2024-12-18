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

package org.apache.flink.connectors.hive.read;
import edu.illinois.CTestJUnit5Extension;

import org.junit.jupiter.api.extension.ExtendWith;

import edu.illinois.CTestClass;

import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.connectors.hive.util.HivePartitionUtils;
import org.apache.flink.connectors.hive.util.JobConfUtils;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/** Hive {@link SplitReader} to read files using hadoop mapred {@link RecordReader}. */
public class HiveMapredSplitReader implements SplitReader {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMapredSplitReader.class);

    private RecordReader<Writable, Writable> recordReader;
    protected Writable key;
    protected Writable value;
    private boolean fetched = false;
    private boolean hasNext;
    private final Deserializer deserializer;

    // indices of fields to be returned, with projection applied (if any)
    // TODO: push projection into underlying input format that supports it
    private final int[] selectedFields;

    // Hive StructField list contain all related info for specific serde.
    private final List<? extends StructField> structFields;

    // StructObjectInspector in hive helps us to look into the internal structure of a struct
    // object.
    private final StructObjectInspector structObjectInspector;

    private final DataFormatConverters.DataFormatConverter[] converters;

    private final HiveShim hiveShim;

    private final GenericRowData row;

    public HiveMapredSplitReader(
            JobConf jobConf,
            List<String> partitionKeys,
            DataType[] fieldTypes,
            int[] selectedFields,
            HiveTableInputSplit split,
            HiveShim hiveShim)
            throws IOException {
        HiveTablePartition hiveTablePartition = split.getHiveTablePartition();
        StorageDescriptor sd = hiveTablePartition.getStorageDescriptor();
        jobConf.set(INPUT_DIR, sd.getLocation());
        InputFormat mapredInputFormat;
        try {
            mapredInputFormat =
                    (InputFormat)
                            Class.forName(
                                            sd.getInputFormat(),
                                            true,
                                            Thread.currentThread().getContextClassLoader())
                                    .newInstance();
        } catch (Exception e) {
            throw new FlinkHiveException("Unable to instantiate the hadoop input format", e);
        }
        ReflectionUtils.setConf(mapredInputFormat, jobConf);
        if (mapredInputFormat instanceof Configurable) {
            ((Configurable) mapredInputFormat).setConf(jobConf);
        } else if (mapredInputFormat instanceof JobConfigurable) {
            ((JobConfigurable) mapredInputFormat).configure(jobConf);
        }
        //noinspection unchecked
        this.recordReader =
                mapredInputFormat.getRecordReader(
                        split.getHadoopInputSplit(), jobConf, new HadoopDummyReporter());
        if (this.recordReader instanceof Configurable) {
            ((Configurable) this.recordReader).setConf(jobConf);
        }
        key = this.recordReader.createKey();
        value = this.recordReader.createValue();
        try {
            deserializer =
                    (Deserializer)
                            Class.forName(sd.getSerdeInfo().getSerializationLib()).newInstance();
            Configuration conf = new Configuration();
            SerDeUtils.initializeSerDe(
                    deserializer, conf, hiveTablePartition.getTableProps(), null);
            structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
            structFields = structObjectInspector.getAllStructFieldRefs();
        } catch (Exception e) {
            throw new FlinkHiveException("Error happens when deserialize from storage file.", e);
        }

        this.selectedFields = selectedFields;
        this.converters =
                Arrays.stream(selectedFields)
                        .mapToObj(i -> fieldTypes[i])
                        .map(DataFormatConverters::getConverterForDataType)
                        .toArray(DataFormatConverters.DataFormatConverter[]::new);
        this.hiveShim = hiveShim;

        // construct reuse row
        this.row = new GenericRowData(selectedFields.length);
        // set partition columns
        if (!partitionKeys.isEmpty()) {
            String defaultPartitionName = JobConfUtils.getDefaultPartitionName(jobConf);
            for (int i = 0; i < selectedFields.length; i++) {
                if (selectedFields[i] >= structFields.size()) {
                    LogicalType partitionType = fieldTypes[selectedFields[i]].getLogicalType();
                    String partition = partitionKeys.get(selectedFields[i] - structFields.size());
                    String valStr = hiveTablePartition.getPartitionSpec().get(partition);
                    Object partitionVal =
                            HivePartitionUtils.restorePartitionValueFromType(
                                    hiveShim, valStr, partitionType, defaultPartitionName);
                    row.setField(i, converters[i].toInternal(partitionVal));
                }
            }
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (!fetched) {
            hasNext = recordReader.next(key, value);
            fetched = true;
        }
        return !hasNext;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData nextRecord(RowData reuse) throws IOException {
        if (reachedEnd()) {
            return null;
        }
        try {
            // Use HiveDeserializer to deserialize an object out of a Writable blob
            Object hiveRowStruct = deserializer.deserialize(value);
            for (int i = 0; i < selectedFields.length; i++) {
                // set non-partition columns
                if (selectedFields[i] < structFields.size()) {
                    StructField structField = structFields.get(selectedFields[i]);
                    Object object =
                            HiveInspectors.toFlinkObject(
                                    structField.getFieldObjectInspector(),
                                    structObjectInspector.getStructFieldData(
                                            hiveRowStruct, structField),
                                    hiveShim);
                    row.setField(i, converters[i].toInternal(object));
                }
            }
        } catch (Exception e) {
            LOG.error("Error happens when converting hive data type to flink data type.");
            throw new FlinkHiveException(e);
        }
        this.fetched = false;
        return row;
    }

    @Override
    public void close() throws IOException {
        if (this.recordReader != null) {
            this.recordReader.close();
            this.recordReader = null;
        }
    }
}
