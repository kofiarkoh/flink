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

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** A utility class to calculate parallelism for Hive connector considering various factors. */
class HiveParallelismInference {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParallelismInference.class);

    private final ObjectPath tablePath;
    private final boolean infer;
    private final int inferMaxParallelism;

    private int parallelism;

    HiveParallelismInference(
            ObjectPath tablePath, boolean infer, int inferMaxParallelism, int parallelism) {
        this.tablePath = tablePath;
        this.infer = infer;
        this.inferMaxParallelism = inferMaxParallelism;
        this.parallelism = parallelism;
    }

    /**
     * Apply limit to calculate the parallelism. Here limit is the limit in query <code>
     * SELECT * FROM xxx LIMIT [limit]</code>.
     */
    int limit(Long limit) {
        if (!infer) {
            return parallelism;
        }

        if (limit != null) {
            parallelism = Math.min(parallelism, (int) (limit / 1000));
        }

        // make sure that parallelism is at least 1
        return Math.max(1, parallelism);
    }

    /**
     * Infer parallelism by number of files and number of splits. If {@link
     * HiveOptions#TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MODE} is none, this method does nothing.
     */
    HiveParallelismInference infer(
            SupplierWithException<Integer, IOException> numFiles,
            SupplierWithException<Integer, IOException> numSplits) {
        if (!infer) {
            return this;
        }

        try {
            // `createInputSplits` is costly,
            // so we try to avoid calling it by first checking the number of files
            // which is the lower bound of the number of splits
            int lowerBound = logRunningTime("getNumFiles", numFiles);
            if (lowerBound >= inferMaxParallelism) {
                parallelism = inferMaxParallelism;
                return this;
            }

            int splitNum = logRunningTime("createInputSplits", numSplits);
            parallelism = Math.min(splitNum, inferMaxParallelism);
        } catch (IOException e) {
            throw new FlinkHiveException(e);
        }
        return this;
    }

    private int logRunningTime(
            String operationName, SupplierWithException<Integer, IOException> supplier)
            throws IOException {
        long startTimeMillis = System.currentTimeMillis();
        int result = supplier.get();
        LOG.info(
                "Hive source({}}) {} use time: {} ms, result: {}",
                tablePath,
                operationName,
                System.currentTimeMillis() - startTimeMillis,
                result);
        return result;
    }

    /** Factory for the {@code HiveParallelismInference}. */
    interface Provider {

        /**
         * Creates a new {@code HiveParallelismInference}.
         *
         * @return a new {@code HiveParallelismInference} with designated factors.
         */
        HiveParallelismInference create();
    }
}
