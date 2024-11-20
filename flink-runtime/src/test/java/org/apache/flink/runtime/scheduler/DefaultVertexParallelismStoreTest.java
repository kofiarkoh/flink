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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultVertexParallelismStore}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class DefaultVertexParallelismStoreTest {
    @Test
    void testNotSet() {
        DefaultVertexParallelismStore store = new DefaultVertexParallelismStore();

        assertThatThrownBy(() -> store.getParallelismInfo(new JobVertexID()))
                .withFailMessage("No parallelism information set for vertex")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testSetInfo() {
        JobVertexID id = new JobVertexID();
        VertexParallelismInformation info = new MockVertexParallelismInfo();
        DefaultVertexParallelismStore store = new DefaultVertexParallelismStore();

        store.setParallelismInfo(id, info);

        VertexParallelismInformation storedInfo = store.getParallelismInfo(id);

        assertThat(storedInfo).isEqualTo(info);
    }

    private static final class MockVertexParallelismInfo implements VertexParallelismInformation {
        @Override
        public int getMinParallelism() {
            return 0;
        }

        @Override
        public int getParallelism() {
            return 0;
        }

        @Override
        public int getMaxParallelism() {
            return 0;
        }

        @Override
        public void setParallelism(int parallelism) {}

        @Override
        public void setMaxParallelism(int maxParallelism) {}

        @Override
        public boolean canRescaleMaxParallelism(int desiredMaxParallelism) {
            return false;
        }
    }
}
