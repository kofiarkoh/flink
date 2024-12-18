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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;

/** A simple test mock for a {@link StreamStateHandle}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class TestingStreamStateHandle extends ByteStreamStateHandle
        implements DiscardRecordedStateObject {
    private static final long serialVersionUID = 1L;

    private boolean disposed;

    public TestingStreamStateHandle() {
        super(UUID.randomUUID().toString(), new byte[0]);
    }

    public TestingStreamStateHandle(String handleName, byte[] data) {
        super(handleName, data);
    }

    // ------------------------------------------------------------------------

    @Override
    public void discardState() {
        super.discardState();
        disposed = true;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean isDisposed() {
        return disposed;
    }
}
