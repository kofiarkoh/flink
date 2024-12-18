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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.reflect.Field;
import java.net.InetAddress;

import static org.assertj.core.api.Assertions.assertThat;

/** Simple netty connection manager test. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class NettyConnectionManagerTest {

    /**
     * Tests that the number of arenas and number of threads of the client and server are set to the
     * same number, that is the number of configured task slots.
     */
    @Test
    void testMatchingNumberOfArenasAndThreadsAsDefault() throws Exception {
        // Expected number of arenas and threads
        int numberOfSlots = 2;
        NettyConnectionManager connectionManager;
        {
            NettyConfig config =
                    new NettyConfig(
                            InetAddress.getLocalHost(),
                            0,
                            1024,
                            numberOfSlots,
                            new Configuration());

            connectionManager = createNettyConnectionManager(config);
            connectionManager.start();
        }
        assertThat(connectionManager)
                .withFailMessage("connectionManager is null due to fail to get a free port")
                .isNotNull();

        assertThat(connectionManager.getBufferPool().getNumberOfArenas()).isEqualTo(numberOfSlots);

        {
            // Client event loop group
            Bootstrap boostrap = connectionManager.getClient().getBootstrap();
            EventLoopGroup group = boostrap.config().group();

            Field f = group.getClass().getSuperclass().getSuperclass().getDeclaredField("children");
            f.setAccessible(true);
            Object[] eventExecutors = (Object[]) f.get(group);

            assertThat(eventExecutors).hasSize(numberOfSlots);
        }

        {
            // Server event loop group
            ServerBootstrap bootstrap = connectionManager.getServer().getBootstrap();
            EventLoopGroup group = bootstrap.config().group();

            Field f = group.getClass().getSuperclass().getSuperclass().getDeclaredField("children");
            f.setAccessible(true);
            Object[] eventExecutors = (Object[]) f.get(group);

            assertThat(eventExecutors).hasSize(numberOfSlots);
        }

        {
            // Server child event loop group
            ServerBootstrap bootstrap = connectionManager.getServer().getBootstrap();
            EventLoopGroup group = bootstrap.childGroup();

            Field f = group.getClass().getSuperclass().getSuperclass().getDeclaredField("children");
            f.setAccessible(true);
            Object[] eventExecutors = (Object[]) f.get(group);

            assertThat(eventExecutors).hasSize(numberOfSlots);
        }
    }

    private NettyConnectionManager createNettyConnectionManager(NettyConfig config) {
        return new NettyConnectionManager(
                new ResultPartitionManager(), new TaskEventDispatcher(), config, true);
    }
}
