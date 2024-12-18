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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Test utility for Netty server and client setup. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class NettyTestUtil {

    static final int DEFAULT_SEGMENT_SIZE = 1024;

    // ---------------------------------------------------------------------------------------------
    // NettyServer and NettyClient
    // ---------------------------------------------------------------------------------------------

    static NettyServer initServer(
            NettyConfig config, NettyProtocol protocol, NettyBufferPool bufferPool)
            throws Exception {
        final NettyServer server = new NettyServer(config);

        try {
            server.init(protocol, bufferPool);
        } catch (Exception e) {
            server.shutdown();
            throw e;
        }

        return server;
    }

    static NettyServer initServer(
            NettyConfig config,
            NettyBufferPool bufferPool,
            Function<SSLHandlerFactory, NettyServer.ServerChannelInitializer> channelInitializer)
            throws Exception {
        final NettyServer server = new NettyServer(config);

        try {
            server.init(bufferPool, channelInitializer);
        } catch (Exception e) {
            server.shutdown();
            throw e;
        }

        return server;
    }

    static NettyClient initClient(
            NettyConfig config, NettyProtocol protocol, NettyBufferPool bufferPool)
            throws Exception {
        final NettyClient client = new NettyClient(config);

        try {
            client.init(protocol, bufferPool);
        } catch (Exception e) {
            client.shutdown();
            throw e;
        }

        return client;
    }

    static NettyServerAndClient initServerAndClient(NettyProtocol protocol) throws Exception {
        // It is possible that between checking a port available and binding to the port something
        // takes this port. So we initialize a server in the loop to decrease the probability of it.
        int attempts = 42; // The arbitrary number of attempts to avoid an infinity loop.
        while (true) {
            try {
                return initServerAndClient(protocol, createConfig());
            } catch (Exception ex) {
                if (!(ex instanceof BindException)
                        && !findThrowableWithMessage(ex, "Address already in use").isPresent()) {
                    throw ex;
                }

                if (attempts-- < 0) {
                    throw new Exception("Failed to initialize netty server and client", ex);
                }
            }
        }
    }

    static NettyServerAndClient initServerAndClient(NettyProtocol protocol, NettyConfig config)
            throws Exception {

        NettyBufferPool bufferPool = new NettyBufferPool(1);

        final NettyClient client = initClient(config, protocol, bufferPool);
        final NettyServer server = initServer(config, protocol, bufferPool);

        return new NettyServerAndClient(server, client);
    }

    static Channel connect(NettyServerAndClient serverAndClient) throws Exception {
        return connect(serverAndClient.client(), serverAndClient.server());
    }

    static Channel connect(NettyClient client, NettyServer server) throws Exception {
        final NettyConfig config = server.getConfig();

        return client.connect(
                        new InetSocketAddress(config.getServerAddress(), server.getListeningPort()))
                .sync()
                .channel();
    }

    static void awaitClose(Channel ch) throws InterruptedException {
        // Wait for the channel to be closed
        while (ch.isActive()) {
            ch.closeFuture().await(1, TimeUnit.SECONDS);
        }
    }

    static void shutdown(NettyServerAndClient serverAndClient) {
        if (serverAndClient != null) {
            if (serverAndClient.server() != null) {
                serverAndClient.server().shutdown();
            }

            if (serverAndClient.client() != null) {
                serverAndClient.client().shutdown();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // NettyConfig
    // ---------------------------------------------------------------------------------------------

    static NettyConfig createConfig() throws Exception {
        return createConfig(DEFAULT_SEGMENT_SIZE, new Configuration());
    }

    static NettyConfig createConfig(int segmentSize) throws Exception {
        return createConfig(segmentSize, new Configuration());
    }

    static NettyConfig createConfig(Configuration config) throws Exception {
        return createConfig(DEFAULT_SEGMENT_SIZE, config);
    }

    static NettyConfig createConfig(int segmentSize, Configuration config) throws Exception {
        checkArgument(segmentSize > 0);
        checkNotNull(config);

        try (NetUtils.Port port = NetUtils.getAvailablePort()) {
            return new NettyConfig(
                    InetAddress.getLocalHost(), port.getPort(), segmentSize, 1, config);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Encoding & Decoding
    // ---------------------------------------------------------------------------------------------

    static <T extends NettyMessage> T encodeAndDecode(T msg, EmbeddedChannel channel) {
        channel.writeOutbound(msg);
        ByteBuf encoded;
        boolean msgNotEmpty = false;
        while ((encoded = channel.readOutbound()) != null) {
            msgNotEmpty = channel.writeInbound(encoded);
        }
        assertThat(msgNotEmpty).isTrue();

        return channel.readInbound();
    }

    // ---------------------------------------------------------------------------------------------
    // Message Verification
    // ---------------------------------------------------------------------------------------------

    static void verifyErrorResponse(ErrorResponse expected, ErrorResponse actual) {
        assertThat(actual.receiverId).isEqualTo(expected.receiverId);
        assertThat(expected.cause).hasSameClassAs(actual.cause);
        assertThat(expected.cause.getMessage()).isEqualTo(actual.cause.getMessage());

        if (expected.receiverId == null) {
            assertThat(actual.isFatalError()).isTrue();
        }
    }

    static void verifyBufferResponseHeader(BufferResponse expected, BufferResponse actual) {
        assertThat(expected.backlog).isEqualTo(actual.backlog);
        assertThat(expected.sequenceNumber).isEqualTo(actual.sequenceNumber);
        assertThat(expected.bufferSize).isEqualTo(actual.bufferSize);
        assertThat(expected.receiverId).isEqualTo(actual.receiverId);
        assertThat(expected.subpartitionId).isEqualTo(actual.subpartitionId);
    }

    // ------------------------------------------------------------------------

    static final class NettyServerAndClient {

        private final NettyServer server;
        private final NettyClient client;

        NettyServerAndClient(NettyServer server, NettyClient client) {
            this.server = checkNotNull(server);
            this.client = checkNotNull(client);
        }

        NettyServer server() {
            return server;
        }

        NettyClient client() {
            return client;
        }

        ConnectionID getConnectionID(ResourceID resourceID, int connectionIndex) {
            return new ConnectionID(
                    resourceID,
                    new InetSocketAddress(
                            server.getConfig().getServerAddress(), server.getListeningPort()),
                    connectionIndex);
        }
    }

    static final class NoOpProtocol extends NettyProtocol {

        NoOpProtocol() {
            super(null, null);
        }

        @Override
        public ChannelHandler[] getServerChannelHandlers() {
            return new ChannelHandler[0];
        }

        @Override
        public ChannelHandler[] getClientChannelHandlers() {
            return new ChannelHandler[0];
        }
    }
}
