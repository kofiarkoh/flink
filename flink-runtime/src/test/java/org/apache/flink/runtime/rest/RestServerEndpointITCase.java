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

package org.apache.flink.runtime.rest;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.net.SSLUtilsTest;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.legacy.files.WebContentHandlerSpecification;
import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.util.TestRestHandler;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.EndpointNotStartedException;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

/** IT cases for {@link RestClient} and {@link RestServerEndpoint}. */
@ExtendWith(ParameterizedTestExtension.class)
public class RestServerEndpointITCase {

    private static final JobID PATH_JOB_ID = new JobID();
    private static final JobID QUERY_JOB_ID = new JobID();
    private static final String JOB_ID_KEY = "jobid";
    private static final Duration timeout = Duration.ofSeconds(10L);
    private static final int TEST_REST_MAX_CONTENT_LENGTH = 4096;

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @TempDir java.nio.file.Path tempFolder;

    private RestServerEndpoint serverEndpoint;
    private RestClient restClient;
    private TestUploadHandler testUploadHandler;
    private InetSocketAddress serverAddress;

    private final Configuration config;
    private SSLContext defaultSSLContext;
    private SSLSocketFactory defaultSSLSocketFactory;

    private TestHandler testHandler;

    public RestServerEndpointITCase(final Configuration config) {
        this.config = requireNonNull(config);
    }

    @Parameters
    public static Collection<Object[]> data() throws Exception {
        final Configuration config = getBaseConfig();

        final String truststorePath = getTestResource("local127.truststore").getAbsolutePath();
        final String keystorePath = getTestResource("local127.keystore").getAbsolutePath();

        final Configuration sslConfig = new Configuration(config);
        sslConfig.set(SecurityOptions.SSL_REST_ENABLED, true);
        sslConfig.set(SecurityOptions.SSL_REST_TRUSTSTORE, truststorePath);
        sslConfig.set(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "password");
        sslConfig.set(SecurityOptions.SSL_REST_KEYSTORE, keystorePath);
        sslConfig.set(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "password");
        sslConfig.set(SecurityOptions.SSL_REST_KEY_PASSWORD, "password");

        final Configuration sslRestAuthConfig = new Configuration(sslConfig);
        sslRestAuthConfig.set(SecurityOptions.SSL_REST_AUTHENTICATION_ENABLED, true);

        final Configuration sslPinningRestAuthConfig = new Configuration(sslRestAuthConfig);
        sslPinningRestAuthConfig.set(
                SecurityOptions.SSL_REST_CERT_FINGERPRINT,
                SSLUtilsTest.getRestCertificateFingerprint(sslPinningRestAuthConfig, "flink.test"));

        return Arrays.asList(
                new Object[][] {
                    {config}, {sslConfig}, {sslRestAuthConfig}, {sslPinningRestAuthConfig}
                });
    }

    private static Configuration getBaseConfig() {
        final String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();

        final Configuration config = new Configuration();
        config.set(RestOptions.BIND_PORT, "0");
        config.set(RestOptions.BIND_ADDRESS, loopbackAddress);
        config.set(RestOptions.ADDRESS, loopbackAddress);
        config.set(RestOptions.SERVER_MAX_CONTENT_LENGTH, TEST_REST_MAX_CONTENT_LENGTH);
        config.set(RestOptions.CLIENT_MAX_CONTENT_LENGTH, TEST_REST_MAX_CONTENT_LENGTH);
        return config;
    }

    @BeforeEach
    void setup() throws Exception {
        config.set(WebOptions.UPLOAD_DIR, tempFolder.toUri().getPath());

        defaultSSLContext = SSLContext.getDefault();
        defaultSSLSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
        final SSLContext sslClientContext = SSLUtils.createRestSSLContext(config, true);
        if (sslClientContext != null) {
            SSLContext.setDefault(sslClientContext);
            HttpsURLConnection.setDefaultSSLSocketFactory(sslClientContext.getSocketFactory());
        }

        RestfulGateway mockRestfulGateway = new TestingRestfulGateway.Builder().build();

        final GatewayRetriever<RestfulGateway> mockGatewayRetriever =
                () -> CompletableFuture.completedFuture(mockRestfulGateway);

        testHandler = new TestHandler(mockGatewayRetriever, RpcUtils.INF_TIMEOUT);

        TestVersionHandler testVersionHandler =
                new TestVersionHandler(mockGatewayRetriever, RpcUtils.INF_TIMEOUT);

        TestRestHandler<RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters>
                testVersionSelectionHandler1 =
                        new TestRestHandler<>(
                                mockGatewayRetriever,
                                TestVersionSelectionHeaders1.INSTANCE,
                                FutureUtils.completedExceptionally(
                                        new RestHandlerException(
                                                "test failure 1", HttpResponseStatus.OK)));

        TestRestHandler<RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters>
                testVersionSelectionHandler2 =
                        new TestRestHandler<>(
                                mockGatewayRetriever,
                                TestVersionSelectionHeaders2.INSTANCE,
                                FutureUtils.completedExceptionally(
                                        new RestHandlerException(
                                                "test failure 2", HttpResponseStatus.ACCEPTED)));

        testUploadHandler = new TestUploadHandler(mockGatewayRetriever, RpcUtils.INF_TIMEOUT);

        final StaticFileServerHandler<RestfulGateway> staticFileServerHandler =
                new StaticFileServerHandler<>(
                        mockGatewayRetriever, RpcUtils.INF_TIMEOUT, tempFolder.toFile());

        serverEndpoint =
                TestRestServerEndpoint.builder(config)
                        .withHandler(new TestHeaders(), testHandler)
                        .withHandler(TestUploadHeaders.INSTANCE, testUploadHandler)
                        .withHandler(testVersionHandler)
                        .withHandler(testVersionSelectionHandler1)
                        .withHandler(testVersionSelectionHandler2)
                        .withHandler(
                                WebContentHandlerSpecification.getInstance(),
                                staticFileServerHandler)
                        .withHandler(new TestUnavailableHandler(mockGatewayRetriever))
                        .buildAndStart();
        restClient = new RestClient(config, EXECUTOR_EXTENSION.getExecutor());

        serverAddress = serverEndpoint.getServerAddress();
    }

    @AfterEach
    void teardown() throws Exception {
        if (defaultSSLContext != null) {
            SSLContext.setDefault(defaultSSLContext);
            HttpsURLConnection.setDefaultSSLSocketFactory(defaultSSLSocketFactory);
        }

        if (restClient != null) {
            restClient.shutdown(timeout);
            restClient = null;
        }

        if (serverEndpoint != null) {
            serverEndpoint.closeAsync().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            serverEndpoint = null;
        }
    }

    /**
     * Tests that request are handled as individual units which don't interfere with each other.
     * This means that request responses can overtake each other.
     */
    @TestTemplate
    void testRequestInterleaving() throws Exception {
        final BlockerSync sync = new BlockerSync();
        testHandler.handlerBody =
                id -> {
                    if (id == 1) {
                        try {
                            sync.block();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return CompletableFuture.completedFuture(new TestResponse(id));
                };

        // send first request and wait until the handler blocks
        final CompletableFuture<TestResponse> response1 =
                sendRequestToTestHandler(new TestRequest(1));
        sync.awaitBlocker();

        // send second request and verify response
        final CompletableFuture<TestResponse> response2 =
                sendRequestToTestHandler(new TestRequest(2));
        assertThat(response2.get().id).isEqualTo(2);

        // wake up blocked handler
        sync.releaseBlocker();

        // verify response to first request
        assertThat(response1.get().id).isOne();
    }

    /**
     * Tests that a bad handler request (HandlerRequest cannot be created) is reported as a
     * BAD_REQUEST and not an internal server error.
     *
     * <p>See FLINK-7663
     */
    @TestTemplate
    void testBadHandlerRequest() throws Exception {
        final FaultyTestParameters parameters = new FaultyTestParameters();

        parameters.faultyJobIDPathParameter.resolve(PATH_JOB_ID);
        ((TestParameters) parameters)
                .jobIDQueryParameter.resolve(Collections.singletonList(QUERY_JOB_ID));

        CompletableFuture<TestResponse> response =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        new TestHeaders(),
                        parameters,
                        new TestRequest(2));
        assertThatFuture(response)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(RestClientException.class)
                .satisfies(
                        e ->
                                assertThat(
                                                ((RestClientException) e.getCause())
                                                        .getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.BAD_REQUEST));
    }

    /** Tests that requests larger than {@link #TEST_REST_MAX_CONTENT_LENGTH} are rejected. */
    @TestTemplate
    void testShouldRespectMaxContentLengthLimitForRequests() throws Exception {
        testHandler.handlerBody =
                id -> {
                    throw new AssertionError("Request should not arrive at server.");
                };

        assertThatFuture(
                        sendRequestToTestHandler(
                                new TestRequest(
                                        2, createStringOfSize(TEST_REST_MAX_CONTENT_LENGTH))))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(RestClientException.class)
                .withMessageContaining("Try to raise");
    }

    /** Tests that responses larger than {@link #TEST_REST_MAX_CONTENT_LENGTH} are rejected. */
    @TestTemplate
    void testShouldRespectMaxContentLengthLimitForResponses() throws Exception {
        testHandler.handlerBody =
                id ->
                        CompletableFuture.completedFuture(
                                new TestResponse(
                                        id, createStringOfSize(TEST_REST_MAX_CONTENT_LENGTH)));
        assertThatFuture(sendRequestToTestHandler(new TestRequest(1)))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(TooLongFrameException.class)
                .withMessageContaining("Try to raise");
    }

    /**
     * Tests that multipart/form-data uploads work correctly.
     *
     * @see FileUploadHandler
     */
    @TestTemplate
    void testFileUpload() throws Exception {
        final String boundary = generateMultiPartBoundary();
        final String uploadedContent = "hello";
        final HttpURLConnection connection =
                openHttpConnectionForUpload(
                        boundary, TestUploadHeaders.INSTANCE.getTargetRestEndpointURL());

        uploadFile(connection, uploadedContent, boundary);

        assertThat(connection.getResponseCode()).isEqualTo(200);
        final byte[] lastUploadedFileContents = testUploadHandler.getLastUploadedFileContents();
        assertThat(uploadedContent)
                .isEqualTo(new String(lastUploadedFileContents, StandardCharsets.UTF_8));
    }

    /**
     * Tests that when a handler is marked as not accepting file uploads we (1) return an error and
     * (2) don't upload the file to the upload directory.
     */
    @TestTemplate
    void testFileUploadLimitedToAllowedUris() throws Exception {
        final String boundary = generateMultiPartBoundary();
        final File uploadDir = new File(tempFolder.toString(), "flink-web-upload");
        final File[] preUploadFiles = uploadDir.listFiles();

        // We need a handler that does not accept file uploads for this test
        assertThat(TestVersionHeaders.INSTANCE.acceptsFileUploads()).isFalse();
        String uri = TestVersionHeaders.INSTANCE.getTargetRestEndpointURL();

        final HttpURLConnection connection = openHttpConnectionForUpload(boundary, uri);

        uploadFile(connection, "hello", boundary);

        assertThat(connection.getResponseCode()).isEqualTo(400);

        // This is the important check. We don't want additional files when the handler does
        // not accept file uploads.
        final File[] postUploadFiles = uploadDir.listFiles();
        assertThat(postUploadFiles).isEqualTo(preUploadFiles);
    }

    /**
     * Sending multipart/form-data without a file should result in a bad request if the handler
     * expects a file upload.
     */
    @TestTemplate
    void testMultiPartFormDataWithoutFileUpload() throws Exception {
        final String boundary = generateMultiPartBoundary();
        final String crlf = "\r\n";
        final HttpURLConnection connection =
                openHttpConnectionForUpload(
                        boundary, TestUploadHeaders.INSTANCE.getTargetRestEndpointURL());

        try (OutputStream output = connection.getOutputStream();
                PrintWriter writer =
                        new PrintWriter(
                                new OutputStreamWriter(output, StandardCharsets.UTF_8), true)) {

            writer.append("--" + boundary).append(crlf);
            writer.append("Content-Disposition: form-data; name=\"foo\"").append(crlf);
            writer.append(crlf).flush();
            output.write("test".getBytes(StandardCharsets.UTF_8));
            output.flush();
            writer.append(crlf).flush();
            writer.append("--" + boundary + "--").append(crlf).flush();
        }

        assertThat(connection.getResponseCode()).isEqualTo(400);
    }

    /** Tests that files can be served with the {@link StaticFileServerHandler}. */
    @TestTemplate
    void testStaticFileServerHandler() throws Exception {
        File file = TempDirUtils.newFile(tempFolder);
        Files.write(file.toPath(), Collections.singletonList("foobar"));

        final URL url = new URL(serverEndpoint.getRestBaseUrl() + "/" + file.getName());
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        final String fileContents = IOUtils.toString(connection.getInputStream());

        assertThat(fileContents.trim()).isEqualTo("foobar");
    }

    @TestTemplate
    void testVersioning() throws Exception {
        CompletableFuture<EmptyResponseBody> unspecifiedVersionResponse =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        TestVersionHeaders.INSTANCE,
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        Collections.emptyList());

        unspecifiedVersionResponse.get(5, TimeUnit.SECONDS);

        CompletableFuture<EmptyResponseBody> specifiedVersionResponse =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        TestVersionHeaders.INSTANCE,
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        Collections.emptyList(),
                        RuntimeRestAPIVersion.V1);

        specifiedVersionResponse.get(5, TimeUnit.SECONDS);
    }

    @TestTemplate
    void testVersionSelection() throws Exception {
        CompletableFuture<EmptyResponseBody> version1Response =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        TestVersionSelectionHeaders1.INSTANCE,
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        Collections.emptyList(),
                        RuntimeRestAPIVersion.V0);

        assertThatFuture(version1Response)
                .failsWithin(5, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RestClientException.class)
                .satisfies(
                        e ->
                                assertThat(
                                                ((RestClientException) e.getCause())
                                                        .getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.OK));

        CompletableFuture<EmptyResponseBody> version2Response =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        TestVersionSelectionHeaders2.INSTANCE,
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        Collections.emptyList(),
                        RuntimeRestAPIVersion.V1);

        assertThatFuture(version2Response)
                .failsWithin(5, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RestClientException.class)
                .satisfies(
                        e ->
                                assertThat(
                                                ((RestClientException) e.getCause())
                                                        .getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.ACCEPTED));
    }

    @TestTemplate
    void testDefaultVersionRouting() throws Exception {
        assumeThat(config.get(SecurityOptions.SSL_REST_ENABLED))
                .as("Ignoring SSL-enabled test to keep OkHttp usage simple.")
                .isFalse();

        OkHttpClient client = new OkHttpClient();

        final Request request =
                new Request.Builder()
                        .url(
                                serverEndpoint.getRestBaseUrl()
                                        + TestVersionSelectionHeaders2.INSTANCE
                                                .getTargetRestEndpointURL())
                        .build();

        try (final Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
        }
    }

    @TestTemplate
    void testNonSslRedirectForEnabledSsl() throws Exception {
        assumeThat(config.get(SecurityOptions.SSL_REST_ENABLED)).isTrue();

        OkHttpClient client = new OkHttpClient.Builder().followRedirects(false).build();
        String httpsUrl = serverEndpoint.getRestBaseUrl() + "/path";
        String httpUrl = httpsUrl.replace("https://", "http://");
        Request request = new Request.Builder().url(httpUrl).build();
        try (final Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(HttpResponseStatus.MOVED_PERMANENTLY.code());
            assertThat(response.headers().names()).contains("location");
            assertThat(response.header("location")).isEqualTo(httpsUrl);
        }
    }

    /**
     * Tests that after calling {@link RestServerEndpoint#closeAsync()}, the handlers are closed
     * first, and we wait for in-flight requests to finish. As long as not all handlers are closed,
     * HTTP requests should be served.
     */
    @TestTemplate
    void testShouldWaitForHandlersWhenClosing() throws Exception {
        testHandler.closeFuture = new CompletableFuture<>();
        final BlockerSync sync = new BlockerSync();
        testHandler.handlerBody =
                id -> {
                    // Intentionally schedule the work on a different thread. This is to simulate
                    // handlers where the CompletableFuture is finished by the RPC framework.
                    return CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    sync.block();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return new TestResponse(id);
                            });
                };

        // Initiate closing RestServerEndpoint but the test handler should block.
        final CompletableFuture<Void> closeRestServerEndpointFuture = serverEndpoint.closeAsync();
        assertThat(closeRestServerEndpointFuture).isNotDone();

        // create an in-flight request
        final CompletableFuture<TestResponse> request =
                sendRequestToTestHandler(new TestRequest(1));
        sync.awaitBlocker();

        // Allow handler to close but there is still one in-flight request which should prevent
        // the RestServerEndpoint from closing.
        testHandler.closeFuture.complete(null);
        assertThat(closeRestServerEndpointFuture).isNotDone();

        // Finish the in-flight request.
        sync.releaseBlocker();

        request.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        closeRestServerEndpointFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Tests that new requests are ignored after a handler is shut down. */
    @TestTemplate
    void testRequestsRejectedAfterShutdownOfHandlerIsCompleted() throws Exception {
        testHandler.handlerBody =
                id -> CompletableFuture.completedFuture(new TestResponse(id, "foobar"));

        // let the test upload handler block the shut down of the RestServerEndpoint
        testUploadHandler.closeFuture = new CompletableFuture<>();

        final CompletableFuture<Void> closeRestServerEndpointFuture = serverEndpoint.closeAsync();

        assertThat(closeRestServerEndpointFuture).isNotDone();

        // wait until the TestHandler is closed
        testHandler.closeLatch.await();

        // requests to the TestHandler should now get rejected
        final CompletableFuture<TestResponse> request =
                sendRequestToTestHandler(new TestRequest(1));

        try {
            request.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail("Expected a ConnectionClosedException");
        } catch (ExecutionException ee) {
            if (!ExceptionUtils.findThrowable(ee, ConnectionClosedException.class).isPresent()) {
                throw ee;
            }
        } finally {
            testUploadHandler.closeFuture.complete(null);
            closeRestServerEndpointFuture.get();
        }
    }

    @TestTemplate
    void testRestServerBindPort() throws Exception {
        final int portRangeStart = 52300;
        final int portRangeEnd = 52400;
        final Configuration config = new Configuration();
        config.set(RestOptions.ADDRESS, "localhost");
        config.set(RestOptions.BIND_PORT, portRangeStart + "-" + portRangeEnd);

        try (RestServerEndpoint serverEndpoint1 = TestRestServerEndpoint.builder(config).build();
                RestServerEndpoint serverEndpoint2 =
                        TestRestServerEndpoint.builder(config).build()) {

            serverEndpoint1.start();
            serverEndpoint2.start();

            assertThat(serverEndpoint1.getServerAddress().getPort())
                    .isNotEqualTo(serverEndpoint2.getServerAddress().getPort());

            assertThat(serverEndpoint1.getServerAddress().getPort())
                    .isGreaterThanOrEqualTo(portRangeStart);
            assertThat(serverEndpoint1.getServerAddress().getPort())
                    .isLessThanOrEqualTo(portRangeEnd);

            assertThat(serverEndpoint2.getServerAddress().getPort())
                    .isGreaterThanOrEqualTo(portRangeStart);
            assertThat(serverEndpoint2.getServerAddress().getPort())
                    .isLessThanOrEqualTo(portRangeEnd);
        }
    }

    @TestTemplate
    void testEndpointsMustBeUnique() throws Exception {
        assertThrows(
                "REST handler registration",
                FlinkRuntimeException.class,
                () -> {
                    try (TestRestServerEndpoint restServerEndpoint =
                            TestRestServerEndpoint.builder(config)
                                    .withHandler(new TestHeaders(), testHandler)
                                    .withHandler(new TestHeaders(), testUploadHandler)
                                    .build()) {
                        restServerEndpoint.start();
                        return null;
                    }
                });
    }

    @TestTemplate
    void testDuplicateHandlerRegistrationIsForbidden() throws Exception {
        assertThrows(
                "Duplicate REST handler",
                FlinkRuntimeException.class,
                () -> {
                    try (TestRestServerEndpoint restServerEndpoint =
                            TestRestServerEndpoint.builder(config)
                                    .withHandler(new TestHeaders(), testHandler)
                                    .withHandler(TestUploadHeaders.INSTANCE, testHandler)
                                    .build()) {
                        restServerEndpoint.start();
                        return null;
                    }
                });
    }

    @TestTemplate
    void testOnUnavailableRpcEndpointReturns503() throws IOException {
        CompletableFuture<EmptyResponseBody> response =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        TestUnavailableHeaders.INSTANCE);

        assertThatThrownBy(response::get)
                .extracting(x -> ExceptionUtils.findThrowable(x, RestClientException.class))
                .extracting(Optional::get)
                .extracting(RestClientException::getHttpResponseStatus)
                .isEqualTo(HttpResponseStatus.SERVICE_UNAVAILABLE);
    }

    private static File getTestResource(final String fileName) {
        final ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        final URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException(
                    String.format("Test resource %s does not exist", fileName));
        }
        return new File(resource.getFile());
    }

    private HttpURLConnection openHttpConnectionForUpload(
            final String boundary, final String uploadUri) throws IOException {
        final HttpURLConnection connection =
                (HttpURLConnection)
                        new URL(serverEndpoint.getRestBaseUrl() + uploadUri).openConnection();
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
        return connection;
    }

    private static String generateMultiPartBoundary() {
        return Long.toHexString(System.currentTimeMillis());
    }

    private static String createStringOfSize(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    private static void uploadFile(HttpURLConnection connection, String content, String boundary)
            throws IOException {
        final String crlf = "\r\n";
        try (OutputStream output = connection.getOutputStream();
                PrintWriter writer =
                        new PrintWriter(
                                new OutputStreamWriter(output, StandardCharsets.UTF_8), true)) {

            writer.append("--" + boundary).append(crlf);
            writer.append("Content-Disposition: form-data; name=\"foo\"; filename=\"bar\"")
                    .append(crlf);
            writer.append("Content-Type: plain/text; charset=utf8").append(crlf);
            writer.append(crlf).flush();
            output.write(content.getBytes(StandardCharsets.UTF_8));
            output.flush();
            writer.append(crlf).flush();
            writer.append("--" + boundary + "--").append(crlf).flush();
        }
    }

    private static class TestHandler
            extends AbstractRestHandler<RestfulGateway, TestRequest, TestResponse, TestParameters> {

        private final OneShotLatch closeLatch = new OneShotLatch();

        private CompletableFuture<Void> closeFuture = CompletableFuture.completedFuture(null);

        private Function<Integer, CompletableFuture<TestResponse>> handlerBody;

        TestHandler(GatewayRetriever<RestfulGateway> leaderRetriever, Duration timeout) {
            super(leaderRetriever, timeout, Collections.emptyMap(), new TestHeaders());
        }

        @Override
        protected CompletableFuture<TestResponse> handleRequest(
                @Nonnull HandlerRequest<TestRequest> request, RestfulGateway gateway) {
            assertThat(request.getPathParameter(JobIDPathParameter.class)).isEqualTo(PATH_JOB_ID);
            assertThat(request.getQueryParameter(JobIDQueryParameter.class).get(0))
                    .isEqualTo(QUERY_JOB_ID);

            final int id = request.getRequestBody().id;
            return handlerBody.apply(id);
        }

        @Override
        public CompletableFuture<Void> closeHandlerAsync() {
            closeLatch.trigger();
            return closeFuture;
        }
    }

    private CompletableFuture<TestResponse> sendRequestToTestHandler(
            final TestRequest testRequest) {
        try {
            return restClient.sendRequest(
                    serverAddress.getHostName(),
                    serverAddress.getPort(),
                    new TestHeaders(),
                    createTestParameters(),
                    testRequest);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static TestParameters createTestParameters() {
        final TestParameters parameters = new TestParameters();
        parameters.jobIDPathParameter.resolve(PATH_JOB_ID);
        parameters.jobIDQueryParameter.resolve(Collections.singletonList(QUERY_JOB_ID));
        return parameters;
    }

    private static class TestRequest implements RequestBody {
        public final int id;

        public final String content;

        public TestRequest(int id) {
            this(id, null);
        }

        @JsonCreator
        public TestRequest(
                @JsonProperty("id") int id, @JsonProperty("content") final String content) {
            this.id = id;
            this.content = content;
        }
    }

    private static class TestResponse implements ResponseBody {

        public final int id;

        public final String content;

        public TestResponse(int id) {
            this(id, null);
        }

        @JsonCreator
        public TestResponse(@JsonProperty("id") int id, @JsonProperty("content") String content) {
            this.id = id;
            this.content = content;
        }
    }

    private static class TestHeaders
            implements RuntimeMessageHeaders<TestRequest, TestResponse, TestParameters> {

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.POST;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/test/:jobid";
        }

        @Override
        public Class<TestRequest> getRequestClass() {
            return TestRequest.class;
        }

        @Override
        public Class<TestResponse> getResponseClass() {
            return TestResponse.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "";
        }

        @Override
        public TestParameters getUnresolvedMessageParameters() {
            return new TestParameters();
        }
    }

    private static class TestParameters extends MessageParameters {
        private final JobIDPathParameter jobIDPathParameter = new JobIDPathParameter();
        private final JobIDQueryParameter jobIDQueryParameter = new JobIDQueryParameter();

        @Override
        public Collection<MessagePathParameter<?>> getPathParameters() {
            return Collections.singleton(jobIDPathParameter);
        }

        @Override
        public Collection<MessageQueryParameter<?>> getQueryParameters() {
            return Collections.singleton(jobIDQueryParameter);
        }
    }

    private static @ExtendWith(CTestJUnit5Extension.class) @CTestClass class FaultyTestParameters
            extends TestParameters {
        private final FaultyJobIDPathParameter faultyJobIDPathParameter =
                new FaultyJobIDPathParameter();

        @Override
        public Collection<MessagePathParameter<?>> getPathParameters() {
            return Collections.singleton(faultyJobIDPathParameter);
        }
    }

    static class JobIDPathParameter extends MessagePathParameter<JobID> {
        JobIDPathParameter() {
            super(JOB_ID_KEY);
        }

        @Override
        public JobID convertFromString(String value) {
            return JobID.fromHexString(value);
        }

        @Override
        protected String convertToString(JobID value) {
            return value.toString();
        }

        @Override
        public String getDescription() {
            return "correct JobID parameter";
        }
    }

    static class FaultyJobIDPathParameter extends MessagePathParameter<JobID> {

        FaultyJobIDPathParameter() {
            super(JOB_ID_KEY);
        }

        @Override
        protected JobID convertFromString(String value) throws ConversionException {
            return JobID.fromHexString(value);
        }

        @Override
        protected String convertToString(JobID value) {
            return "foobar";
        }

        @Override
        public String getDescription() {
            return "faulty JobID parameter";
        }
    }

    static class JobIDQueryParameter extends MessageQueryParameter<JobID> {
        JobIDQueryParameter() {
            super(JOB_ID_KEY, MessageParameterRequisiteness.MANDATORY);
        }

        @Override
        public JobID convertStringToValue(String value) {
            return JobID.fromHexString(value);
        }

        @Override
        public String convertValueToString(JobID value) {
            return value.toString();
        }

        @Override
        public String getDescription() {
            return "query JobID parameter";
        }
    }

    private static class TestUploadHandler
            extends AbstractRestHandler<
                    RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

        private volatile CompletableFuture<Void> closeFuture =
                CompletableFuture.completedFuture(null);

        private volatile byte[] lastUploadedFileContents;

        private TestUploadHandler(
                final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                final Duration timeout) {
            super(leaderRetriever, timeout, Collections.emptyMap(), TestUploadHeaders.INSTANCE);
        }

        @Override
        protected CompletableFuture<EmptyResponseBody> handleRequest(
                @Nonnull final HandlerRequest<EmptyRequestBody> request,
                @Nonnull final RestfulGateway gateway)
                throws RestHandlerException {
            Collection<Path> uploadedFiles =
                    request.getUploadedFiles().stream()
                            .map(File::toPath)
                            .collect(Collectors.toList());
            if (uploadedFiles.size() != 1) {
                throw new RestHandlerException(
                        "Expected 1 file, received " + uploadedFiles.size() + '.',
                        HttpResponseStatus.BAD_REQUEST);
            }

            try {
                lastUploadedFileContents = Files.readAllBytes(uploadedFiles.iterator().next());
            } catch (IOException e) {
                throw new RestHandlerException(
                        "Could not read contents of uploaded file.",
                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        e);
            }
            return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
        }

        public byte[] getLastUploadedFileContents() {
            return lastUploadedFileContents;
        }

        @Override
        protected CompletableFuture<Void> closeHandlerAsync() {
            return closeFuture;
        }
    }

    static class TestVersionHandler
            extends AbstractRestHandler<
                    RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

        TestVersionHandler(
                final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                final Duration timeout) {
            super(leaderRetriever, timeout, Collections.emptyMap(), TestVersionHeaders.INSTANCE);
        }

        @Override
        protected CompletableFuture<EmptyResponseBody> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
        }
    }

    enum TestVersionHeaders
            implements
                    RuntimeMessageHeaders<
                            EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {
        INSTANCE;

        @Override
        public Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.GET;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/test/versioning";
        }

        @Override
        public Class<EmptyResponseBody> getResponseClass() {
            return EmptyResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return null;
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }

        @Override
        public Collection<RuntimeRestAPIVersion> getSupportedAPIVersions() {
            return Collections.singleton(RuntimeRestAPIVersion.V1);
        }
    }

    private interface TestVersionSelectionHeadersBase
            extends MessageHeaders<EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

        @Override
        default Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        default HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.GET;
        }

        @Override
        default String getTargetRestEndpointURL() {
            return "/test/select-version";
        }

        @Override
        default Class<EmptyResponseBody> getResponseClass() {
            return EmptyResponseBody.class;
        }

        @Override
        default HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        default String getDescription() {
            return null;
        }

        @Override
        default EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }
    }

    private enum TestVersionSelectionHeaders1 implements TestVersionSelectionHeadersBase {
        INSTANCE;

        @Override
        public Collection<RuntimeRestAPIVersion> getSupportedAPIVersions() {
            return Collections.singleton(RuntimeRestAPIVersion.V0);
        }
    }

    private enum TestVersionSelectionHeaders2 implements TestVersionSelectionHeadersBase {
        INSTANCE;

        @Override
        public Collection<RuntimeRestAPIVersion> getSupportedAPIVersions() {
            return Collections.singleton(RuntimeRestAPIVersion.V1);
        }
    }

    private enum TestUploadHeaders
            implements
                    RuntimeMessageHeaders<
                            EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {
        INSTANCE;

        @Override
        public Class<EmptyResponseBody> getResponseClass() {
            return EmptyResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.POST;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/upload";
        }

        @Override
        public String getDescription() {
            return "";
        }

        @Override
        public boolean acceptsFileUploads() {
            return true;
        }
    }

    private enum TestUnavailableHeaders
            implements
                    RuntimeMessageHeaders<
                            EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {
        INSTANCE;

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.GET;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/unavailable";
        }

        @Override
        public Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        public Class<EmptyResponseBody> getResponseClass() {
            return EmptyResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "";
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }
    }

    private static class TestUnavailableHandler
            extends TestRestHandler<
                    RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

        protected TestUnavailableHandler(GatewayRetriever<RestfulGateway> leaderRetriever) {
            super(
                    leaderRetriever,
                    TestUnavailableHeaders.INSTANCE,
                    FutureUtils.completedExceptionally(
                            new EndpointNotStartedException("test exception")));
        }
    }
}
