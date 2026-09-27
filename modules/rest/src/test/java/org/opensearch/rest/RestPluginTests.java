/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginResources;
import org.opensearch.rest.spi.RestHandler;
import org.opensearch.rest.spi.RestHandlerPlugin;
import org.opensearch.rest.spi.RestHeaderDefinition;
import org.opensearch.rest.spi.RestSecurityExtension;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class RestPluginTests extends RandomizedTest {
    public void testMalformedParametersAndContentTypeReturnBadRequest() throws Exception {
        assertMalformedRequest("/test?value=%", "invalid");
    }

    public void testMalformedChannelParametersAndContentTypeReturnBadRequest() throws Exception {
        assertMalformedRequest("/test?pretty=invalid", "invalid");
    }

    public void testMalformedParametersAloneReturnBadRequest() throws Exception {
        assertMalformedRequest("/test?value=%", "application/json");
    }

    private void assertMalformedRequest(String uri, String contentType) throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            RestPlugin plugin = new RestPlugin();
            plugin.createComponents(new PluginResources(NamedXContentRegistry.EMPTY, null, null, null, null));
            HttpServerTransport.Dispatcher dispatcher = plugin.getHttpServerTransportDispatcher(
                BigArrays.NON_RECYCLING_INSTANCE,
                Settings.EMPTY,
                new NoneCircuitBreakerService(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            ).orElseThrow();
            HttpRequest request = malformedRequest(uri, Map.of("Content-Type", List.of(contentType)));
            HttpRequest withoutContentType = malformedRequest(uri, Map.of());
            when(request.removeHeader("Content-Type")).thenReturn(withoutContentType);
            when(request.createResponse(any(), any())).thenReturn(mock(HttpResponse.class));
            HttpChannel channel = mock(HttpChannel.class);
            doAnswer(invocation -> {
                ActionListener<Void> listener = invocation.getArgument(1);
                listener.onResponse(null);
                return null;
            }).when(channel).sendResponse(any(), any());
            dispatcher.dispatchRequest(request, channel, context);
            verify(request).createResponse(eq(RestStatus.BAD_REQUEST), any());
            verify(request).release();
            verify(channel).sendResponse(any(), any());
        }
    }

    private static HttpRequest malformedRequest(String uri, Map<String, List<String>> headers) {
        HttpRequest request = mock(HttpRequest.class);
        when(request.method()).thenReturn(HttpRequest.Method.GET);
        when(request.uri()).thenReturn(uri);
        when(request.content()).thenReturn(BytesArray.EMPTY);
        when(request.protocolVersion()).thenReturn(HttpRequest.HttpVersion.HTTP_1_1);
        when(request.getHeaders()).thenReturn(headers);
        return request;
    }

    public void testCopiesOpaqueIdToHandlerContext() throws Exception {
        assertHeaderDispatch(List.of("request-42"), "request-42", 1);
    }

    public void testAcceptsRequestsWithoutOpaqueId() throws Exception {
        assertHeaderDispatch(List.of(), null, 1);
    }

    public void testAcceptsRepeatedIdenticalOpaqueId() throws Exception {
        assertHeaderDispatch(List.of("request-42", "request-42"), "request-42", 1);
    }

    public void testRejectsConflictingOpaqueIdsBeforeCallingHandler() throws Exception {
        assertHeaderDispatch(List.of("first", "second"), null, 0);
    }

    public void testSecurityExtensionCopiesAuthenticationHeaderBeforeWrapper() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(context);
        AtomicReference<String> captured = new AtomicReference<>();
        AtomicInteger wrapperCalls = new AtomicInteger();
        RestPlugin plugin = new RestPlugin();
        plugin.accept(new SecurityPlugin(captured, wrapperCalls));
        plugin.accept(new HandlerPlugin(List.of(new RestHandler() {
            @Override
            public void handleRequest(
                org.opensearch.rest.spi.RestRequest request,
                org.opensearch.rest.spi.RestChannel channel,
                org.opensearch.transport.client.node.NodeClient client
            ) {
                channel.sendResponse(
                    new org.opensearch.rest.spi.BytesRestResponse(org.opensearch.core.rest.RestStatus.OK, "text/plain", "ok")
                );
            }

            @Override
            public List<Route> routes() {
                return List.of(new Route(HttpRequest.Method.GET, "/secure"));
            }
        })));
        plugin.createComponents(new PluginResources(NamedXContentRegistry.EMPTY, null, null, threadPool, null));
        HttpServerTransport.Dispatcher dispatcher = plugin.getHttpServerTransportDispatcher(
            BigArrays.NON_RECYCLING_INSTANCE,
            Settings.EMPTY,
            new NoneCircuitBreakerService(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            NoopTracer.INSTANCE
        ).orElseThrow();
        HttpRequest request = mock(HttpRequest.class);
        when(request.method()).thenReturn(HttpRequest.Method.GET);
        when(request.uri()).thenReturn("/secure");
        when(request.content()).thenReturn(BytesArray.EMPTY);
        when(request.protocolVersion()).thenReturn(HttpRequest.HttpVersion.HTTP_1_1);
        when(request.getHeaders()).thenReturn(Map.of("Authorization", List.of("Bearer token")));
        when(request.allHeaders("Authorization")).thenReturn(List.of("Bearer token"));
        when(request.releaseAndCopy()).thenReturn(request);
        when(request.createResponse(any(), any())).thenReturn(mock(HttpResponse.class));
        HttpChannel channel = mock(HttpChannel.class);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(channel).sendResponse(any(), any());

        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            dispatcher.dispatchRequest(request, channel, context);
        }

        assertEquals(1, wrapperCalls.get());
        assertEquals("Bearer token", captured.get());
    }

    public void testRejectsMultipleSecurityWrappers() {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(context);
        RestPlugin plugin = new RestPlugin();
        plugin.accept(new SecurityPlugin(new AtomicReference<>(), new AtomicInteger()));
        plugin.accept(new SecurityPlugin(new AtomicReference<>(), new AtomicInteger()));
        plugin.createComponents(new PluginResources(NamedXContentRegistry.EMPTY, null, null, threadPool, null));

        assertThrows(
            IllegalArgumentException.class,
            () -> plugin.getHttpServerTransportDispatcher(
                BigArrays.NON_RECYCLING_INSTANCE,
                Settings.EMPTY,
                new NoneCircuitBreakerService(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            )
        );
    }

    private void assertHeaderDispatch(List<String> values, String expectedHeader, int expectedCalls) throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            RestPlugin plugin = new RestPlugin();
            AtomicReference<String> captured = new AtomicReference<>();
            AtomicInteger calls = new AtomicInteger();
            plugin.accept(new HandlerPlugin(List.of(new RestHandler() {
                @Override
                public void handleRequest(
                    org.opensearch.rest.spi.RestRequest request,
                    org.opensearch.rest.spi.RestChannel channel,
                    org.opensearch.transport.client.node.NodeClient client
                ) {
                    calls.incrementAndGet();
                    captured.set(context.getHeader(Task.X_OPAQUE_ID));
                    channel.sendResponse(
                        new org.opensearch.rest.spi.BytesRestResponse(org.opensearch.core.rest.RestStatus.OK, "text/plain", "ok")
                    );
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(HttpRequest.Method.GET, "/test"));
                }
            })));
            plugin.createComponents(new PluginResources(NamedXContentRegistry.EMPTY, null, null, null, null));
            HttpServerTransport.Dispatcher dispatcher = plugin.getHttpServerTransportDispatcher(
                BigArrays.NON_RECYCLING_INSTANCE,
                Settings.EMPTY,
                new NoneCircuitBreakerService(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            ).orElseThrow();
            HttpRequest request = mock(HttpRequest.class);
            when(request.method()).thenReturn(HttpRequest.Method.GET);
            when(request.uri()).thenReturn("/test");
            when(request.content()).thenReturn(BytesArray.EMPTY);
            when(request.protocolVersion()).thenReturn(HttpRequest.HttpVersion.HTTP_1_1);
            when(request.getHeaders()).thenReturn(values.isEmpty() ? Map.of() : Map.of(Task.X_OPAQUE_ID, values));
            when(request.allHeaders(Task.X_OPAQUE_ID)).thenReturn(values);
            when(request.header(Task.X_OPAQUE_ID)).thenReturn(values.isEmpty() ? null : values.get(0));
            when(request.releaseAndCopy()).thenReturn(request);
            when(request.createResponse(any(), any())).thenReturn(mock(HttpResponse.class));
            HttpChannel channel = mock(HttpChannel.class);
            doAnswer(invocation -> {
                ActionListener<Void> listener = invocation.getArgument(1);
                listener.onResponse(null);
                return null;
            }).when(channel).sendResponse(any(), any());
            dispatcher.dispatchRequest(request, channel, context);
            assertEquals(expectedCalls, calls.get());
            assertEquals(expectedHeader, captured.get());
            if (expectedCalls == 0) {
                org.mockito.Mockito.verify(request).createResponse(eq(org.opensearch.core.rest.RestStatus.BAD_REQUEST), any());
                assertNull(context.getHeader(Task.X_OPAQUE_ID));
            }
        }
    }

    private static class HandlerPlugin extends Plugin implements RestHandlerPlugin {
        private final List<RestHandler> handlers;

        HandlerPlugin(List<RestHandler> handlers) {
            this.handlers = handlers;
        }

        @Override
        public List<RestHandler> getRestHandlers() {
            return handlers;
        }
    }

    private static class SecurityPlugin extends Plugin implements RestSecurityExtension {
        private final AtomicReference<String> captured;
        private final AtomicInteger calls;

        SecurityPlugin(AtomicReference<String> captured, AtomicInteger calls) {
            this.captured = captured;
            this.calls = calls;
        }

        @Override
        public Collection<RestHeaderDefinition> getRestHeaders() {
            return List.of(new RestHeaderDefinition("Authorization", false));
        }

        @Override
        public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext, Set<RestHeaderDefinition> headersToCopy) {
            return original -> (request, channel, client) -> {
                calls.incrementAndGet();
                captured.set(threadContext.getHeader("Authorization"));
                original.handleRequest(request, channel, client);
            };
        }
    }
}
