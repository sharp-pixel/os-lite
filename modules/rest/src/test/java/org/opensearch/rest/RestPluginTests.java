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
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginResources;
import org.opensearch.rest.spi.RestHandler;
import org.opensearch.rest.spi.RestHandlerPlugin;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class RestPluginTests extends RandomizedTest {
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
}
