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

import org.opensearch.Version;
import org.opensearch.api.indexing.IndexingApiPlugin;
import org.opensearch.api.management.ManagementApiPlugin;
import org.opensearch.api.search.SearchApiPlugin;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.helloworld.HelloWorldPlugin;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginResources;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rest.spi.RestApiPlugin;
import org.opensearch.rest.spi.RestHandler;
import org.opensearch.rest.spi.RestHandlerPlugin;
import org.opensearch.rest.spi.RestOperationCategory;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class RestApiCategoriesTests extends RandomizedTest {
    public void testDefaultEnablesEveryCategory() throws Exception {
        for (RestOperationCategory category : RestOperationCategory.values()) {
            assertDispatch(category, Settings.EMPTY, HttpRequest.Method.POST, "/test", RestStatus.OK);
        }
    }

    public void testSearchNodeAcceptsPostSearchAndRejectsPostBulk() throws Exception {
        Settings search = categories("search");
        assertDispatch(RestOperationCategory.SEARCH, search, HttpRequest.Method.POST, "/test", RestStatus.OK);
        assertDispatch(RestOperationCategory.INDEXING, search, HttpRequest.Method.POST, "/test", RestStatus.NOT_FOUND);
        assertDispatch(RestOperationCategory.MANAGEMENT, search, HttpRequest.Method.GET, "/test", RestStatus.NOT_FOUND);
    }

    public void testIndexingNodeRejectsGetSearch() throws Exception {
        Settings indexing = categories("indexing");
        assertDispatch(RestOperationCategory.INDEXING, indexing, HttpRequest.Method.POST, "/test", RestStatus.OK);
        assertDispatch(RestOperationCategory.SEARCH, indexing, HttpRequest.Method.GET, "/test", RestStatus.NOT_FOUND);
    }

    public void testEmptyCategoriesDisablesAllHandlers() throws Exception {
        for (RestOperationCategory category : RestOperationCategory.values()) {
            assertDispatch(category, categories(), HttpRequest.Method.GET, "/test", RestStatus.NOT_FOUND);
        }
    }

    public void testCategoriesCanBeCombined() throws Exception {
        Settings combined = categories("search", "management");
        assertDispatch(RestOperationCategory.SEARCH, combined, HttpRequest.Method.POST, "/test", RestStatus.OK);
        assertDispatch(RestOperationCategory.MANAGEMENT, combined, HttpRequest.Method.GET, "/test", RestStatus.OK);
        assertDispatch(RestOperationCategory.INDEXING, combined, HttpRequest.Method.POST, "/test", RestStatus.NOT_FOUND);
    }

    public void testDeprecatedAndReplacedRoutesRetainCategory() throws Exception {
        for (String path : List.of("/deprecated", "/replacement", "/old")) {
            assertDispatch(RestOperationCategory.SEARCH, categories("search"), HttpRequest.Method.POST, path, RestStatus.OK);
            assertDispatch(RestOperationCategory.INDEXING, categories("search"), HttpRequest.Method.POST, path, RestStatus.NOT_FOUND);
        }
    }

    public void testDisabledPathsAreAbsentForEveryMethodAndRouteAlias() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        HttpServerTransport.Dispatcher dispatcher = createDispatcher(categories("search"), handler(RestOperationCategory.INDEXING, calls));
        for (String path : List.of("/test", "/deprecated", "/replacement", "/old")) {
            for (HttpRequest.Method method : HttpRequest.Method.values()) {
                HttpResponse response = dispatch(dispatcher, method, path, RestStatus.NOT_FOUND);
                verify(response, never()).addHeader(eq("Allow"), any());
            }
        }
        assertEquals(0, calls.get());
        assertTrue(((RestController) dispatcher).getAllHandlers().hasNext() == false);
    }

    public void testOptionsAndAllowOnlyAdvertiseEnabledMethods() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        HttpServerTransport.Dispatcher dispatcher = createDispatcher(
            categories("search"),
            routedHandler(RestOperationCategory.SEARCH, HttpRequest.Method.GET, "/shared", calls),
            routedHandler(RestOperationCategory.INDEXING, HttpRequest.Method.POST, "/shared", calls)
        );
        HttpResponse options = dispatch(dispatcher, HttpRequest.Method.OPTIONS, "/shared", RestStatus.OK);
        verify(options).addHeader("Allow", "GET");
        for (HttpRequest.Method method : List.of(HttpRequest.Method.POST, HttpRequest.Method.DELETE)) {
            HttpResponse unsupported = dispatch(dispatcher, method, "/shared", RestStatus.METHOD_NOT_ALLOWED);
            verify(unsupported).addHeader("Allow", "GET");
        }
        assertEquals(0, calls.get());
        dispatch(dispatcher, HttpRequest.Method.GET, "/shared", RestStatus.OK);
        assertEquals(1, calls.get());
    }

    public void testUnknownPathsAndDisabledFaviconReturnNotFound() throws Exception {
        HttpServerTransport.Dispatcher dispatcher = createDispatcher(categories("search"));
        for (String path : List.of("/unknown", "/favicon.ico")) {
            for (HttpRequest.Method method : List.of(HttpRequest.Method.GET, HttpRequest.Method.OPTIONS)) {
                HttpResponse response = dispatch(dispatcher, method, path, RestStatus.NOT_FOUND);
                verify(response, never()).addHeader(eq("Allow"), any());
            }
        }
    }

    public void testDefaultFaviconRemainsAvailable() throws Exception {
        HttpServerTransport.Dispatcher dispatcher = createDispatcher(Settings.EMPTY);
        dispatch(dispatcher, HttpRequest.Method.GET, "/favicon.ico", RestStatus.OK);
        HttpResponse response = dispatch(dispatcher, HttpRequest.Method.OPTIONS, "/favicon.ico", RestStatus.OK);
        verify(response).addHeader("Allow", "GET");
    }

    private static RestHandler routedHandler(RestOperationCategory category, HttpRequest.Method method, String path, AtomicInteger calls) {
        return new RestHandler.Wrapper(handler(category, calls)) {
            @Override
            public List<Route> routes() {
                return List.of(new Route(method, path));
            }

            @Override
            public List<DeprecatedRoute> deprecatedRoutes() {
                return List.of();
            }

            @Override
            public List<ReplacedRoute> replacedRoutes() {
                return List.of();
            }
        };
    }

    public void testWrapperRetainsCategory() {
        RestHandler handler = handler(RestOperationCategory.SEARCH, new AtomicInteger());
        assertEquals(RestOperationCategory.SEARCH, RestHandler.wrapper(handler).operationCategory());
    }

    public void testLegacyHandlerDefaultsToManagement() {
        RestHandler legacy = (request, channel, client) -> {};
        assertEquals(RestOperationCategory.MANAGEMENT, legacy.operationCategory());
    }

    public void testSettingsAreRegisteredAndRejectUnknownCategories() {
        assertTrue(new RestPlugin().getSettings().contains(RestPlugin.API_CATEGORIES));
        assertEquals(List.of(RestOperationCategory.SEARCH), RestPlugin.API_CATEGORIES.get(categories("search")));
        assertThrows(IllegalArgumentException.class, () -> RestPlugin.API_CATEGORIES.get(categories("read")));
        assertThrows(IllegalArgumentException.class, () -> RestPlugin.API_CATEGORIES.get(categories("SEARCH")));
    }

    public void testCategoryHubsAcceptMatchingExtensionsAndRejectMismatches() {
        for (RestApiPlugin hub : List.of(new SearchApiPlugin(), new IndexingApiPlugin(), new ManagementApiPlugin())) {
            assertTrue(hub.getRestHandlers().isEmpty());
            RestHandler matching = handler(hub.operationCategory(), new AtomicInteger());
            hub.accept(new HandlerPlugin(matching));
            assertEquals(List.of(matching), hub.getRestHandlers());
            RestOperationCategory wrong = hub.operationCategory() == RestOperationCategory.SEARCH
                ? RestOperationCategory.INDEXING
                : RestOperationCategory.SEARCH;
            hub.accept(new HandlerPlugin(handler(wrong, new AtomicInteger())));
            assertThrows(IllegalArgumentException.class, hub::getRestHandlers);
        }
    }

    public void testPluginServiceConnectsManagementFeatureThroughApiHub() throws Exception {
        PluginsService plugins = new PluginsService(
            Settings.EMPTY,
            null,
            null,
            null,
            List.of(
                pluginInfo("rest", RestPlugin.class, List.of()),
                pluginInfo("search-api", SearchApiPlugin.class, List.of("rest")),
                pluginInfo("indexing-api", IndexingApiPlugin.class, List.of("rest")),
                pluginInfo("management-api", ManagementApiPlugin.class, List.of("rest")),
                pluginInfo("hello-world", HelloWorldPlugin.class, List.of("management-api"))
            )
        );
        ManagementApiPlugin management = plugins.filterPlugins(ManagementApiPlugin.class).get(0);
        List<RestHandler> handlers = management.getRestHandlers();
        assertEquals(1, handlers.size());
        assertEquals(RestOperationCategory.MANAGEMENT, handlers.get(0).operationCategory());
        assertEquals("/", handlers.get(0).routes().get(0).getPath());
        assertEquals(HttpRequest.Method.GET, handlers.get(0).routes().get(0).getMethod());
        assertTrue(plugins.filterPlugins(SearchApiPlugin.class).get(0).getRestHandlers().isEmpty());
        assertTrue(plugins.filterPlugins(IndexingApiPlugin.class).get(0).getRestHandlers().isEmpty());
        for (Plugin plugin : plugins.filterPlugins(Plugin.class)) {
            plugin.close();
        }
    }

    private static PluginInfo pluginInfo(String name, Class<? extends Plugin> pluginClass, List<String> extensions) {
        return new PluginInfo(name, "test API plugin", "1", Version.CURRENT, "21", pluginClass.getName(), null, extensions, false);
    }

    public void testCategoryHubsAndRestRejectInvalidExtensions() {
        assertThrows(IllegalArgumentException.class, () -> new SearchApiPlugin().accept(new Plugin() {
        }));
        assertThrows(IllegalArgumentException.class, () -> new RestPlugin().accept(new Plugin() {
        }));
    }

    private static Settings categories(String... categories) {
        return Settings.builder().putList("rest.api.categories", categories).build();
    }

    private void assertDispatch(
        RestOperationCategory category,
        Settings settings,
        HttpRequest.Method method,
        String path,
        RestStatus expected
    ) throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RestApiPlugin hub = switch (category) {
            case SEARCH -> new SearchApiPlugin();
            case INDEXING -> new IndexingApiPlugin();
            case MANAGEMENT -> new ManagementApiPlugin();
        };
        hub.accept(new HandlerPlugin(RestHandler.wrapper(handler(category, calls))));
        RestPlugin plugin = new RestPlugin();
        plugin.accept(hub);
        plugin.createComponents(new PluginResources(NamedXContentRegistry.EMPTY, null, null, null, null));
        HttpServerTransport.Dispatcher dispatcher = plugin.getHttpServerTransportDispatcher(
            BigArrays.NON_RECYCLING_INSTANCE,
            settings,
            new NoneCircuitBreakerService(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            NoopTracer.INSTANCE
        ).orElseThrow();
        dispatch(dispatcher, method, path, expected);
        assertEquals(expected == RestStatus.OK ? 1 : 0, calls.get());
    }

    private static HttpServerTransport.Dispatcher createDispatcher(Settings settings, RestHandler... handlers) {
        RestPlugin plugin = new RestPlugin();
        plugin.accept(new HandlerPlugin(handlers));
        plugin.createComponents(new PluginResources(NamedXContentRegistry.EMPTY, null, null, null, null));
        return plugin.getHttpServerTransportDispatcher(
            BigArrays.NON_RECYCLING_INSTANCE,
            settings,
            new NoneCircuitBreakerService(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            NoopTracer.INSTANCE
        ).orElseThrow();
    }

    private static HttpResponse dispatch(
        HttpServerTransport.Dispatcher dispatcher,
        HttpRequest.Method method,
        String path,
        RestStatus expected
    ) {
        HttpRequest request = mock(HttpRequest.class);
        when(request.method()).thenReturn(method);
        when(request.uri()).thenReturn(path);
        when(request.content()).thenReturn(BytesArray.EMPTY);
        when(request.protocolVersion()).thenReturn(HttpRequest.HttpVersion.HTTP_1_1);
        when(request.getHeaders()).thenReturn(Map.of());
        when(request.releaseAndCopy()).thenReturn(request);
        HttpResponse response = mock(HttpResponse.class);
        when(request.createResponse(any(), any())).thenAnswer(invocation -> {
            if (expected == RestStatus.NOT_FOUND) {
                String body = ((org.opensearch.core.common.bytes.BytesReference) invocation.getArgument(1)).utf8ToString();
                assertTrue(body, body.contains("disabled") == false);
                assertTrue(body, body.contains("category") == false);
            }
            return response;
        });
        HttpChannel channel = mock(HttpChannel.class);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(channel).sendResponse(any(), any());
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            dispatcher.dispatchRequest(request, channel, context);
        }
        verify(request).createResponse(eq(expected), any());
        verify(channel).sendResponse(any(), any());
        verify(request).release();
        return response;
    }

    private static RestHandler handler(RestOperationCategory category, AtomicInteger calls) {
        return new RestHandler() {
            @Override
            public RestOperationCategory operationCategory() {
                return category;
            }

            @Override
            public void handleRequest(
                org.opensearch.rest.spi.RestRequest request,
                org.opensearch.rest.spi.RestChannel channel,
                org.opensearch.transport.client.node.NodeClient client
            ) {
                calls.incrementAndGet();
                channel.sendResponse(new org.opensearch.rest.spi.BytesRestResponse(RestStatus.OK, "text/plain", "ok"));
            }

            @Override
            public List<Route> routes() {
                return List.of(new Route(HttpRequest.Method.GET, "/test"), new Route(HttpRequest.Method.POST, "/test"));
            }

            @Override
            public List<DeprecatedRoute> deprecatedRoutes() {
                return List.of(new DeprecatedRoute(HttpRequest.Method.POST, "/deprecated", "Use /test instead"));
            }

            @Override
            public List<ReplacedRoute> replacedRoutes() {
                return List.of(new ReplacedRoute(HttpRequest.Method.POST, "/replacement", HttpRequest.Method.POST, "/old"));
            }
        };
    }

    private static class HandlerPlugin extends Plugin implements RestHandlerPlugin {
        private final List<RestHandler> handlers;

        HandlerPlugin(RestHandler... handlers) {
            this.handlers = List.of(handlers);
        }

        @Override
        public List<RestHandler> getRestHandlers() {
            return handlers;
        }
    }
}
