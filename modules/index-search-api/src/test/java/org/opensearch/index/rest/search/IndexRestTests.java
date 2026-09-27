/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.rest.search;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.api.indexing.IndexingApiPlugin;
import org.opensearch.api.management.ManagementApiPlugin;
import org.opensearch.api.search.SearchApiPlugin;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.engine.EngineRuntime;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.lucene.LuceneEngineProvider;
import org.opensearch.env.Environment;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.index.api.IndexActions;
import org.opensearch.index.api.IndexRequest;
import org.opensearch.index.api.IndexResponse;
import org.opensearch.index.rest.indexing.IndexIndexingApiPlugin;
import org.opensearch.index.rest.management.IndexManagementApiPlugin;
import org.opensearch.index.service.IndexTransportActions;
import org.opensearch.index.service.LocalIndexService;
import org.opensearch.plugins.PluginResources;
import org.opensearch.rest.RestPlugin;
import org.opensearch.tasks.TaskManager;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class IndexRestTests extends RandomizedTest {
    public void testHttpToTransportToEngineWorkflow() throws Exception {
        try (Fixture fixture = fixture(Settings.EMPTY)) {
            fixture.request(
                HttpRequest.Method.PUT,
                "/books",
                "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"},\"tag\":{\"type\":\"keyword\"}}}}",
                RestStatus.OK
            );
            Reply put = fixture.request(
                HttpRequest.Method.PUT,
                "/books/_doc/1",
                "{\"title\":\"Quick brown fox\",\"tag\":\"animal\"}",
                RestStatus.OK
            );
            assertTrue(put.body().contains("\"sequence\":1"));
            assertTrue(
                fixture.request(HttpRequest.Method.GET, "/books/_doc/1", "", RestStatus.NOT_FOUND).body().contains("\"found\":false")
            );
            fixture.request(HttpRequest.Method.POST, "/books/_refresh", "", RestStatus.OK);
            assertTrue(fixture.request(HttpRequest.Method.GET, "/books/_doc/1", "", RestStatus.OK).body().contains("Quick brown fox"));
            Reply search = fixture.request(
                HttpRequest.Method.POST,
                "/books/_search",
                "{\"query\":{\"match\":{\"title\":\"QUICK\"}},\"size\":10}",
                RestStatus.OK
            );
            assertTrue(search.body(), search.body().contains("\"value\":1"));
            assertTrue(search.body(), search.body().contains("\"_id\":\"1\""));
            fixture.request(HttpRequest.Method.GET, "/books", "", RestStatus.OK);
            fixture.request(HttpRequest.Method.DELETE, "/books/_doc/1", "", RestStatus.OK);
            fixture.request(HttpRequest.Method.POST, "/books/_refresh", "", RestStatus.OK);
            fixture.request(HttpRequest.Method.GET, "/books/_doc/1", "", RestStatus.NOT_FOUND);
        }
    }

    public void testSearchAndIndexingCategoryRouting() throws Exception {
        try (Fixture search = fixture(Settings.builder().putList("rest.api.categories", "search").build())) {
            search.create();
            search.request(HttpRequest.Method.POST, "/books/_search", "{\"query\":{\"match_all\":{}}}", RestStatus.OK);
            Reply absent = search.request(HttpRequest.Method.PUT, "/other", "{}", RestStatus.NOT_FOUND);
            assertTrue(absent.body().contains("disabled") == false);
            search.request(HttpRequest.Method.POST, "/books/_refresh", "", RestStatus.NOT_FOUND);
            // GET remains enabled on this shared path, so other methods have normal 405 semantics.
            search.request(HttpRequest.Method.PUT, "/books/_doc/1", "{}", RestStatus.METHOD_NOT_ALLOWED);
        }
        try (Fixture indexing = fixture(Settings.builder().putList("rest.api.categories", "indexing").build())) {
            indexing.create();
            indexing.request(HttpRequest.Method.PUT, "/books/_doc/1", "{\"title\":\"written\"}", RestStatus.OK);
            indexing.request(HttpRequest.Method.POST, "/books/_search", "{}", RestStatus.NOT_FOUND);
            indexing.request(HttpRequest.Method.GET, "/books", "", RestStatus.NOT_FOUND);
        }
    }

    public void testMalformedOrUnsupportedQueriesNeverReachActionExecution() throws Exception {
        try (Fixture fixture = fixture(Settings.EMPTY)) {
            for (String body : List.of(
                "{",
                "{} {}",
                "{\"size\":1,\"size\":2}",
                "{\"query\":{\"range\":{\"title\":{}}}}",
                "{\"size\":1001}",
                "{\"size\":1.5}",
                "{\"query\":null}",
                "{\"query\":{\"bool\":{\"should\":null}}}",
                "{\"sort\":[]}",
                "{\"query\":{\"match_all\":{\"boost\":2}}}"
            )) {
                fixture.request(HttpRequest.Method.POST, "/books/_search", body, RestStatus.BAD_REQUEST);
            }
            fixture.request(HttpRequest.Method.POST, "/books/_search?id=1", "{}", RestStatus.BAD_REQUEST);
            fixture.request(HttpRequest.Method.GET, "/books/_search?size=10", "", RestStatus.BAD_REQUEST);
            assertEquals(0, fixture.calls.get());
        }
    }

    public void testStrictSchemaAndDocumentParsing() throws Exception {
        try (Fixture fixture = fixture(Settings.EMPTY)) {
            for (String body : List.of(
                "{}",
                "{\"mappings\":{\"properties\":{}}}",
                "{\"settings\":{},\"mappings\":{}}",
                "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"integer\"}}}}"
            ))
                fixture.request(HttpRequest.Method.PUT, "/books", body, RestStatus.BAD_REQUEST);
            for (String body : List.of("{\"title\":1}", "{\"title\":null}", "{\"title\":[]}", "{\"title\":\"one\",\"title\":\"two\"}"))
                fixture.request(HttpRequest.Method.PUT, "/books/_doc/1", body, RestStatus.BAD_REQUEST);
            fixture.request(HttpRequest.Method.DELETE, "/books/_doc/1", "{}", RestStatus.BAD_REQUEST);
            fixture.request(HttpRequest.Method.POST, "/books/_refresh", "{}", RestStatus.BAD_REQUEST);
            assertEquals(0, fixture.calls.get());
        }
    }

    public void testMissingConflictAndEngineValidationStatuses() throws Exception {
        try (Fixture fixture = fixture(Settings.EMPTY)) {
            fixture.request(HttpRequest.Method.GET, "/missing/_doc/1", "", RestStatus.NOT_FOUND);
            fixture.create();
            fixture.request(
                HttpRequest.Method.PUT,
                "/books",
                "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}",
                RestStatus.CONFLICT
            );
            fixture.request(HttpRequest.Method.PUT, "/books/_doc/1", "{\"unknown\":\"field\"}", RestStatus.BAD_REQUEST);
            fixture.request(
                HttpRequest.Method.POST,
                "/books/_search",
                "{\"query\":{\"term\":{\"unknown\":\"value\"}}}",
                RestStatus.BAD_REQUEST
            );
        }
    }

    public void testBodyAndNestingLimits() throws Exception {
        try (Fixture fixture = fixture(Settings.EMPTY)) {
            fixture.request(HttpRequest.Method.POST, "/books/_search", " ".repeat(65537), RestStatus.BAD_REQUEST);
            fixture.request(HttpRequest.Method.PUT, "/books/_doc/1", " ".repeat((1 << 20) + 1), RestStatus.BAD_REQUEST);
            fixture.request(
                HttpRequest.Method.POST,
                "/books/_search",
                "{\"query\":" + "[".repeat(26) + "0" + "]".repeat(26) + "}",
                RestStatus.BAD_REQUEST
            );
            assertEquals(0, fixture.calls.get());
        }
    }

    private Fixture fixture(Settings settings) throws java.io.IOException {
        return new Fixture(
            new Environment(Settings.builder().put(settings).put("path.home", newTempDir().toString()).build(), null),
            settings
        );
    }

    private record Reply(RestStatus status, String body) {
    }

    private static final class Fixture implements AutoCloseable {
        final EngineRuntime engine;
        final LocalIndexService indices;
        final ThreadContext context;
        final HttpServerTransport.Dispatcher dispatcher;
        final AtomicInteger calls = new AtomicInteger();

        @SuppressWarnings("unchecked")
        Fixture(Environment environment, Settings settings) {
            engine = new EngineRuntime(
                Map.of("lucene", new LuceneEngineProvider()),
                environment.dataFiles()[0].resolve("engines"),
                settings
            );
            indices = new LocalIndexService(engine, environment, settings);
            engine.start();
            indices.start();
            context = new ThreadContext(settings);
            ThreadPool pool = mock(ThreadPool.class);
            when(pool.getThreadContext()).thenReturn(context);
            TransportService transport = mock(TransportService.class);
            when(transport.getThreadPool()).thenReturn(pool);
            when(transport.getTaskManager()).thenReturn(mock(TaskManager.class));
            ActionFilters filters = new ActionFilters(Set.of());
            Map<String, TransportAction<?, ?>> actions = Map.of(
                IndexActions.CREATE.name(),
                new IndexTransportActions.Create(transport, filters, indices),
                IndexActions.DESCRIBE.name(),
                new IndexTransportActions.Describe(transport, filters, indices),
                IndexActions.PUT.name(),
                new IndexTransportActions.Put(transport, filters, indices),
                IndexActions.DELETE.name(),
                new IndexTransportActions.Delete(transport, filters, indices),
                IndexActions.REFRESH.name(),
                new IndexTransportActions.Refresh(transport, filters, indices),
                IndexActions.GET.name(),
                new IndexTransportActions.Get(transport, filters, indices),
                IndexActions.SEARCH.name(),
                new IndexTransportActions.Search(transport, filters, indices)
            );
            NodeClient client = mock(NodeClient.class);
            when(client.threadPool()).thenReturn(pool);
            doAnswer(invocation -> {
                ActionType<?> type = invocation.getArgument(0);
                IndexRequest request = invocation.getArgument(1);
                ActionListener<IndexResponse> listener = invocation.getArgument(2);
                TransportAction<IndexRequest, IndexResponse> action = (TransportAction<IndexRequest, IndexResponse>) actions.get(
                    type.name()
                );
                action.execute(
                    request.createTask(calls.incrementAndGet(), "direct", type.name(), TaskId.EMPTY_TASK_ID, Map.of()),
                    request,
                    listener
                );
                return null;
            }).when(client).execute(any(), any(), any());
            SearchApiPlugin search = new SearchApiPlugin();
            search.accept(new IndexSearchApiPlugin());
            IndexingApiPlugin indexing = new IndexingApiPlugin();
            indexing.accept(new IndexIndexingApiPlugin());
            ManagementApiPlugin management = new ManagementApiPlugin();
            management.accept(new IndexManagementApiPlugin());
            RestPlugin rest = new RestPlugin();
            rest.accept(search);
            rest.accept(indexing);
            rest.accept(management);
            rest.createComponents(new PluginResources(NamedXContentRegistry.EMPTY, null, environment, pool, client));
            dispatcher = rest.getHttpServerTransportDispatcher(
                BigArrays.NON_RECYCLING_INSTANCE,
                settings,
                new NoneCircuitBreakerService(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            ).orElseThrow();
        }

        void create() throws Exception {
            indices.create(
                new IndexRequest.Create("books", "lucene", new Schema(1, Map.of("title", Schema.FieldType.TEXT))),
                OperationContext.standard()
            ).toCompletableFuture().get(10, TimeUnit.SECONDS);
        }

        @SuppressWarnings("try") // Closing the stored context restores the dispatcher's thread context.
        Reply request(HttpRequest.Method method, String uri, String body, RestStatus expected) throws Exception {
            HttpRequest request = mock(HttpRequest.class);
            when(request.method()).thenReturn(method);
            when(request.uri()).thenReturn(uri);
            when(request.content()).thenReturn(new BytesArray(body));
            when(request.protocolVersion()).thenReturn(HttpRequest.HttpVersion.HTTP_1_1);
            when(request.getHeaders()).thenReturn(body.isEmpty() ? Map.of() : Map.of("Content-Type", List.of("application/json")));
            when(request.header("Content-Type")).thenReturn(body.isEmpty() ? null : "application/json");
            when(request.releaseAndCopy()).thenReturn(request);
            AtomicReference<Reply> result = new AtomicReference<>();
            CountDownLatch sent = new CountDownLatch(1);
            HttpResponse response = mock(HttpResponse.class);
            when(request.createResponse(any(), any())).thenAnswer(invocation -> {
                result.set(new Reply(invocation.getArgument(0), ((BytesReference) invocation.getArgument(1)).utf8ToString()));
                return response;
            });
            HttpChannel channel = mock(HttpChannel.class);
            doAnswer(invocation -> {
                ActionListener<Void> listener = invocation.getArgument(1);
                listener.onResponse(null);
                sent.countDown();
                return null;
            }).when(channel).sendResponse(any(), any());
            try (ThreadContext.StoredContext ignored = context.stashContext()) {
                dispatcher.dispatchRequest(request, channel, context);
            }
            assertTrue("response timed out: " + uri, sent.await(10, TimeUnit.SECONDS));
            assertEquals(result.get().body(), expected, result.get().status());
            verify(request).release();
            return result.get();
        }

        @Override
        public void close() {
            try {
                engine.close();
            } finally {
                indices.close();
            }
        }
    }
}
