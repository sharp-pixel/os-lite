/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.service;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.engine.EngineRuntime;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;
import org.opensearch.engine.lucene.LuceneEngineProvider;
import org.opensearch.env.Environment;
import org.opensearch.index.api.IndexMetadata;
import org.opensearch.index.api.IndexRequest;
import org.opensearch.index.api.IndexResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class LocalIndexServiceTests extends RandomizedTest {
    public void testGuiceUsesNodeEnvironmentWithoutASettingsBinding() throws Exception {
        Environment environment = environment(newTempDir(), Settings.EMPTY);
        try (EngineRuntime engine = engine(environment, Settings.EMPTY)) {
            java.util.List<org.opensearch.common.inject.Module> modules = new java.util.ArrayList<>(
                new IndexServicePlugin().createGuiceModules()
            );
            modules.add(binder -> {
                binder.bind(org.opensearch.engine.api.EngineService.class).toInstance(engine);
                binder.bind(Environment.class).toInstance(environment);
            });
            var injector = org.opensearch.common.inject.Guice.createInjector(modules);
            try (LocalIndexService indices = injector.getInstance(LocalIndexService.class)) {
                org.junit.Assert.assertSame(indices, injector.getInstance(LocalIndexService.class));
            }
        }
    }

    private static final Schema SCHEMA = new Schema(1, Map.of("title", Schema.FieldType.TEXT));

    public void testCreateWriteRefreshGetSearchAndRestart() throws Exception {
        Path home = newTempDir();
        IndexMetadata metadata;
        try (Fixture fixture = fixture(home, Settings.EMPTY)) {
            metadata = await(fixture.indices.create(create(), context())).metadata();
            IndexResponse.Mutation write = await(fixture.indices.put(put("1", "Hello world"), context()));
            assertEquals(1, write.checkpoint().sequence());
            assertTrue(await(fixture.indices.get(new IndexRequest.Get("books", "1"), context())).document().isEmpty());
            assertEquals(write.checkpoint(), await(fixture.indices.refresh(new IndexRequest.Refresh("books"), context())).checkpoint());
            assertEquals(
                "Hello world",
                await(fixture.indices.get(new IndexRequest.Get("books", "1"), context())).document().orElseThrow().fields().get("title")
            );
            assertEquals(
                1,
                await(fixture.indices.search(new IndexRequest.Search("books", new SearchQuery.Match("title", "HELLO"), 10), context()))
                    .result()
                    .totalHits()
            );
        }
        try (Fixture recovered = fixture(home, Settings.EMPTY)) {
            assertEquals(metadata, await(recovered.indices.describe(new IndexRequest.Describe("books"), context())).metadata());
            assertTrue(await(recovered.indices.get(new IndexRequest.Get("books", "1"), context())).document().isPresent());
            await(recovered.indices.delete(new IndexRequest.Delete("books", "1"), context()));
            await(recovered.indices.refresh(new IndexRequest.Refresh("books"), context()));
            assertTrue(await(recovered.indices.get(new IndexRequest.Get("books", "1"), context())).document().isEmpty());
        }
    }

    public void testDuplicateMissingAndUnsupportedProvider() throws Exception {
        try (Fixture fixture = fixture(newTempDir(), Settings.EMPTY)) {
            await(fixture.indices.create(create(), context()));
            assertEquals(RestStatus.CONFLICT, ((OpenSearchStatusException) failure(fixture.indices.create(create(), context()))).status());
            assertEquals(
                RestStatus.NOT_FOUND,
                ((OpenSearchStatusException) failure(fixture.indices.get(new IndexRequest.Get("absent", "1"), context()))).status()
            );
            assertEquals(
                EngineException.Code.UNSUPPORTED,
                ((EngineException) failure(fixture.indices.create(new IndexRequest.Create("other", "missing", SCHEMA), context()))).code()
            );
        }
    }

    public void testReaderRoleCanRecoverAndReadButCannotCreateOrWrite() throws Exception {
        Path home = newTempDir();
        try (Fixture writer = fixture(home, Settings.EMPTY)) {
            await(writer.indices.create(create(), context()));
            await(writer.indices.put(put("1", "visible"), context()));
        }
        Settings reader = Settings.builder().putList("engine.roles", "reader").build();
        try (Fixture fixture = fixture(home, reader)) {
            assertTrue(await(fixture.indices.get(new IndexRequest.Get("books", "1"), context())).document().isPresent());
            assertEquals(
                EngineException.Code.UNSUPPORTED,
                ((EngineException) failure(fixture.indices.put(put("2", "forbidden"), context()))).code()
            );
            assertEquals(
                EngineException.Code.UNSUPPORTED,
                ((EngineException) failure(fixture.indices.create(new IndexRequest.Create("new", "lucene", SCHEMA), context()))).code()
            );
        }
    }

    public void testConcurrentFirstReadsCoalesceShardOpen() throws Exception {
        Path home = newTempDir();
        try (Fixture writer = fixture(home, Settings.EMPTY)) {
            await(writer.indices.create(create(), context()));
            await(writer.indices.put(put("1", "visible"), context()));
        }
        try (Fixture fixture = fixture(home, Settings.EMPTY)) {
            List<CompletableFuture<?>> reads = new java.util.ArrayList<>();
            for (int i = 0; i < 20; i++)
                reads.add(
                    fixture.indices.get(new IndexRequest.Get("books", "1"), context())
                        .thenAccept(result -> assertTrue(result.document().isPresent()))
                        .toCompletableFuture()
                );
            CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
        }
    }

    public void testCatalogLimitAndCancellationAreEnforced() throws Exception {
        try (Fixture fixture = fixture(newTempDir(), Settings.builder().put("index_service.max_indices", 1).build())) {
            OperationContext cancelled = context();
            cancelled.cancel();
            assertEquals(EngineException.Code.CANCELLED, ((EngineException) failure(fixture.indices.create(create(), cancelled))).code());
            await(fixture.indices.create(create(), context()));
            assertEquals(
                EngineException.Code.RESOURCE_LIMIT,
                ((EngineException) failure(fixture.indices.create(new IndexRequest.Create("other", "lucene", SCHEMA), context()))).code()
            );
        }
    }

    public void testSecondWriterCannotOwnCatalog() throws Exception {
        Path home = newTempDir();
        try (Fixture first = fixture(home, Settings.EMPTY)) {
            Environment environment = environment(home, Settings.EMPTY);
            try (
                EngineRuntime engine = engine(environment, Settings.EMPTY);
                LocalIndexService second = new LocalIndexService(engine, environment, Settings.EMPTY)
            ) {
                engine.start();
                assertThrows(IllegalStateException.class, second::start);
            }
            await(first.indices.create(create(), context()));
        }
    }

    public void testCatalogQueueRejectsExcessWorkAndCancelsQueuedCreates() throws Exception {
        Environment environment = environment(newTempDir(), Settings.EMPTY);
        IndexCatalog catalog = spy(new IndexCatalog(environment.dataFiles()[0].resolve("index-catalog")));
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            return invocation.callRealMethod();
        }).when(catalog).save(any());
        try (
            EngineRuntime engine = engine(environment, Settings.EMPTY);
            LocalIndexService indices = new LocalIndexService(engine, Settings.EMPTY, catalog)
        ) {
            engine.start();
            indices.start();
            CompletionStage<?> first = indices.create(create(), context());
            List<CompletionStage<?>> queued = new java.util.ArrayList<>();
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                for (int i = 0; i < 64; i++) {
                    OperationContext cancelled = context();
                    queued.add(indices.create(new IndexRequest.Create("queued" + i, "lucene", SCHEMA), cancelled));
                    cancelled.cancel();
                }
                assertEquals(
                    EngineException.Code.RESOURCE_LIMIT,
                    ((EngineException) failure(indices.create(new IndexRequest.Create("excess", "lucene", SCHEMA), context()))).code()
                );
            } finally {
                release.countDown();
            }
            await(first);
            for (CompletionStage<?> stage : queued)
                assertEquals(EngineException.Code.CANCELLED, ((EngineException) failure(stage)).code());
            assertEquals(
                RestStatus.NOT_FOUND,
                ((OpenSearchStatusException) failure(indices.describe(new IndexRequest.Describe("queued0"), context()))).status()
            );
        }
    }

    public void testCorruptCatalogFailsStartupWithoutReinitializingData() throws Exception {
        Path home = newTempDir();
        IndexMetadata metadata;
        try (Fixture first = fixture(home, Settings.EMPTY)) {
            metadata = await(first.indices.create(create(), context())).metadata();
        }
        Environment environment = environment(home, Settings.EMPTY);
        Path file = environment.dataFiles()[0].resolve("index-catalog").resolve(metadata.id() + ".meta");
        byte[] original = Files.readAllBytes(file);
        Files.write(file, java.util.Arrays.copyOf(original, original.length - 1));
        try (
            EngineRuntime engine = engine(environment, Settings.EMPTY);
            LocalIndexService indices = new LocalIndexService(engine, environment, Settings.EMPTY)
        ) {
            engine.start();
            assertThrows(IllegalStateException.class, indices::start);
        }
        assertEquals(original.length - 1, Files.size(file));
        Files.write(file, original);
        try (Fixture recovered = fixture(home, Settings.EMPTY)) {
            assertEquals(metadata, await(recovered.indices.describe(new IndexRequest.Describe("books"), context())).metadata());
        }
    }

    public void testPublicationFailureRequiresReconciliationBeforeAnotherCreate() throws Exception {
        Path home = newTempDir();
        Environment environment = environment(home, Settings.EMPTY);
        IndexCatalog catalog = spy(new IndexCatalog(environment.dataFiles()[0].resolve("index-catalog")));
        doAnswer(invocation -> {
            invocation.callRealMethod();
            throw new IOException("failure after publication");
        }).when(catalog).save(any());
        try (
            EngineRuntime engine = engine(environment, Settings.EMPTY);
            LocalIndexService indices = new LocalIndexService(engine, Settings.EMPTY, catalog)
        ) {
            engine.start();
            indices.start();
            assertEquals(
                RestStatus.INTERNAL_SERVER_ERROR,
                ((OpenSearchStatusException) failure(indices.create(create(), context()))).status()
            );
            assertEquals(EngineException.Code.UNAVAILABLE, ((EngineException) failure(indices.create(create(), context()))).code());
        }
        try (Fixture recovered = fixture(home, Settings.EMPTY)) {
            assertEquals("books", await(recovered.indices.describe(new IndexRequest.Describe("books"), context())).metadata().name());
        }
    }

    public void testEngineFailureCategoriesMapToActionStatusWithoutLuceneCause() {
        for (var entry : Map.of(
            EngineException.Code.RESOURCE_LIMIT,
            RestStatus.TOO_MANY_REQUESTS,
            EngineException.Code.INVALID_ARGUMENT,
            RestStatus.BAD_REQUEST,
            EngineException.Code.UNAVAILABLE,
            RestStatus.SERVICE_UNAVAILABLE,
            EngineException.Code.WRITE_OUTCOME_UNKNOWN,
            RestStatus.INTERNAL_SERVER_ERROR,
            EngineException.Code.DEADLINE_EXCEEDED,
            RestStatus.REQUEST_TIMEOUT
        ).entrySet()) {
            OpenSearchStatusException result = (OpenSearchStatusException) IndexFailures.action(
                new java.util.concurrent.CompletionException(new EngineException(entry.getKey(), "failure"))
            );
            assertEquals(entry.getValue(), result.status());
            assertTrue(result.getMessage().contains(entry.getKey().name()));
            assertEquals(null, result.getCause());
        }
    }

    private Fixture fixture(Path home, Settings settings) {
        return new Fixture(environment(home, settings), settings);
    }

    private static Environment environment(Path home, Settings settings) {
        return new Environment(Settings.builder().put(settings).put("path.home", home.toString()).build(), null);
    }

    private static EngineRuntime engine(Environment environment, Settings settings) {
        return new EngineRuntime(Map.of("lucene", new LuceneEngineProvider()), environment.dataFiles()[0].resolve("engines"), settings);
    }

    private static final class Fixture implements AutoCloseable {
        final EngineRuntime engine;
        final LocalIndexService indices;

        Fixture(Environment environment, Settings settings) {
            engine = engine(environment, settings);
            indices = new LocalIndexService(engine, environment, settings);
            engine.start();
            indices.start();
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

    private static IndexRequest.Create create() {
        return new IndexRequest.Create("books", "lucene", SCHEMA);
    }

    private static IndexRequest.Put put(String id, String title) {
        return new IndexRequest.Put("books", new EngineDocument(id, Map.of("title", title), new byte[0]));
    }

    private static OperationContext context() {
        return OperationContext.standard();
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static Throwable failure(CompletionStage<?> stage) {
        return assertThrows(ExecutionException.class, () -> await(stage)).getCause();
    }
}
