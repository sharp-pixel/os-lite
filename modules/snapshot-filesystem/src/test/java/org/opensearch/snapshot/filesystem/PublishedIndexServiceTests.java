/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshot.filesystem;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.common.settings.Settings;
import org.opensearch.engine.EngineRuntime;
import org.opensearch.engine.api.Checkpoint;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;
import org.opensearch.engine.lucene.LuceneEngineProvider;
import org.opensearch.env.Environment;
import org.opensearch.index.api.IndexRequest;
import org.opensearch.index.api.IndexResponse;
import org.opensearch.index.api.SnapshotRepository;
import org.opensearch.index.api.SnapshotRepositoryProvider;
import org.opensearch.index.service.LocalIndexService;
import org.opensearch.index.service.RepositoryRegistry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class PublishedIndexServiceTests extends RandomizedTest {
    private static final Schema SCHEMA = new Schema(1, Map.of("title", Schema.FieldType.TEXT));

    public void testIndependentNodesPublishRefreshAndReadMinimumCheckpoint() throws Exception {
        Path repository = newTempDir();
        try (Fixture writer = fixture(newTempDir(), repository, true); Fixture reader = fixture(newTempDir(), repository, false)) {
            var created = await(writer.indices.create(create(), context()));
            assertEquals(1, created.writerEpoch());
            var first = await(writer.indices.put(put("1"), context()));
            assertEquals(1, await(reader.indices.search(search(), context())).result().totalHits());
            var second = await(writer.indices.put(put("2"), context()));
            assertEquals(1, await(reader.indices.search(search(), context())).result().totalHits());
            assertEquals(
                2,
                await(
                    reader.indices.search(new IndexRequest.Search("books", new SearchQuery.All(), 10, second.checkpoint(), 1000), context())
                ).result().totalHits()
            );
            assertTrue(await(reader.indices.get(new IndexRequest.Get("books", "2"), context())).document().isPresent());
            assertEquals(first.checkpoint().history(), second.checkpoint().history());
            var deleted = await(writer.indices.delete(new IndexRequest.Delete("books", "1"), context()));
            assertEquals(deleted.checkpoint(), await(reader.indices.refresh(new IndexRequest.Refresh("books"), context())).checkpoint());
            assertEquals(1, await(reader.indices.search(search(), context())).result().totalHits());
            assertEquals(EngineException.Code.UNSUPPORTED, failure(reader.indices.put(put("3"), context())).code());
            assertEquals(EngineException.Code.UNSUPPORTED, failure(writer.indices.search(search(), context())).code());
        }
    }

    public void testNewWriterNeedsExplicitEpochTakeoverAndRestoresPublishedData() throws Exception {
        Path repository = newTempDir();
        Path original = newTempDir();
        try (Fixture writer = fixture(original, repository, true)) {
            await(writer.indices.create(create(), context()));
            await(writer.indices.put(put("1"), context()));
        }
        try (
            Fixture replacement = fixture(newTempDir(), repository, true);
            Fixture old = fixture(original, repository, true);
            Fixture reader = fixture(newTempDir(), repository, false)
        ) {
            assertEquals(EngineException.Code.CONFLICT, failure(replacement.indices.put(put("2"), context())).code());
            assertEquals(2, await(replacement.indices.claim(new IndexRequest.Claim("books", 1), context())).writerEpoch());
            var written = await(replacement.indices.put(put("2"), context()));
            assertEquals(2, written.checkpoint().sequence());
            assertEquals(EngineException.Code.CONFLICT, failure(old.indices.put(put("3"), context())).code());
            assertEquals(2, await(reader.indices.search(search(), context())).result().totalHits());
        }
    }

    public void testUnknownPublicationStopsWriterAndRestartReconcilesDurableHead() throws Exception {
        for (boolean after : List.of(false, true)) {
            Path repository = newTempDir();
            Path home = newTempDir();
            SnapshotRepositoryProvider provider = new SnapshotRepositoryProvider() {
                public String id() {
                    return "filesystem";
                }

                public SnapshotRepository open(Path path, boolean writable) {
                    var repository = spy(new FilesystemSnapshotRepository(path, writable));
                    doAnswer(invocation -> {
                        if (after) invocation.callRealMethod();
                        throw new EngineException(EngineException.Code.IO_ERROR, "injected publication failure");
                    }).when(repository).publish(any(), any(), any());
                    return repository;
                }
            };
            try (Fixture writer = new Fixture(home, repository, true, provider)) {
                await(writer.indices.create(create(), context()));
                assertEquals(EngineException.Code.WRITE_OUTCOME_UNKNOWN, failure(writer.indices.put(put("1"), context())).code());
                assertEquals(EngineException.Code.UNAVAILABLE, failure(writer.indices.put(put("2"), context())).code());
            }
            try (Fixture recovered = fixture(home, repository, true); Fixture reader = fixture(newTempDir(), repository, false)) {
                assertEquals(after ? 1 : 0, await(reader.indices.search(search(), context())).result().totalHits());
                var written = await(recovered.indices.put(put("2"), context()));
                assertEquals(after ? 2 : 1, written.checkpoint().sequence());
                await(reader.indices.refresh(new IndexRequest.Refresh("books"), context()));
                assertEquals(after ? 2 : 1, await(reader.indices.search(search(), context())).result().totalHits());
            }
        }
    }

    public void testChecksumFailurePreservesPreviousReader() throws Exception {
        Path root = newTempDir();
        try (
            Fixture writer = fixture(newTempDir(), root, true);
            Fixture reader = fixture(newTempDir(), root, false);
            var repository = new FilesystemSnapshotRepository(root, false)
        ) {
            await(writer.indices.create(create(), context()));
            await(writer.indices.put(put("1"), context()));
            assertEquals(1, await(reader.indices.search(search(), context())).result().totalHits());
            await(writer.indices.put(put("2"), context()));
            var head = repository.current("books", context()).orElseThrow();
            var file = head.manifest().files().stream().filter(value -> value.length() > 0).findFirst().orElseThrow();
            Path path = root.resolve("snapshots").resolve(head.snapshotId().toString()).resolve(file.name());
            byte[] original = Files.readAllBytes(path);
            byte[] corrupt = original.clone();
            corrupt[0] ^= 1;
            Files.write(path, corrupt);
            assertEquals(
                EngineException.Code.INCOMPATIBLE,
                failure(reader.indices.refresh(new IndexRequest.Refresh("books"), context())).code()
            );
            assertEquals(1, await(reader.indices.search(search(), context())).result().totalHits());
            Files.write(path, original);
            await(reader.indices.refresh(new IndexRequest.Refresh("books"), context()));
            assertEquals(2, await(reader.indices.search(search(), context())).result().totalHits());
        }
    }

    public void testMinimumCheckpointWaitTimeoutCancellationAndHistoryMismatch() throws Exception {
        Path repository = newTempDir();
        try (Fixture writer = fixture(newTempDir(), repository, true); Fixture reader = fixture(newTempDir(), repository, false)) {
            var initial = await(writer.indices.create(create(), context())).published();
            await(reader.indices.search(search(), context()));
            Checkpoint future = new Checkpoint(initial.shard(), initial.history(), 1);
            CompletionStage<IndexResponse.Search> waiting = reader.indices.search(
                new IndexRequest.Search("books", new SearchQuery.All(), 10, future, 5000),
                context()
            );
            await(writer.indices.put(put("1"), context()));
            assertEquals(1, await(waiting).result().totalHits());
            Checkpoint absent = new Checkpoint(initial.shard(), initial.history(), 2);
            assertEquals(
                EngineException.Code.DEADLINE_EXCEEDED,
                failure(reader.indices.search(new IndexRequest.Search("books", new SearchQuery.All(), 10, absent, 20), context())).code()
            );
            OperationContext cancelled = context();
            var pending = reader.indices.search(new IndexRequest.Search("books", new SearchQuery.All(), 10, absent, 5000), cancelled);
            cancelled.cancel();
            assertEquals(EngineException.Code.CANCELLED, failure(pending).code());
            Checkpoint wrong = new Checkpoint(initial.shard(), UUID.randomUUID(), 1);
            assertEquals(
                EngineException.Code.INCOMPATIBLE,
                failure(reader.indices.search(new IndexRequest.Search("books", new SearchQuery.All(), 10, wrong, 0), context())).code()
            );
        }
    }

    @SuppressWarnings("try") // Construction must fail before the second fixture can acquire storage.
    public void testRepositoryProfileRejectsMixedRolesAndSharedNodeStorage() throws Exception {
        Path home = newTempDir(), repository = newTempDir();
        try (Fixture first = fixture(home, repository, true)) {
            assertThrows(IllegalStateException.class, () -> {
                try (Fixture ignored = fixture(home, repository, true)) {
                    fail("second node acquired storage");
                }
            });
            await(first.indices.create(create(), context()));
        }
        Settings settings = Settings.builder()
            .put("path.home", newTempDir())
            .put("index_service.repository", "filesystem")
            .put("index_service.repository_path", repository)
            .build();
        Environment environment = new Environment(settings, null);
        try (
            EngineRuntime engine = new EngineRuntime(
                Map.of("lucene", new LuceneEngineProvider()),
                environment.dataFiles()[0].resolve("engines"),
                settings
            )
        ) {
            assertThrows(
                IllegalArgumentException.class,
                () -> new LocalIndexService(engine, environment, settings, registry(defaultProvider()))
            );
        }
    }

    private Fixture fixture(Path home, Path repository, boolean writer) {
        return new Fixture(home, repository, writer, defaultProvider());
    }

    private static SnapshotRepositoryProvider defaultProvider() {
        return new FilesystemSnapshotPlugin().repositoryProviders().getFirst();
    }

    private static RepositoryRegistry registry(SnapshotRepositoryProvider provider) {
        return new RepositoryRegistry(Map.of(provider.id(), provider));
    }

    private static final class Fixture implements AutoCloseable {
        final EngineRuntime engine;
        final LocalIndexService indices;

        Fixture(Path home, Path repository, boolean writer, SnapshotRepositoryProvider provider) {
            Settings settings = Settings.builder()
                .put("path.home", home)
                .putList("engine.roles", writer ? "writer" : "reader")
                .put("index_service.repository", "filesystem")
                .put("index_service.repository_path", repository)
                .build();
            Environment environment = new Environment(settings, null);
            engine = new EngineRuntime(
                Map.of("lucene", new LuceneEngineProvider()),
                environment.dataFiles()[0].resolve("engines"),
                settings
            );
            indices = new LocalIndexService(engine, environment, settings, registry(provider));
            engine.start();
            try {
                indices.start();
            } catch (Exception e) {
                close();
                throw e;
            }
        }

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

    private static IndexRequest.Put put(String id) {
        return new IndexRequest.Put("books", new EngineDocument(id, Map.of("title", "document " + id), new byte[0]));
    }

    private static IndexRequest.Search search() {
        return new IndexRequest.Search("books", new SearchQuery.All(), 10);
    }

    private static OperationContext context() {
        return OperationContext.standard();
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static EngineException failure(CompletionStage<?> stage) {
        return (EngineException) assertThrows(ExecutionException.class, () -> await(stage)).getCause();
    }
}
