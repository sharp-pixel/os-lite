/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.lucene;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.settings.Settings;
import org.opensearch.engine.EngineRuntime;
import org.opensearch.engine.api.Checkpoint;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.EngineService;
import org.opensearch.engine.api.Mutation;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.ReadView;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;
import org.opensearch.engine.api.ShardId;
import org.opensearch.engine.api.ShardReader;
import org.opensearch.engine.api.ShardSpec;
import org.opensearch.engine.api.ShardWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class LuceneEngineProviderTests extends RandomizedTest {
    private static final Schema SCHEMA = new Schema(1, Map.of("title", Schema.FieldType.TEXT, "tag", Schema.FieldType.KEYWORD));

    public void testCommitIsIndependentlyReadableAndSurvivesReopen() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = spec();
        Checkpoint acknowledged;
        try (ShardWriter writer = provider.openWriter(spec)) {
            acknowledged = writer.write(List.of(put("1", "The quick fox", "animal")), context()).checkpoint();
            // Independent Lucene reader proves data and metadata were committed before the writer was closed.
            try (Directory directory = FSDirectory.open(spec.directory()); DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals(1, new IndexSearcher(reader).count(new TermQuery(new Term("title", "quick"))));
                assertEquals("1", reader.getIndexCommit().getUserData().get("sequence"));
                assertEquals(acknowledged.history().toString(), reader.getIndexCommit().getUserData().get("history"));
                assertEquals(spec.id().indexId().toString(), reader.getIndexCommit().getUserData().get("index"));
            }
        }
        try (
            ShardWriter recovered = provider.openWriter(spec);
            ShardReader reader = provider.openReader(spec);
            ReadView view = reader.acquireView()
        ) {
            assertEquals(acknowledged, recovered.checkpoint());
            assertTrue(view.checkpoint().covers(acknowledged));
            assertEquals("The quick fox", view.get("1", context()).orElseThrow().fields().get("title"));
            Checkpoint next = recovered.write(List.of(new Mutation.Delete("1")), context()).checkpoint();
            assertEquals(acknowledged.history(), next.history());
            assertEquals(2, next.sequence());
        }
    }

    @SuppressWarnings("try") // Explicit close verifies outstanding views and repeated cleanup.
    public void testRefreshAndPinnedViewsHaveSeparateVisibility() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = spec();
        try (
            ShardWriter writer = provider.openWriter(spec);
            ShardReader reader = provider.openReader(spec);
            ReadView old = reader.acquireView()
        ) {
            Checkpoint committed = writer.write(List.of(put("1", "new value", "tag")), context()).checkpoint();
            assertTrue(old.get("1", context()).isEmpty());
            try (ReadView current = reader.acquireView()) {
                assertTrue(current.get("1", context()).isEmpty());
            }
            assertEquals(committed, reader.refresh(context()));
            try (ReadView current = reader.acquireView()) {
                assertTrue(current.get("1", context()).isPresent());
                reader.close();
                // Both views keep their directories and snapshot references alive after their owner closes.
                assertTrue(current.get("1", context()).isPresent());
                assertTrue(old.get("1", context()).isEmpty());
                error(EngineException.Code.CLOSED, reader::acquireView);
            }
        }
    }

    public void testReplacementDeleteAndTypedQueries() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = spec();
        try (ShardWriter writer = provider.openWriter(spec); ShardReader reader = provider.openReader(spec)) {
            writer.write(List.of(put("1", "Quick brown FOX", "Animal"), put("2", "blue bird", "Animal")), context());
            reader.refresh(context());
            try (ReadView view = reader.acquireView()) {
                assertEquals(2, view.search(new SearchQuery.All(), 10, context()).totalHits());
                assertEquals(1, view.search(new SearchQuery.Match("title", "QUICK"), 10, context()).totalHits());
                assertEquals(0, view.search(new SearchQuery.Term("tag", "animal"), 10, context()).totalHits());
                assertEquals(2, view.search(new SearchQuery.Term("tag", "Animal"), 10, context()).totalHits());
                SearchQuery query = new SearchQuery.Bool(
                    List.of(new SearchQuery.Term("tag", "Animal")),
                    List.of(),
                    List.of(new SearchQuery.Term("title", "bird")),
                    0
                );
                assertEquals("1", view.search(query, 10, context()).hits().get(0).document().id());
                assertEquals(
                    1,
                    view.search(
                        new SearchQuery.Bool(List.of(), List.of(), List.of(new SearchQuery.Term("title", "bird")), 0),
                        10,
                        context()
                    ).totalHits()
                );
            }
            writer.write(List.of(put("1", "replacement", "New"), new Mutation.Delete("2")), context());
            reader.refresh(context());
            try (ReadView view = reader.acquireView()) {
                assertEquals(1, view.search(new SearchQuery.All(), 10, context()).totalHits());
                assertEquals(0, view.search(new SearchQuery.Term("title", "quick"), 10, context()).totalHits());
                assertTrue(view.get("2", context()).isEmpty());
            }
        }
    }

    public void testDocumentBytesAreOwnedAcrossWriteAndRead() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = spec();
        byte[] source = { 1, 2, 3 };
        EngineDocument input = new EngineDocument("1", Map.of(), source);
        source[0] = 9;
        input.source()[0] = 8;
        EngineDocument result;
        try (ShardWriter writer = provider.openWriter(spec); ShardReader reader = provider.openReader(spec)) {
            writer.write(List.of(new Mutation.Put(input)), context());
            reader.refresh(context());
            try (ReadView view = reader.acquireView()) {
                result = view.get("1", context()).orElseThrow();
            }
        }
        result.source()[0] = 7;
        assertArrayEquals(new byte[] { 1, 2, 3 }, result.source());
    }

    public void testValidationRejectsWholeBatchBeforeApplication() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = spec();
        try (ShardWriter writer = provider.openWriter(spec); ShardReader reader = provider.openReader(spec)) {
            Mutation unknown = new Mutation.Put(new EngineDocument("2", Map.of("absent", "value"), new byte[0]));
            error(EngineException.Code.INVALID_ARGUMENT, () -> writer.write(List.of(put("1", "valid", "tag"), unknown), context()));
            error(EngineException.Code.INVALID_ARGUMENT, () -> writer.write(List.of(put("1", "title", "x".repeat(32767))), context()));
            assertEquals(0, writer.checkpoint().sequence());
            writer.write(List.of(put("3", "good", "tag")), context());
            reader.refresh(context());
            try (ReadView view = reader.acquireView()) {
                assertTrue(view.get("1", context()).isEmpty());
                assertEquals(1, view.search(new SearchQuery.All(), 10, context()).totalHits());
            }
        }
    }

    public void testFailedCommitPoisonsWriterAndCloseDoesNotCommitIt() throws Exception {
        AtomicBoolean failSync = new AtomicBoolean();
        LuceneEngineProvider provider = new LuceneEngineProvider(path -> new FilterDirectory(FSDirectory.open(path)) {
            @Override
            public void sync(Collection<String> names) throws IOException {
                if (failSync.get()) throw new IOException("injected sync failure");
                super.sync(names);
            }
        });
        ShardSpec spec = spec();
        Checkpoint committed;
        try (ShardWriter writer = provider.openWriter(spec); ShardReader reader = provider.openReader(spec)) {
            committed = writer.write(List.of(put("1", "durable", "tag")), context()).checkpoint();
            failSync.set(true);
            error(EngineException.Code.WRITE_OUTCOME_UNKNOWN, () -> writer.write(List.of(put("2", "uncertain", "tag")), context()));
            error(EngineException.Code.UNAVAILABLE, () -> writer.write(List.of(new Mutation.Delete("1")), context()));
            reader.refresh(context());
            try (ReadView view = reader.acquireView()) {
                assertEquals(committed, view.checkpoint());
                assertTrue(view.get("2", context()).isEmpty());
            }
        }
        failSync.set(false);
        try (
            ShardWriter writer = provider.openWriter(spec);
            ShardReader reader = provider.openReader(spec);
            ReadView view = reader.acquireView()
        ) {
            assertEquals(committed, writer.checkpoint());
            assertTrue(view.get("1", context()).isPresent());
            assertTrue(view.get("2", context()).isEmpty());
        }
    }

    public void testIncompatibleOpenClosesAllocatedDirectoriesAndPreservesData() throws Exception {
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        LuceneEngineProvider provider = new LuceneEngineProvider(path -> {
            opened.incrementAndGet();
            return new FilterDirectory(FSDirectory.open(path)) {
                @Override
                public void close() throws IOException {
                    closed.incrementAndGet();
                    super.close();
                }
            };
        });
        ShardSpec spec = spec();
        try (ShardWriter writer = provider.openWriter(spec)) {
            writer.write(List.of(put("1", "kept", "tag")), context());
        }
        ShardSpec different = new ShardSpec(spec.id(), spec.directory(), new Schema(2, SCHEMA.fields()));
        error(EngineException.Code.INCOMPATIBLE, () -> provider.openWriter(different));
        error(EngineException.Code.INCOMPATIBLE, () -> provider.openReader(different));
        ShardSpec wrongId = new ShardSpec(new ShardId(UUID.randomUUID(), 0), spec.directory(), SCHEMA);
        error(EngineException.Code.INCOMPATIBLE, () -> provider.openWriter(wrongId));
        try (ShardReader reader = provider.openReader(spec); ReadView view = reader.acquireView()) {
            assertTrue(view.get("1", context()).isPresent());
        }
        assertEquals(opened.get(), closed.get());
    }

    public void testOnlyOneWriterAndReadersDoNotAcquireWriteLock() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = spec();
        try (ShardWriter writer = provider.openWriter(spec); ShardReader reader = provider.openReader(spec)) {
            error(EngineException.Code.IO_ERROR, () -> provider.openWriter(spec));
            assertEquals(writer.checkpoint(), reader.refresh(context()));
        }
        try (ShardReader reader = provider.openReader(spec); ShardWriter writer = provider.openWriter(spec)) {
            assertEquals(writer.checkpoint(), reader.refresh(context()));
        }
    }

    public void testMissingReaderAndCorruptDirectoryAreNotInitialized() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = spec();
        ShardSpec missing = new ShardSpec(spec.id(), spec.directory().resolve("missing"), SCHEMA);
        error(EngineException.Code.UNAVAILABLE, () -> provider.openReader(missing));
        assertFalse(Files.exists(missing.directory()));
        Files.writeString(spec.directory().resolve("orphaned-segment"), "preserve");
        error(EngineException.Code.INCOMPATIBLE, () -> provider.openWriter(spec));
        assertEquals("preserve", Files.readString(spec.directory().resolve("orphaned-segment")));
    }

    @SuppressWarnings("try") // Explicit close verifies operations fail after the view is released.
    public void testQueryLimitsCancellationAndCheckpointIdentity() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = spec();
        try (
            ShardWriter writer = provider.openWriter(spec);
            ShardReader reader = provider.openReader(spec);
            ReadView view = reader.acquireView()
        ) {
            error(EngineException.Code.INVALID_ARGUMENT, () -> view.search(new SearchQuery.Term("missing", "x"), 1, context()));
            error(EngineException.Code.UNSUPPORTED, () -> view.search(new SearchQuery.Match("tag", "x"), 1, context()));
            error(EngineException.Code.RESOURCE_LIMIT, () -> view.search(new SearchQuery.All(), 1001, context()));
            SearchQuery deep = new SearchQuery.All();
            for (int i = 0; i < 18; i++)
                deep = new SearchQuery.Bool(List.of(deep), List.of(), List.of(), 0);
            SearchQuery tooDeep = deep;
            error(EngineException.Code.RESOURCE_LIMIT, () -> view.search(tooDeep, 1, context()));
            error(
                EngineException.Code.RESOURCE_LIMIT,
                () -> view.search(new SearchQuery.Match("title", "word ".repeat(513)), 1, context())
            );
            OperationContext cancelled = context();
            cancelled.cancel();
            error(EngineException.Code.CANCELLED, () -> view.search(new SearchQuery.All(), 1, cancelled));
            error(EngineException.Code.CANCELLED, () -> writer.write(List.of(put("1", "cancelled", "tag")), cancelled));
            assertEquals(0, writer.checkpoint().sequence());
            Checkpoint unrelated = new Checkpoint(spec.id(), UUID.randomUUID(), 0);
            error(EngineException.Code.INCOMPATIBLE, () -> view.checkpoint().covers(unrelated));
            view.close();
            error(EngineException.Code.CLOSED, () -> view.search(new SearchQuery.All(), 1, context()));
        }
    }

    public void testResponseByteLimitReleasesView() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = spec();
        try (ShardWriter writer = provider.openWriter(spec); ShardReader reader = provider.openReader(spec)) {
            for (int batch = 0; batch < 3; batch++) {
                List<Mutation> mutations = new ArrayList<>();
                for (int i = 0; i < 4; i++)
                    mutations.add(new Mutation.Put(new EngineDocument(batch + ":" + i, Map.of(), new byte[800000])));
                writer.write(mutations, context());
            }
            reader.refresh(context());
            try (ReadView view = reader.acquireView()) {
                error(EngineException.Code.RESOURCE_LIMIT, () -> view.search(new SearchQuery.All(), 100, context()));
            }
            try (ReadView view = reader.acquireView()) {
                assertEquals(1, view.search(new SearchQuery.All(), 1, context()).hits().size());
            }
        }
    }

    public void testConcurrentRuntimeReadsAndWritesKeepCommittedVisibility() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardId id = new ShardId(UUID.randomUUID(), 0);
        Settings settings = Settings.builder().put("engine.workers", 4).build();
        try (EngineRuntime runtime = new EngineRuntime(Map.of("lucene", provider), newTempDir(), settings)) {
            runtime.start();
            runtime.openShard("lucene", id, SCHEMA, EngineService.Mode.READ_WRITE, context())
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
            List<CompletableFuture<?>> requests = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                requests.add(
                    runtime.write(id, List.of(put(Integer.toString(i), "concurrent write", "tag")), context()).toCompletableFuture()
                );
                requests.add(
                    runtime.search(id, new SearchQuery.All(), 20, context())
                        .thenAccept(result -> assertEquals(0, result.totalHits()))
                        .toCompletableFuture()
                );
            }
            CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new)).get(20, TimeUnit.SECONDS);
            assertEquals(12, runtime.refresh(id, context()).toCompletableFuture().get(10, TimeUnit.SECONDS).sequence());
            assertEquals(
                12,
                runtime.search(id, new SearchQuery.All(), 20, context()).toCompletableFuture().get(10, TimeUnit.SECONDS).totalHits()
            );
        }
    }

    private ShardSpec spec() throws IOException {
        return new ShardSpec(new ShardId(UUID.randomUUID(), 0), newTempDir(), SCHEMA);
    }

    private static OperationContext context() {
        return OperationContext.standard();
    }

    private static Mutation put(String id, String title, String tag) {
        return new Mutation.Put(new EngineDocument(id, Map.of("title", title, "tag", tag), new byte[] { 1, 2 }));
    }

    private static void error(EngineException.Code code, Runnable operation) {
        assertEquals(code, assertThrows(EngineException.class, operation::run).code());
    }
}
