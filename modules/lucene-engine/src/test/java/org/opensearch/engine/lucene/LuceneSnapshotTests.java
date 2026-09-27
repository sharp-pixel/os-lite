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

import org.opensearch.common.settings.Settings;
import org.opensearch.engine.EngineRuntime;
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
import org.opensearch.engine.api.SnapshotManifest;
import org.opensearch.engine.api.SnapshotSource;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class LuceneSnapshotTests extends RandomizedTest {
    private static final Schema SCHEMA = new Schema(1, Map.of("title", Schema.FieldType.TEXT));

    @SuppressWarnings("try") // Closing the writer early proves leases retain its resources and commit files.
    public void testLeaseSurvivesLaterCommitsAndWriterClose() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = new ShardSpec(new ShardId(UUID.randomUUID(), 0), newTempDir(), SCHEMA);
        try (ShardWriter writer = provider.openWriter(spec)) {
            var written = writer.write(List.of(put("1")), context());
            try (SnapshotSource lease = writer.snapshot(context())) {
                writer.write(List.of(put("2")), context());
                writer.close();
                assertEquals(written.checkpoint(), lease.manifest().checkpoint());
                assertThrows(EngineException.class, () -> provider.openWriter(spec));
                Path restored = newTempDir();
                for (SnapshotManifest.File file : lease.manifest().files()) {
                    try (InputStream input = lease.open(file.name())) {
                        byte[] bytes = input.readAllBytes();
                        assertEquals(file.length(), bytes.length);
                        assertEquals(
                            file.sha256(),
                            java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes))
                        );
                        java.nio.file.Files.write(restored.resolve(file.name()), bytes);
                    }
                }
                try (
                    ShardReader reader = provider.openReader(new ShardSpec(spec.id(), restored, SCHEMA));
                    ReadView view = reader.acquireView()
                ) {
                    assertTrue(view.get("1", context()).isPresent());
                    assertTrue(view.get("2", context()).isEmpty());
                }
            }
        }
        try (ShardWriter reopened = provider.openWriter(spec)) {
            assertEquals(2, reopened.checkpoint().sequence());
        }
    }

    public void testFailedConcurrentInstallationKeepsOldReaderAvailable() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = new ShardSpec(new ShardId(UUID.randomUUID(), 0), newTempDir(), SCHEMA);
        try (
            ShardWriter writer = provider.openWriter(spec);
            EngineRuntime runtime = new EngineRuntime(Map.of("lucene", provider), newTempDir(), Settings.EMPTY)
        ) {
            runtime.start();
            writer.write(List.of(put("1")), context());
            runtime.installSnapshot("lucene", spec.id(), SCHEMA, EngineService.Mode.READ_ONLY, () -> writer.snapshot(context()), context())
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
            writer.write(List.of(put("2")), context());
            CountDownLatch copying = new CountDownLatch(1), release = new CountDownLatch(1);
            var installing = runtime.installSnapshot("lucene", spec.id(), SCHEMA, EngineService.Mode.READ_ONLY, () -> {
                SnapshotSource delegate = writer.snapshot(context());
                return new SnapshotSource() {
                    public SnapshotManifest manifest() {
                        return delegate.manifest();
                    }

                    public InputStream open(String name) throws IOException {
                        return new FilterInputStream(delegate.open(name)) {
                            private boolean first = true;

                            @Override
                            public int read(byte[] bytes, int offset, int length) throws IOException {
                                if (first) {
                                    first = false;
                                    copying.countDown();
                                    try {
                                        if (release.await(5, TimeUnit.SECONDS) == false) throw new IOException("copy was not released");
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new IOException(e);
                                    }
                                    int count = in.read(bytes, offset, length);
                                    if (count > 0) bytes[offset] ^= 1;
                                    return count;
                                }
                                return in.read(bytes, offset, length);
                            }
                        };
                    }

                    public void close() {
                        delegate.close();
                    }
                };
            }, context()).toCompletableFuture();
            try {
                assertTrue(copying.await(5, TimeUnit.SECONDS));
                assertEquals(
                    1,
                    runtime.search(spec.id(), new SearchQuery.All(), 10, context())
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)
                        .totalHits()
                );
            } finally {
                release.countDown();
            }
            var failure = assertThrows(ExecutionException.class, () -> installing.get(5, TimeUnit.SECONDS));
            assertEquals(EngineException.Code.INCOMPATIBLE, ((EngineException) failure.getCause()).code());
            assertEquals(
                1,
                runtime.search(spec.id(), new SearchQuery.All(), 10, context()).toCompletableFuture().get(5, TimeUnit.SECONDS).totalHits()
            );
        }
    }

    public void testAbandonedCacheSymlinkCannotDeleteOutsideItsDirectory() throws Exception {
        LuceneEngineProvider provider = new LuceneEngineProvider();
        ShardSpec spec = new ShardSpec(new ShardId(UUID.randomUUID(), 0), newTempDir(), SCHEMA);
        Path root = newTempDir();
        Path external = newTempDir();
        Path keep = java.nio.file.Files.writeString(external.resolve("keep"), "outside cache");
        Path parent = root.resolve(".snapshots").resolve(spec.id().indexId().toString()).resolve("0");
        java.nio.file.Files.createDirectories(parent);
        Path orphan = java.nio.file.Files.createSymbolicLink(parent.resolve("orphan"), external);
        try (
            ShardWriter writer = provider.openWriter(spec);
            EngineRuntime runtime = new EngineRuntime(Map.of("lucene", provider), root, Settings.EMPTY)
        ) {
            runtime.start();
            runtime.installSnapshot("lucene", spec.id(), SCHEMA, EngineService.Mode.READ_ONLY, () -> writer.snapshot(context()), context())
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
            assertTrue(java.nio.file.Files.exists(keep));
            assertFalse(java.nio.file.Files.exists(orphan, java.nio.file.LinkOption.NOFOLLOW_LINKS));
        }
    }

    private static Mutation put(String id) {
        return new Mutation.Put(new EngineDocument(id, Map.of("title", "document " + id), new byte[0]));
    }

    private static OperationContext context() {
        return OperationContext.standard();
    }
}
