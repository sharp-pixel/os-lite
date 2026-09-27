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

import org.opensearch.engine.api.Checkpoint;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SnapshotManifest;
import org.opensearch.engine.api.SnapshotSource;
import org.opensearch.index.api.IndexMetadata;
import org.opensearch.index.api.SnapshotRepository;
import org.opensearch.index.api.SnapshotWire;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class FilesystemSnapshotRepositoryTests extends RandomizedTest {
    private static final Schema SCHEMA = new Schema(1, Map.of("title", Schema.FieldType.TEXT));
    private static final String ABC = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    public void testPublicationFencesStaleEpochAndCompetingHead() throws Exception {
        Path root = newTempDir();
        try (var repository = new FilesystemSnapshotRepository(root, true)) {
            IndexMetadata metadata = metadata();
            UUID owner = UUID.randomUUID();
            var first = repository.create(metadata, owner, source(metadata, UUID.randomUUID(), 0, false), context());
            var second = repository.publish(first, source(metadata, first.manifest().checkpoint().history(), 1, false), context());
            assertEquals(
                EngineException.Code.CONFLICT,
                assertThrows(
                    EngineException.class,
                    () -> repository.publish(first, source(metadata, first.manifest().checkpoint().history(), 2, false), context())
                ).code()
            );
            var claimed = repository.claim("books", 1, UUID.randomUUID(), context());
            assertEquals(2, claimed.epoch());
            assertEquals(second.snapshotId(), claimed.snapshotId());
            assertEquals(
                EngineException.Code.CONFLICT,
                assertThrows(
                    EngineException.class,
                    () -> repository.publish(second, source(metadata, second.manifest().checkpoint().history(), 2, false), context())
                ).code()
            );
            assertEquals(
                EngineException.Code.CONFLICT,
                assertThrows(EngineException.class, () -> repository.claim("books", 1, owner, context())).code()
            );
        }
        try (var recovered = new FilesystemSnapshotRepository(root, false)) {
            assertEquals(2, recovered.current("books", context()).orElseThrow().epoch());
            try (var read = recovered.open("books", context()); InputStream input = read.open("data.bin")) {
                assertArrayEquals(new byte[] { 'a', 'b', 'c' }, input.readAllBytes());
            }
        }
    }

    public void testIncompleteAndCorruptUploadsLeaveHeadUntouched() throws Exception {
        try (var repository = new FilesystemSnapshotRepository(newTempDir(), true)) {
            IndexMetadata metadata = metadata();
            var initial = repository.create(metadata, UUID.randomUUID(), source(metadata, UUID.randomUUID(), 0, false), context());
            assertThrows(
                EngineException.class,
                () -> repository.publish(initial, source(metadata, initial.manifest().checkpoint().history(), 1, true), context())
            );
            assertEquals(initial, repository.current("books", context()).orElseThrow());
            SnapshotSource wrongHash = source(metadata, initial.manifest().checkpoint().history(), 1, false);
            SnapshotSource corrupt = new SnapshotSource() {
                public SnapshotManifest manifest() {
                    return wrongHash.manifest();
                }

                public InputStream open(String name) {
                    return new ByteArrayInputStream(new byte[] { 'b', 'a', 'd' });
                }

                public void close() {}
            };
            assertEquals(
                EngineException.Code.INCOMPATIBLE,
                assertThrows(EngineException.class, () -> repository.publish(initial, corrupt, context())).code()
            );
            assertEquals(initial, repository.current("books", context()).orElseThrow());
        }
    }

    public void testActiveTransferPinsFilesAndRestartReclaimsOnlyOrphans() throws Exception {
        Path root = newTempDir();
        IndexMetadata metadata = metadata();
        try (var repository = new FilesystemSnapshotRepository(root, true)) {
            var first = repository.create(metadata, UUID.randomUUID(), source(metadata, UUID.randomUUID(), 0, false), context());
            SnapshotRepository.Publication second;
            try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                CompletableFuture<SnapshotRepository.Publication> pending = new CompletableFuture<>();
                CountDownLatch entered = new CountDownLatch(1);
                try (var lease = repository.open("books", context())) {
                    executor.execute(() -> {
                        entered.countDown();
                        try {
                            pending.complete(
                                repository.publish(first, source(metadata, first.manifest().checkpoint().history(), 1, false), context())
                            );
                        } catch (Exception e) {
                            pending.completeExceptionally(e);
                        }
                    });
                    assertTrue(entered.await(5, TimeUnit.SECONDS));
                    assertThrows(TimeoutException.class, () -> pending.get(50, TimeUnit.MILLISECONDS));
                    try (InputStream input = lease.open("data.bin")) {
                        assertEquals('a', input.read());
                    }
                }
                second = pending.get(5, TimeUnit.SECONDS);
            }
            Path orphan = root.resolve("snapshots").resolve(UUID.randomUUID() + ".pending");
            Files.createDirectory(orphan);
            Files.writeString(orphan.resolve("partial"), "incomplete");
            repository.publish(second, source(metadata, second.manifest().checkpoint().history(), 2, false), context());
            assertFalse(Files.exists(orphan));
            assertFalse(Files.exists(root.resolve("snapshots").resolve(first.snapshotId().toString())));
            assertTrue(Files.exists(root.resolve("snapshots").resolve(second.snapshotId().toString())));
        }
        try (var repository = new FilesystemSnapshotRepository(root, false); var read = repository.open("books", context())) {
            assertEquals(2, read.manifest().checkpoint().sequence());
            try (InputStream input = read.open("data.bin")) {
                assertEquals('a', input.read());
            }
        }
    }

    public void testMalformedStateFailsBeforeReclamation() throws Exception {
        Path root = newTempDir();
        try (var repository = new FilesystemSnapshotRepository(root, true)) {
            IndexMetadata metadata = metadata();
            var first = repository.create(metadata, UUID.randomUUID(), source(metadata, UUID.randomUUID(), 0, false), context());
            Path file = root.resolve("indices/books.state");
            byte[] original = Files.readAllBytes(file);
            Files.write(file, java.util.Arrays.copyOf(original, original.length - 1));
            assertThrows(EngineException.class, () -> repository.current("books", context()));
            assertThrows(
                EngineException.class,
                () -> repository.create(
                    new IndexMetadata("other", UUID.randomUUID(), "lucene", SCHEMA),
                    UUID.randomUUID(),
                    source(metadata, UUID.randomUUID(), 0, false),
                    context()
                )
            );
            assertTrue(Files.exists(root.resolve("snapshots").resolve(first.snapshotId().toString())));
        }
    }

    public void testRepositoryWaitObservesCancellationAndReleasesChannel() throws Exception {
        try (var repository = new FilesystemSnapshotRepository(newTempDir(), true)) {
            IndexMetadata metadata = metadata();
            repository.create(metadata, UUID.randomUUID(), source(metadata, UUID.randomUUID(), 0, false), context());
            try (var lease = repository.open("books", context())) {
                assertNotNull(lease.manifest());
                OperationContext expired = OperationContext.withTimeout(java.time.Duration.ofMillis(20));
                assertEquals(
                    EngineException.Code.DEADLINE_EXCEEDED,
                    assertThrows(EngineException.class, () -> repository.current("books", expired)).code()
                );
            }
            assertTrue(repository.current("books", context()).isPresent());
        }
    }

    public void testUncertainDirectorySyncCannotReclaimPreviousDurableHead() throws Exception {
        Path root = newTempDir();
        var fail = new java.util.concurrent.atomic.AtomicBoolean();
        FilesystemSnapshotRepository.DirectorySync sync = directory -> {
            if (fail.get() && directory.equals(root.resolve("indices"))) {
                Path state = directory.resolve("books.state");
                if (Files.exists(state)) {
                    try (var input = org.opensearch.core.common.io.stream.StreamInput.wrap(Files.readAllBytes(state))) {
                        if (SnapshotWire.publication(input).manifest().checkpoint().sequence() > 0) throw new IOException(
                            "directory sync failed"
                        );
                    }
                }
            }
            try (var channel = java.nio.channels.FileChannel.open(directory, java.nio.file.StandardOpenOption.READ)) {
                channel.force(true);
            }
        };
        try (var repository = new FilesystemSnapshotRepository(root, true, sync)) {
            IndexMetadata metadata = metadata();
            var first = repository.create(metadata, UUID.randomUUID(), source(metadata, UUID.randomUUID(), 0, false), context());
            fail.set(true);
            assertThrows(
                EngineException.class,
                () -> repository.publish(first, source(metadata, first.manifest().checkpoint().history(), 1, false), context())
            );
            var uncertain = repository.current("books", context()).orElseThrow();
            assertEquals(1, uncertain.manifest().checkpoint().sequence());
            assertThrows(
                EngineException.class,
                () -> repository.publish(uncertain, source(metadata, first.manifest().checkpoint().history(), 2, false), context())
            );
            assertTrue(
                "the last durably acknowledged head must remain recoverable",
                Files.exists(root.resolve("snapshots").resolve(first.snapshotId().toString()))
            );
        }
    }

    public void testInvalidIndexNamesAreRejectedWithoutReplacingTheirException() throws Exception {
        try (var repository = new FilesystemSnapshotRepository(newTempDir(), true)) {
            assertThrows(IllegalArgumentException.class, () -> repository.open("../bad", context()));
        }
    }

    public void testManifestRejectsTraversalDuplicateFilesAndOversizedValues() {
        for (String name : List.of("../x", "a/b", "..", "engine.id", "write.lock")) {
            assertThrows(IllegalArgumentException.class, () -> new SnapshotManifest.File(name, 1, ABC));
        }
        assertThrows(IllegalArgumentException.class, () -> new SnapshotManifest.File("data", Long.MAX_VALUE, ABC));
        IndexMetadata metadata = metadata();
        var file = new SnapshotManifest.File("data", 1, ABC);
        assertThrows(
            IllegalArgumentException.class,
            () -> new SnapshotManifest("lucene", "1", SCHEMA, new Checkpoint(metadata.shard(), UUID.randomUUID(), 0), List.of(file, file))
        );
    }

    private static IndexMetadata metadata() {
        return new IndexMetadata("books", UUID.randomUUID(), "lucene", SCHEMA);
    }

    private static OperationContext context() {
        return OperationContext.standard();
    }

    private static SnapshotSource source(IndexMetadata metadata, UUID history, long sequence, boolean truncated) {
        return new SnapshotSource() {
            public SnapshotManifest manifest() {
                return new SnapshotManifest(
                    "lucene",
                    "lucene-1",
                    SCHEMA,
                    new Checkpoint(metadata.shard(), history, sequence),
                    List.of(new SnapshotManifest.File("data.bin", 3, ABC))
                );
            }

            public InputStream open(String name) throws IOException {
                return new ByteArrayInputStream(truncated ? new byte[] { 'a' } : new byte[] { 'a', 'b', 'c' });
            }

            public void close() {}
        };
    }
}
