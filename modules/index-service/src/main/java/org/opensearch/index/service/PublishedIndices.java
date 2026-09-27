/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.service;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.io.ProcessFileLock;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.engine.api.Checkpoint;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.EngineService;
import org.opensearch.engine.api.Mutation;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.env.Environment;
import org.opensearch.index.api.IndexMetadata;
import org.opensearch.index.api.IndexRequest;
import org.opensearch.index.api.IndexResponse;
import org.opensearch.index.api.SnapshotRepository;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/** Repository coordination, serialized by the bounded index-service worker. @opensearch.internal */
final class PublishedIndices implements AutoCloseable {
    private final EngineService engine;
    private final SnapshotRepository repository;
    private final Path storage;
    private final Path localCatalog;
    private final boolean writer;
    private final int maximum;
    private final Map<String, Entry> entries = new HashMap<>();
    private ProcessFileLock ownerLock;
    private UUID owner;
    private boolean creationFailed;

    PublishedIndices(EngineService engine, Environment environment, Settings settings, RepositoryRegistry registry) {
        this.engine = engine;
        this.writer = engine.supportsMode(EngineService.Mode.WRITE_ONLY);
        if (writer == engine.supportsMode(EngineService.Mode.READ_ONLY)) throw new IllegalArgumentException(
            "repository nodes require exactly one engine role: reader or writer"
        );
        String providerId = IndexServicePlugin.REPOSITORY.get(settings);
        var provider = registry.providers().get(providerId);
        if (provider == null) throw new IllegalArgumentException("repository provider is not installed: " + providerId);
        String path = IndexServicePlugin.REPOSITORY_PATH.get(settings);
        if (path.isBlank() || Path.of(path).isAbsolute() == false) throw new IllegalArgumentException("repository_path must be absolute");
        this.repository = provider.open(Path.of(path), writer);
        this.storage = environment.dataFiles()[0].resolve("snapshot-node");
        this.localCatalog = environment.dataFiles()[0].resolve("index-catalog");
        this.maximum = IndexServicePlugin.MAX_INDICES.get(settings);
    }

    void start() throws IOException {
        if (Files.isDirectory(localCatalog)) {
            try (var files = Files.newDirectoryStream(localCatalog, "*.meta")) {
                if (files.iterator().hasNext()) throw new IOException(
                    "use a separate data directory for the repository profile; local indices require explicit migration"
                );
            }
        }
        directories(storage);
        ownerLock = ProcessFileLock.tryAcquire(storage.resolve("node.lock"));
        if (ownerLock == null) throw new IOException("snapshot node storage is already in use");
        Path identity = storage.resolve("owner");
        if (Files.exists(identity) == false) {
            Path temporary = storage.resolve("owner.pending");
            try (
                FileChannel output = FileChannel.open(
                    temporary,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
                )
            ) {
                java.nio.channels.Channels.newOutputStream(output)
                    .write(UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                output.force(true);
            }
            Files.move(temporary, identity, StandardCopyOption.ATOMIC_MOVE);
            sync(storage);
        }
        if (Files.size(identity) != 36) throw new IOException("invalid snapshot owner identity");
        try {
            owner = UUID.fromString(Files.readString(identity));
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid snapshot owner identity", e);
        }
    }

    private void requireWriter() {
        if (writer == false) throw new EngineException(EngineException.Code.UNSUPPORTED, "operation requires a writer node");
    }

    private void requireReader() {
        if (writer) throw new EngineException(EngineException.Code.UNSUPPORTED, "operation requires a reader node");
    }

    IndexResponse.Metadata create(IndexRequest.Create request, OperationContext context) throws Exception {
        requireWriter();
        if (creationFailed) throw new EngineException(
            EngineException.Code.UNAVAILABLE,
            "creation outcome is unknown; restart and inspect repository"
        );
        if (repository.current(request.index(), context).isPresent()) throw new OpenSearchStatusException(
            "index already exists",
            RestStatus.CONFLICT
        );
        if (entries.size() >= maximum) throw new EngineException(EngineException.Code.RESOURCE_LIMIT, "index limit reached");
        IndexMetadata metadata = new IndexMetadata(request.index(), UUID.randomUUID(), request.engine(), request.schema());
        await(engine.createSnapshotWriter(metadata.engine(), metadata.shard(), metadata.schema(), context));
        try {
            SnapshotRepository.Publication publication = await(
                engine.withSnapshot(metadata.shard(), source -> repository.create(metadata, owner, source, context), context)
            );
            Entry entry = new Entry(publication);
            entry.opened = true;
            entries.put(metadata.name(), entry);
            return response(publication);
        } catch (Exception e) {
            creationFailed = true;
            try {
                await(engine.closeShard(metadata.shard(), OperationContext.standard()));
            } catch (Exception close) {
                e.addSuppressed(close);
            }
            throw new EngineException(
                EngineException.Code.WRITE_OUTCOME_UNKNOWN,
                "index creation outcome is unknown; restart and inspect repository",
                e
            );
        }
    }

    IndexResponse.Metadata describe(IndexRequest.Describe request, OperationContext context) {
        return response(current(request.index(), context));
    }

    private static IndexResponse.Metadata response(SnapshotRepository.Publication publication) {
        return new IndexResponse.Metadata(publication.metadata(), publication.epoch(), publication.manifest().checkpoint());
    }

    private SnapshotRepository.Publication current(String name, OperationContext context) {
        return repository.current(name, context)
            .orElseThrow(() -> new OpenSearchStatusException("index not found [" + name + "]", RestStatus.NOT_FOUND));
    }

    private Entry entry(String name, OperationContext context) {
        Entry entry = entries.get(name);
        if (entry == null) {
            if (entries.size() >= maximum) throw new EngineException(EngineException.Code.RESOURCE_LIMIT, "index limit reached");
            entry = new Entry(current(name, context));
            entries.put(name, entry);
        }
        return entry;
    }

    private void openWriter(Entry entry, OperationContext context) throws Exception {
        requireWriter();
        if (entry.failed) throw new EngineException(EngineException.Code.UNAVAILABLE, "writer outcome is unknown; recover before retrying");
        SnapshotRepository.Publication latest = current(entry.metadata.name(), context);
        if (latest.owner().equals(owner) == false || (entry.opened && latest.equals(entry.publication) == false)) {
            throw new EngineException(
                EngineException.Code.CONFLICT,
                "writer assignment or repository head changed; explicit takeover is required"
            );
        }
        if (entry.opened == false) install(entry, context);
    }

    private Checkpoint install(Entry entry, OperationContext context) throws Exception {
        AtomicReference<SnapshotRepository.Publication> captured = new AtomicReference<>();
        Checkpoint installed = await(
            engine.installSnapshot(
                entry.metadata.engine(),
                entry.metadata.shard(),
                entry.metadata.schema(),
                writer ? EngineService.Mode.WRITE_ONLY : EngineService.Mode.READ_ONLY,
                () -> {
                    SnapshotRepository.Read source = repository.open(entry.metadata.name(), context);
                    boolean valid = false;
                    try {
                        SnapshotRepository.Publication publication = source.publication();
                        if (publication.metadata().equals(entry.metadata) == false) throw new EngineException(
                            EngineException.Code.INCOMPATIBLE,
                            "published index identity changed"
                        );
                        if (writer && publication.owner().equals(owner) == false) throw new EngineException(
                            EngineException.Code.CONFLICT,
                            "writer assignment changed"
                        );
                        if (entry.visible != null && publication.manifest().checkpoint().covers(entry.visible) == false) {
                            throw new EngineException(EngineException.Code.INCOMPATIBLE, "published checkpoint moved backwards");
                        }
                        captured.set(publication);
                        valid = true;
                        return source;
                    } finally {
                        if (valid == false) source.close();
                    }
                },
                context
            )
        );
        entry.publication = captured.get();
        entry.visible = installed;
        entry.opened = true;
        return installed;
    }

    IndexResponse.Mutation put(IndexRequest.Put request, OperationContext context) throws Exception {
        return mutate(request.index(), request.document().id(), new Mutation.Put(request.document()), context);
    }

    IndexResponse.Mutation delete(IndexRequest.Delete request, OperationContext context) throws Exception {
        return mutate(request.index(), request.id(), new Mutation.Delete(request.id()), context);
    }

    private IndexResponse.Mutation mutate(String name, String id, Mutation mutation, OperationContext context) throws Exception {
        requireWriter();
        Entry entry = entry(name, context);
        openWriter(entry, context);
        var written = await(engine.write(entry.metadata.shard(), List.of(mutation), context));
        try {
            SnapshotRepository.Publication next = await(
                engine.withSnapshot(entry.metadata.shard(), source -> repository.publish(entry.publication, source, context), context)
            );
            entry.publication = next;
            return new IndexResponse.Mutation(name, id, written.checkpoint());
        } catch (Exception e) {
            entry.failed = true;
            throw new EngineException(
                EngineException.Code.WRITE_OUTCOME_UNKNOWN,
                "repository publication outcome is unknown; recover before retrying",
                e
            );
        }
    }

    IndexResponse.Metadata claim(IndexRequest.Claim request, OperationContext context) throws Exception {
        requireWriter();
        Entry entry = entry(request.index(), context);
        if (entry.opened) {
            await(engine.closeShard(entry.metadata.shard(), context));
            entry.opened = false;
        }
        try {
            entry.publication = repository.claim(request.index(), request.expectedEpoch(), owner, context);
            entry.failed = false;
            entry.visible = null;
            return response(entry.publication);
        } catch (EngineException e) {
            if (e.code() != EngineException.Code.CONFLICT) entry.failed = true;
            throw e;
        }
    }

    IndexResponse.Refreshed refresh(IndexRequest.Refresh request, OperationContext context) throws Exception {
        requireReader();
        Entry entry = entry(request.index(), context);
        return new IndexResponse.Refreshed(request.index(), refresh(entry, context));
    }

    private Checkpoint refresh(Entry entry, OperationContext context) throws Exception {
        SnapshotRepository.Publication latest = current(entry.metadata.name(), context);
        if (entry.opened && latest.manifest().checkpoint().equals(entry.visible)) return entry.visible;
        return install(entry, context);
    }

    IndexResponse.Document get(IndexRequest.Get request, OperationContext context) throws Exception {
        requireReader();
        Entry entry = entry(request.index(), context);
        if (entry.opened == false) install(entry, context);
        return new IndexResponse.Document(request.index(), request.id(), await(engine.get(entry.metadata.shard(), request.id(), context)));
    }

    IndexResponse.Search search(IndexRequest.Search request, OperationContext context) throws Exception {
        requireReader();
        Entry entry = entry(request.index(), context);
        if (entry.opened == false) install(entry, context);
        if (request.minimum() != null) {
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(request.waitMillis());
            while (entry.visible.covers(request.minimum()) == false) {
                context.check();
                refresh(entry, context);
                if (entry.visible.covers(request.minimum())) break;
                if (request.waitMillis() == 0 || System.nanoTime() - deadline >= 0) throw new EngineException(
                    EngineException.Code.DEADLINE_EXCEEDED,
                    "minimum checkpoint is not published before the wait deadline"
                );
                LockSupport.parkNanos(Math.min(50_000_000L, Math.max(1, deadline - System.nanoTime())));
            }
        }
        var result = await(engine.search(entry.metadata.shard(), request.query(), request.limit(), context));
        if (request.minimum() != null && result.checkpoint().covers(request.minimum()) == false) throw new EngineException(
            EngineException.Code.UNAVAILABLE,
            "reader did not reach requested checkpoint"
        );
        return new IndexResponse.Search(request.index(), result);
    }

    private static final class Entry {
        final IndexMetadata metadata;
        SnapshotRepository.Publication publication;
        Checkpoint visible;
        boolean opened;
        boolean failed;

        Entry(SnapshotRepository.Publication publication) {
            this.metadata = publication.metadata();
            this.publication = publication;
        }
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        try {
            return stage.toCompletableFuture().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) throw cause;
            if (e.getCause() instanceof Error cause) throw cause;
            throw e;
        }
    }

    private static void directories(Path path) throws IOException {
        if (Files.exists(path)) return;
        directories(path.getParent());
        try {
            Files.createDirectory(path);
        } catch (java.nio.file.FileAlreadyExistsException e) {
            if (Files.isDirectory(path) == false) throw e;
        }
        sync(path.getParent());
    }

    private static void sync(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        ProcessFileLock lock = ownerLock;
        ownerLock = null;
        try {
            repository.close();
        } catch (RuntimeException e) {
            throw new IOException("cannot close repository", e);
        } finally {
            if (lock != null) lock.close();
        }
    }
}
