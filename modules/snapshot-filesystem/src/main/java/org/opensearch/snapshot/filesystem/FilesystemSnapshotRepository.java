/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshot.filesystem;

import org.opensearch.common.io.ProcessFileLock;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.SnapshotManifest;
import org.opensearch.engine.api.SnapshotSource;
import org.opensearch.index.api.IndexMetadata;
import org.opensearch.index.api.SnapshotRepository;
import org.opensearch.index.api.SnapshotWire;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Requires coherent cross-process locks, atomic rename and directory sync. @opensearch.internal */
public final class FilesystemSnapshotRepository implements SnapshotRepository {
    @FunctionalInterface
    interface DirectorySync {
        void sync(Path directory) throws IOException;
    }

    private final DirectorySync directorySync;
    private static final int MAX_INDICES = 1024;
    private final Path root;
    private final boolean writable;
    private volatile boolean closed;

    public FilesystemSnapshotRepository(Path root, boolean writable) {
        this(root, writable, FilesystemSnapshotRepository::syncDirectory);
    }

    FilesystemSnapshotRepository(Path root, boolean writable, DirectorySync directorySync) {
        this.root = root.toAbsolutePath().normalize();
        this.writable = writable;
        this.directorySync = directorySync;
    }

    private static EngineException io(String message, IOException cause) {
        return new EngineException(EngineException.Code.IO_ERROR, message, cause);
    }

    private void writer() {
        if (writable == false) throw new EngineException(EngineException.Code.UNSUPPORTED, "repository is read-only");
    }

    private ProcessFileLock acquire(OperationContext context) throws IOException {
        context.check();
        if (closed) throw new EngineException(EngineException.Code.CLOSED, "repository is closed");
        if (writable) directories(root);
        if (Files.isDirectory(root) == false) throw new EngineException(
            EngineException.Code.UNAVAILABLE,
            "repository has not been initialized"
        );
        ProcessFileLock lock = ProcessFileLock.acquire(root.resolve("repository.lock"), writable, () -> {
            context.check();
            if (closed) throw new EngineException(EngineException.Code.CLOSED, "repository is closed");
        });
        boolean success = false;
        try {
            if (writable) {
                directories(root.resolve("indices"));
                directories(root.resolve("snapshots"));
            }
            success = true;
            return lock;
        } finally {
            if (success == false) lock.close();
        }
    }

    private Optional<Publication> read(String index) throws IOException {
        IndexMetadata.validateName(index);
        Path file = root.resolve("indices").resolve(index + ".state");
        if (Files.exists(file) == false) return Optional.empty();
        long size = Files.size(file);
        if (size < 5 || size > 512 * 1024) throw new IOException("invalid publication size");
        try (StreamInput input = StreamInput.wrap(Files.readAllBytes(file))) {
            Publication publication = SnapshotWire.publication(input);
            if (input.read() != -1 || publication.metadata().name().equals(index) == false) throw new IOException(
                "publication identity or trailing data mismatch"
            );
            return Optional.of(publication);
        }
    }

    private Publication required(String index) throws IOException {
        return read(index).orElseThrow(() -> new EngineException(EngineException.Code.UNAVAILABLE, "published index not found: " + index));
    }

    @Override
    @SuppressWarnings("try")
    public Optional<Publication> current(String index, OperationContext context) {
        try (ProcessFileLock ignored = acquire(context)) {
            return read(index);
        } catch (IOException e) {
            throw io("cannot read repository publication", e);
        }
    }

    @Override
    public Read open(String index, OperationContext context) {
        ProcessFileLock lock = null;
        try {
            lock = acquire(context);
            Publication publication = required(index);
            return new ReadLease(publication, lock);
        } catch (Exception e) {
            if (lock != null) {
                try {
                    lock.close();
                } catch (IOException close) {
                    e.addSuppressed(close);
                }
            }
            if (e instanceof RuntimeException failure) throw failure;
            throw io("cannot open repository snapshot", (IOException) e);
        }
    }

    private final class ReadLease implements Read {
        private final Publication publication;
        private final ProcessFileLock lock;
        private final Set<InputStream> streams = new HashSet<>();
        private boolean released;

        ReadLease(Publication publication, ProcessFileLock lock) {
            this.publication = publication;
            this.lock = lock;
        }

        @Override
        public Publication publication() {
            return publication;
        }

        @Override
        public SnapshotManifest manifest() {
            return publication.manifest();
        }

        @Override
        public synchronized InputStream open(String name) throws IOException {
            if (released) throw new IOException("snapshot lease is closed");
            if (manifest().files().stream().noneMatch(file -> file.name().equals(name))) throw new IOException("file is outside snapshot");
            InputStream stream = Files.newInputStream(root.resolve("snapshots").resolve(publication.snapshotId().toString()).resolve(name));
            streams.add(stream);
            return new java.io.FilterInputStream(stream) {
                @Override
                public void close() throws IOException {
                    synchronized (ReadLease.this) {
                        if (streams.remove(in)) in.close();
                    }
                }
            };
        }

        @Override
        public synchronized void close() {
            if (released) return;
            released = true;
            IOException failure = null;
            for (InputStream stream : streams) {
                try {
                    stream.close();
                } catch (IOException e) {
                    if (failure == null) failure = e;
                    else failure.addSuppressed(e);
                }
            }
            streams.clear();
            try {
                lock.close();
            } catch (IOException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            }
            if (failure != null) throw io("cannot release repository snapshot", failure);
        }
    }

    @Override
    @SuppressWarnings("try")
    public Publication create(IndexMetadata metadata, UUID owner, SnapshotSource source, OperationContext context) {
        writer();
        try (ProcessFileLock ignored = acquire(context)) {
            if (read(metadata.name()).isPresent()) throw new EngineException(
                EngineException.Code.CONFLICT,
                "index already exists: " + metadata.name()
            );
            if (collect() >= MAX_INDICES) throw new EngineException(EngineException.Code.RESOURCE_LIMIT, "repository index limit reached");
            Publication publication = new Publication(metadata, 1, owner, UUID.randomUUID(), source.manifest());
            return store(publication, source, context);
        } catch (IOException e) {
            throw io("cannot create repository publication", e);
        }
    }

    @Override
    @SuppressWarnings("try")
    public Publication publish(Publication expected, SnapshotSource source, OperationContext context) {
        writer();
        try (ProcessFileLock ignored = acquire(context)) {
            Publication current = required(expected.metadata().name());
            if (current.epoch() != expected.epoch()
                || current.owner().equals(expected.owner()) == false
                || current.snapshotId().equals(expected.snapshotId()) == false
                || current.metadata().equals(expected.metadata()) == false) {
                throw new EngineException(EngineException.Code.CONFLICT, "writer epoch or publication has changed");
            }
            if (source.manifest().checkpoint().covers(current.manifest().checkpoint()) == false
                || source.manifest().checkpoint().sequence() == current.manifest().checkpoint().sequence()) {
                throw new EngineException(EngineException.Code.CONFLICT, "publication must advance the committed checkpoint");
            }
            collect();
            return store(
                new Publication(current.metadata(), current.epoch(), current.owner(), UUID.randomUUID(), source.manifest()),
                source,
                context
            );
        } catch (IOException e) {
            throw io("cannot publish repository snapshot", e);
        }
    }

    @Override
    @SuppressWarnings("try")
    public Publication claim(String index, long expectedEpoch, UUID owner, OperationContext context) {
        writer();
        try (ProcessFileLock ignored = acquire(context)) {
            Publication current = required(index);
            if (expectedEpoch != current.epoch()) throw new EngineException(EngineException.Code.CONFLICT, "writer epoch has changed");
            if (current.epoch() == Long.MAX_VALUE) throw new EngineException(EngineException.Code.RESOURCE_LIMIT, "writer epoch exhausted");
            Publication next = new Publication(current.metadata(), current.epoch() + 1, owner, current.snapshotId(), current.manifest());
            context.check();
            state(next);
            return next;
        } catch (IOException e) {
            throw io("writer assignment outcome is unknown; inspect repository state", e);
        }
    }

    private Publication store(Publication publication, SnapshotSource source, OperationContext context) throws IOException {
        Path pending = root.resolve("snapshots").resolve(publication.snapshotId() + ".pending");
        Path destination = root.resolve("snapshots").resolve(publication.snapshotId().toString());
        try {
            Files.createDirectory(pending);
            byte[] buffer = new byte[65536];
            for (SnapshotManifest.File file : publication.manifest().files()) {
                context.check();
                MessageDigest digest = digest();
                long remaining = file.length();
                try (
                    InputStream input = source.open(file.name());
                    FileChannel output = FileChannel.open(
                        pending.resolve(file.name()),
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE
                    )
                ) {
                    while (remaining > 0) {
                        context.check();
                        int count = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                        if (count <= 0) throw new IOException("truncated or stalled snapshot file");
                        digest.update(buffer, 0, count);
                        java.nio.channels.Channels.newOutputStream(output).write(buffer, 0, count);
                        remaining -= count;
                    }
                    if (input.read() != -1 || HexFormat.of().formatHex(digest.digest()).equals(file.sha256()) == false) {
                        throw new EngineException(EngineException.Code.INCOMPATIBLE, "snapshot checksum or size mismatch");
                    }
                    output.force(true);
                }
            }
            context.check();
            sync(pending);
            Files.move(pending, destination, StandardCopyOption.ATOMIC_MOVE);
            sync(destination.getParent());
            // The publication boundary must finish once begun; cancellation does not imply rollback.
            state(publication);
            return publication;
        } finally {
            delete(pending);
        }
    }

    private void state(Publication publication) throws IOException {
        Path target = root.resolve("indices").resolve(publication.metadata().name() + ".state");
        Path pending = target.resolveSibling(target.getFileName() + ".pending");
        byte[] bytes;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            SnapshotWire.publication(output, publication);
            bytes = BytesReference.toBytes(output.bytes());
        }
        if (bytes.length > 512 * 1024) throw new IOException("publication exceeds metadata limit");
        try (
            FileChannel output = FileChannel.open(
                pending,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            )
        ) {
            java.nio.channels.Channels.newOutputStream(output).write(bytes);
            output.force(true);
        }
        Files.move(pending, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        sync(target.getParent());
    }

    private int collect() throws IOException {
        // Recover any prior atomic rename whose directory sync failed before reclaiming its predecessor.
        sync(root.resolve("indices"));
        Set<String> referenced = new HashSet<>();
        int count = 0;
        try (var files = Files.newDirectoryStream(root.resolve("indices"), "*.state")) {
            for (Path file : files) {
                if (++count > MAX_INDICES) throw new IOException("repository index limit exceeded");
                String name = file.getFileName().toString();
                referenced.add(required(name.substring(0, name.length() - 6)).snapshotId().toString());
            }
        }
        // All references are validated before deleting any orphan; active read leases hold this lock too.
        try (var files = Files.newDirectoryStream(root.resolve("snapshots"))) {
            for (Path file : files)
                if (referenced.contains(file.getFileName().toString()) == false) delete(file);
        }
        return count;
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private void directories(Path path) throws IOException {
        if (Files.exists(path)) {
            if (Files.isDirectory(path) == false) throw new IOException("repository path is not a directory");
            return;
        }
        directories(path.getParent());
        try {
            Files.createDirectory(path);
        } catch (java.nio.file.FileAlreadyExistsException e) {
            if (Files.isDirectory(path) == false) throw e;
        }
        sync(path.getParent());
    }

    private void sync(Path path) throws IOException {
        directorySync.sync(path);
    }

    private static void syncDirectory(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void delete(Path path) throws IOException {
        if (Files.exists(path, java.nio.file.LinkOption.NOFOLLOW_LINKS) == false) return;
        if (Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            try (var children = Files.newDirectoryStream(path)) {
                for (Path child : children)
                    delete(child);
            }
        }
        Files.delete(path);
    }

    @Override
    public void close() {
        closed = true;
    }
}
