/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.service;

import org.opensearch.common.io.ProcessFileLock;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.api.IndexMetadata;
import org.opensearch.index.api.IndexWire;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Local durable index catalog, independent of the engine's files and formats. @opensearch.internal */
final class IndexCatalog implements AutoCloseable {
    private static final int MAGIC = 0x4f534c49;
    private final Path root;
    private ProcessFileLock lock;

    IndexCatalog(Path root) {
        this.root = root;
    }

    Map<String, IndexMetadata> load(int maximum, boolean writable) throws IOException {
        if (Files.exists(root) == false) {
            if (writable == false) return Map.of();
            createDirectories(root);
        }
        if (writable) {
            lock = ProcessFileLock.tryAcquire(root.resolve("catalog.lock"));
            if (lock == null) throw new IOException("index catalog is already owned by another node");
        }
        Map<String, IndexMetadata> entries = new HashMap<>();
        Set<UUID> ids = new HashSet<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(root, "*.meta")) {
            for (Path path : files) {
                if (entries.size() >= maximum) throw new IOException("index catalog exceeds configured maximum");
                long size = Files.size(path);
                if (size < 5 || size > 65536) throw new IOException("invalid catalog entry size");
                try (StreamInput input = StreamInput.wrap(Files.readAllBytes(path))) {
                    if (input.readInt() != MAGIC || input.readByte() != 1) throw new IOException("unsupported index catalog format");
                    IndexMetadata metadata = IndexWire.metadata(input);
                    if (input.read() != -1 || path.getFileName().toString().equals(metadata.id() + ".meta") == false) throw new IOException(
                        "catalog entry identity mismatch or trailing data"
                    );
                    if (entries.putIfAbsent(metadata.name(), metadata) != null || ids.add(metadata.id()) == false) throw new IOException(
                        "duplicate index catalog identity"
                    );
                }
            }
        }
        return Map.copyOf(entries);
    }

    void save(IndexMetadata metadata) throws IOException {
        if (lock == null) throw new IOException("index catalog is not writable");
        Path temporary = root.resolve(metadata.id() + ".pending");
        Path target = root.resolve(metadata.id() + ".meta");
        try {
            byte[] bytes;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.writeInt(MAGIC);
                output.writeByte((byte) 1);
                IndexWire.metadata(output, metadata);
                bytes = BytesReference.toBytes(output.bytes());
            }
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                Channels.newOutputStream(channel).write(bytes);
                channel.force(true);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            sync(root);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void createDirectories(Path path) throws IOException {
        if (Files.exists(path)) return;
        createDirectories(path.getParent());
        try {
            Files.createDirectory(path);
        } catch (java.nio.file.FileAlreadyExistsException e) {
            if (Files.isDirectory(path) == false) throw e;
        }
        sync(path.getParent());
    }

    private static void sync(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        ProcessFileLock ownedLock = lock;
        lock = null;
        if (ownedLock != null) ownedLock.close();
    }
}
