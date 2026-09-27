/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine;

import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.SnapshotManifest;
import org.opensearch.engine.api.SnapshotSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Verified private snapshot copies; no provider file is interpreted here. @opensearch.internal */
final class SnapshotStorage {
    private SnapshotStorage() {}

    static void copy(SnapshotSource source, Path destination, OperationContext context) throws IOException {
        Files.createDirectory(destination);
        byte[] buffer = new byte[65536];
        for (SnapshotManifest.File file : source.manifest().files()) {
            context.check();
            MessageDigest digest = digest();
            long remaining = file.length();
            try (
                InputStream input = source.open(file.name());
                FileChannel output = FileChannel.open(
                    destination.resolve(file.name()),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
                )
            ) {
                while (remaining > 0) {
                    context.check();
                    int count = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (count <= 0) throw new IOException("truncated or stalled snapshot file: " + file.name());
                    digest.update(buffer, 0, count);
                    java.nio.channels.Channels.newOutputStream(output).write(buffer, 0, count);
                    remaining -= count;
                }
                if (input.read() != -1 || HexFormat.of().formatHex(digest.digest()).equals(file.sha256()) == false) {
                    throw new EngineException(EngineException.Code.INCOMPATIBLE, "snapshot checksum or size mismatch: " + file.name());
                }
                output.force(true);
            }
        }
        try (FileChannel directory = FileChannel.open(destination, StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static void delete(Path path) throws IOException {
        if (path == null || Files.exists(path, java.nio.file.LinkOption.NOFOLLOW_LINKS) == false) return;
        if (Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS) == false) {
            Files.delete(path);
            return;
        }
        try (var children = Files.newDirectoryStream(path)) {
            for (Path child : children) {
                if (Files.isDirectory(child, java.nio.file.LinkOption.NOFOLLOW_LINKS)) delete(child);
                else Files.delete(child);
            }
        }
        Files.delete(path);
    }
}
