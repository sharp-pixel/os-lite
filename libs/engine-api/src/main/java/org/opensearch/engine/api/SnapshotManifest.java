/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Complete bounded description of an immutable committed snapshot. @opensearch.experimental */
public record SnapshotManifest(String provider, String format, Schema schema, Checkpoint checkpoint, List<File> files) {

    public static final int MAX_FILES = 1024;
    public static final long MAX_BYTES = 1L << 30;

    public SnapshotManifest {
        if (Objects.requireNonNull(provider).matches("[a-z][a-z0-9-]{0,63}") == false
            || Objects.requireNonNull(format).matches("[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}") == false) {
            throw new IllegalArgumentException("invalid snapshot provider or format");
        }
        Objects.requireNonNull(schema);
        Objects.requireNonNull(checkpoint);
        files = List.copyOf(files);
        if (files.isEmpty() || files.size() > MAX_FILES) throw new IllegalArgumentException("snapshot file limit exceeded");
        var names = new HashSet<String>();
        long total = 0;
        for (File file : files) {
            if (names.add(file.name()) == false) throw new IllegalArgumentException("duplicate snapshot file");
            total += file.length();
            if (total > MAX_BYTES) throw new IllegalArgumentException("snapshot byte limit exceeded");
        }
    }

    public long totalBytes() {
        return files.stream().mapToLong(File::length).sum();
    }

    public record File(String name, long length, String sha256) {
        public File {
            if (Objects.requireNonNull(name).matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}") == false
                || name.equals("engine.id")
                || name.equals("write.lock")) throw new IllegalArgumentException("invalid snapshot file name");
            if (length < 0 || length > MAX_BYTES) throw new IllegalArgumentException("snapshot file size exceeds limit");
            if (Objects.requireNonNull(sha256).matches("[0-9a-f]{64}") == false) throw new IllegalArgumentException(
                "invalid SHA-256 digest"
            );
        }
    }
}
