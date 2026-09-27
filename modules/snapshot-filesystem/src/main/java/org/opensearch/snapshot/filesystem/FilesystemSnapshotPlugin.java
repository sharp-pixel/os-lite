/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshot.filesystem;

import org.opensearch.index.api.IndexExtension;
import org.opensearch.index.api.SnapshotRepository;
import org.opensearch.index.api.SnapshotRepositoryProvider;
import org.opensearch.plugins.Plugin;

import java.nio.file.Path;
import java.util.List;

/** Reference repository plugin; all coordination stays in index-service. @opensearch.internal */
public final class FilesystemSnapshotPlugin extends Plugin implements IndexExtension {
    @Override
    public List<SnapshotRepositoryProvider> repositoryProviders() {
        return List.of(new SnapshotRepositoryProvider() {
            @Override
            public String id() {
                return "filesystem";
            }

            @Override
            public SnapshotRepository open(Path root, boolean writable) {
                return new FilesystemSnapshotRepository(root, writable);
            }
        });
    }
}
