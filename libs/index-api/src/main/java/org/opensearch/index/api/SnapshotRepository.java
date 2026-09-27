/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.api;

import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.SnapshotManifest;
import org.opensearch.engine.api.SnapshotSource;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Blocking durable publication contract, implemented outside index-service. @opensearch.experimental */
public interface SnapshotRepository extends AutoCloseable {
    record Publication(IndexMetadata metadata, long epoch, UUID owner, UUID snapshotId, SnapshotManifest manifest) {
        public Publication {
            Objects.requireNonNull(metadata);
            Objects.requireNonNull(owner);
            Objects.requireNonNull(snapshotId);
            Objects.requireNonNull(manifest);
            if (epoch < 1
                || metadata.shard().equals(manifest.checkpoint().shard()) == false
                || metadata.engine().equals(manifest.provider()) == false
                || metadata.schema().equals(manifest.schema()) == false) {
                throw new IllegalArgumentException("invalid repository publication identity");
            }
        }
    }

    interface Read extends SnapshotSource {
        Publication publication();
    }

    Optional<Publication> current(String index, OperationContext context);

    Read open(String index, OperationContext context);

    Publication create(IndexMetadata metadata, UUID owner, SnapshotSource source, OperationContext context);

    Publication publish(Publication expected, SnapshotSource source, OperationContext context);

    Publication claim(String index, long expectedEpoch, UUID owner, OperationContext context);

    @Override
    void close();
}
