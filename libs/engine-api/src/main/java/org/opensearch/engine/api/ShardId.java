/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.Objects;
import java.util.UUID;

/** Stable identity independent of aliases and index names. @opensearch.experimental */
public record ShardId(UUID indexId, int shard) {
    public ShardId {
        Objects.requireNonNull(indexId);
        if (shard < 0) throw new IllegalArgumentException("negative shard ID");
    }
}
