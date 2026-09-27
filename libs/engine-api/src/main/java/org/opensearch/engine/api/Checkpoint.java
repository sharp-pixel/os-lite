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

/** Durable engine-owned position in one shard history. @opensearch.experimental */
public record Checkpoint(ShardId shard, UUID history, long sequence) {
    public Checkpoint {
        Objects.requireNonNull(shard);
        Objects.requireNonNull(history);
        if (sequence < 0) throw new IllegalArgumentException("negative sequence");
    }

    public boolean covers(Checkpoint minimum) {
        if (shard.equals(minimum.shard) == false || history.equals(minimum.history) == false) {
            throw new EngineException(EngineException.Code.INCOMPATIBLE, "checkpoint belongs to a different shard or history");
        }
        return sequence >= minimum.sequence;
    }
}
