/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.api;

import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.ShardId;

import java.util.Objects;
import java.util.UUID;

/** Persistent logical identity and schema for one local shard. @opensearch.experimental */
public record IndexMetadata(String name, UUID id, String engine, Schema schema) {
    public IndexMetadata {
        validateName(name);
        Objects.requireNonNull(id);
        Objects.requireNonNull(schema);
        if (Objects.requireNonNull(engine).matches("[a-z][a-z0-9-]{0,63}") == false) throw new IllegalArgumentException(
            "invalid engine ID"
        );
    }

    public static void validateName(String name) {
        if (Objects.requireNonNull(name).matches("[a-z0-9][a-z0-9._-]{0,127}") == false) {
            throw new IllegalArgumentException(
                "index name must be 1 to 128 lowercase ASCII letters, digits, dots, underscores or hyphens, starting with a letter or digit"
            );
        }
    }

    public ShardId shard() {
        return new ShardId(id, 0);
    }
}
