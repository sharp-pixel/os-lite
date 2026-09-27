/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.nio.file.Path;
import java.util.Objects;

/** Storage and schema assigned by the runtime, not supplied by HTTP callers. @opensearch.experimental */
public record ShardSpec(ShardId id, Path directory, Schema schema) {
    public ShardSpec {
        Objects.requireNonNull(id);
        Objects.requireNonNull(directory);
        Objects.requireNonNull(schema);
    }
}
