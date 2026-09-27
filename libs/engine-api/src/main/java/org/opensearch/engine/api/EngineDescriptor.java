/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.Objects;
import java.util.Set;

/** Provider compatibility and advertised field support. @opensearch.experimental */
public record EngineDescriptor(String id, int apiVersion, Set<Schema.FieldType> fieldTypes) {

    public static final int CURRENT_API_VERSION = 1;
    public EngineDescriptor {
        if (Objects.requireNonNull(id).matches("[a-z][a-z0-9-]{0,63}") == false) throw new IllegalArgumentException("invalid provider ID");
        fieldTypes = Set.copyOf(fieldTypes);
    }
}
