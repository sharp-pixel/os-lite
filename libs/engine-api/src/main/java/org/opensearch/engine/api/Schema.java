/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Explicit immutable mapping for the initial text backend. @opensearch.experimental */
public record Schema(long version, Map<String, FieldType> fields) {
    public enum FieldType {
        KEYWORD,
        TEXT
    }

    public Schema {
        if (version < 1) throw new IllegalArgumentException("schema version must be positive");
        if (fields.isEmpty() || fields.size() > 128) throw new IllegalArgumentException("schema must have 1 to 128 fields");
        fields = Map.copyOf(fields);
        fields.forEach((name, type) -> {
            if (name.matches("[A-Za-z][A-Za-z0-9_.]{0,127}") == false) throw new IllegalArgumentException("invalid field name: " + name);
            Objects.requireNonNull(type);
        });
    }

    public String signature() {
        StringBuilder value = new StringBuilder().append(version).append('\n');
        new TreeMap<>(fields).forEach((name, type) -> value.append(name).append('=').append(type.name()).append('\n'));
        return value.toString();
    }
}
