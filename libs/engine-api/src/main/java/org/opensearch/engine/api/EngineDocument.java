/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/** Owned document values; source arrays are copied on input and output. @opensearch.experimental */
public record EngineDocument(String id, Map<String, String> fields, byte[] source) {
    public EngineDocument {
        validateId(id);
        if (fields.size() > 128 || Objects.requireNonNull(source).length > 1_048_576) throw new IllegalArgumentException(
            "document exceeds size limits"
        );
        fields = Map.copyOf(fields);
        long bytes = source.length;
        for (Map.Entry<String, String> field : fields.entrySet()) {
            bytes += 2L * ((long) field.getKey().length() + field.getValue().length());
        }
        if (bytes > 1_048_576) throw new IllegalArgumentException("document exceeds size limits");
        fields.forEach((name, value) -> {
            validateUnicode(name, "document field name");
            validateUnicode(value, "document field value");
        });
        source = source.clone();
    }

    @Override
    public byte[] source() {
        return source.clone();
    }

    public long estimatedBytes() {
        long bytes = source.length + 2L * id.length() + 256;
        for (Map.Entry<String, String> field : fields.entrySet())
            bytes += 2L * ((long) field.getKey().length() + field.getValue().length()) + 128;
        return bytes;
    }

    public static void validateId(String id) {
        if (Objects.requireNonNull(id).isEmpty() || id.length() > 1024 || id.getBytes(StandardCharsets.UTF_8).length > 1024) {
            throw new IllegalArgumentException("document ID must contain 1 to 1024 UTF-8 bytes");
        }
        validateUnicode(id, "document ID");
    }

    static void validateUnicode(String value, String description) {
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (Character.isSurrogate(character)) {
                if (Character.isHighSurrogate(character) == false
                    || i + 1 == value.length()
                    || Character.isLowSurrogate(value.charAt(++i)) == false) {
                    throw new IllegalArgumentException(description + " must be valid Unicode");
                }
            }
        }
    }
}
