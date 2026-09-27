/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.Objects;

/** Stable failures across the engine boundary. @opensearch.experimental */
public class EngineException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public enum Code {
        INVALID_ARGUMENT,
        UNSUPPORTED,
        INCOMPATIBLE,
        UNAVAILABLE,
        CLOSED,
        CONFLICT,
        RESOURCE_LIMIT,
        CANCELLED,
        DEADLINE_EXCEEDED,
        IO_ERROR,
        WRITE_OUTCOME_UNKNOWN
    }

    private final Code code;

    public EngineException(Code code, String message) {
        this(code, message, null);
    }

    public EngineException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code);
    }

    public Code code() {
        return code;
    }
}
