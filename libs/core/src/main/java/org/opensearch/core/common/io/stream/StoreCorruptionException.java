/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.core.common.io.stream;

/** Engine-neutral representation of a legacy transport storage error. @opensearch.internal */
public final class StoreCorruptionException extends java.io.IOException {
    private static final long serialVersionUID = 1L;
    private final String originalMessage;
    private final String resource;

    public StoreCorruptionException(String message, String resource, Throwable cause) {
        super(message + " (resource=" + resource + ")", cause);
        this.originalMessage = message;
        this.resource = resource;
    }

    public String getOriginalMessage() {
        return originalMessage;
    }

    public String getResourceDescription() {
        return resource;
    }
}
