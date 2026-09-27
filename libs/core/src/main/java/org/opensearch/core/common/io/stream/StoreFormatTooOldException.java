/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.core.common.io.stream;

/** Engine-neutral representation of a legacy transport storage error. @opensearch.internal */
public final class StoreFormatTooOldException extends java.io.IOException {
    private static final long serialVersionUID = 1L;
    private final String resource, reason;
    private final Integer version, minimum, maximum;

    public StoreFormatTooOldException(String resource, int version, int minimum, int maximum) {
        super(
            "Format version is not supported (resource "
                + resource
                + "): "
                + version
                + " (needs to be between "
                + minimum
                + " and "
                + maximum
                + ")"
        );
        this.resource = resource;
        this.version = version;
        this.minimum = minimum;
        this.maximum = maximum;
        this.reason = null;
    }

    public StoreFormatTooOldException(String resource, String reason) {
        super("Format version is not supported (resource " + resource + "): " + reason);
        this.resource = resource;
        this.reason = reason;
        this.version = null;
        this.minimum = null;
        this.maximum = null;
    }

    public String getResourceDescription() {
        return resource;
    }

    public String getReason() {
        return reason;
    }

    public Integer getVersion() {
        return version;
    }

    public Integer getMinVersion() {
        return minimum;
    }

    public Integer getMaxVersion() {
        return maximum;
    }
}
