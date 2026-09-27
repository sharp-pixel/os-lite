/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.core.common.io.stream;

/** Engine-neutral representation of a legacy transport storage error. @opensearch.internal */
public final class StoreFormatTooNewException extends java.io.IOException {
    private static final long serialVersionUID = 1L;
    private final String resource;
    private final int version, minimum, maximum;

    public StoreFormatTooNewException(String resource, int version, int minimum, int maximum) {
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
    }

    public String getResourceDescription() {
        return resource;
    }

    public int getVersion() {
        return version;
    }

    public int getMinVersion() {
        return minimum;
    }

    public int getMaxVersion() {
        return maximum;
    }
}
