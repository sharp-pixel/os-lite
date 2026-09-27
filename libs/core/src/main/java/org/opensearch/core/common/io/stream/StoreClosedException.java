/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.core.common.io.stream;

/** Engine-neutral representation of a legacy transport storage error. @opensearch.internal */
public final class StoreClosedException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public StoreClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}
