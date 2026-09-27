/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.Objects;

/** Every acknowledged operation is covered by the durable checkpoint. @opensearch.experimental */
public record WriteResult(int operations, Checkpoint checkpoint) {
    public WriteResult {
        Objects.requireNonNull(checkpoint);
        if (operations < 0) throw new IllegalArgumentException("negative operation count");
    }
}
