/*
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.core.common.bytes;

import java.io.IOException;

/** Iterates leased byte slices; a slice may be reused by the next call. @opensearch.internal */
@FunctionalInterface
public interface ByteSliceIterator {
    /** Returns the next slice, or null at the end. */
    ByteSlice next() throws IOException;
}
