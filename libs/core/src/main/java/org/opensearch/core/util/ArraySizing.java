/*
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.core.util;

import java.util.Arrays;

/** Checked array growth shared by transport buffers and paged arrays. @opensearch.internal */
public final class ArraySizing {
    public static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

    private ArraySizing() {}

    public static int oversize(int minimum, int bytesPerElement) {
        if (minimum < 0 || minimum > MAX_ARRAY_LENGTH || bytesPerElement <= 0) throw new IllegalArgumentException("invalid array capacity");
        if (minimum == 0) return 0;
        long size = minimum + Math.max(3L, minimum / 8L);
        int alignment = Math.max(1, Long.BYTES / bytesPerElement);
        size = ((size + alignment - 1) / alignment) * alignment;
        return (int) Math.min(MAX_ARRAY_LENGTH, size);
    }

    public static byte[] copyOfSubArray(byte[] bytes, int from, int to) {
        java.util.Objects.checkFromToIndex(from, to, bytes.length);
        return Arrays.copyOfRange(bytes, from, to);
    }
}
