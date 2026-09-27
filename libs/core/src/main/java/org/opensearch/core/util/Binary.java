/*
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.core.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/** Primitive byte views and signed variable-length encoding. @opensearch.internal */
public final class Binary {
    private Binary() {}

    public static final VarHandle VH_BE_SHORT = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    public static final VarHandle VH_BE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    public static final VarHandle VH_BE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    public static long zigZagEncode(long value) {
        return (value << 1) ^ (value >> 63);
    }

    public static long zigZagDecode(long value) {
        return (value >>> 1) ^ -(value & 1);
    }
}
