/*
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.core.common.bytes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Mutable, non-owning view of a byte array. Compare as unsigned bytes. @opensearch.internal */
public final class ByteSlice implements Comparable<ByteSlice>, Cloneable {
    public static final byte[] EMPTY_BYTES = new byte[0];
    public byte[] bytes;
    public int offset;
    public int length;

    public ByteSlice() {
        this(EMPTY_BYTES);
    }

    public ByteSlice(int capacity) {
        this(new byte[capacity], 0, 0);
    }

    public ByteSlice(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    public ByteSlice(byte[] bytes, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, bytes.length);
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    public ByteSlice(CharSequence text) {
        this(ByteSliceBuilder.encode(text));
    }

    public String utf8ToString() {
        return new String(bytes, offset, length, StandardCharsets.UTF_8);
    }

    public boolean bytesEquals(ByteSlice other) {
        return Arrays.equals(bytes, offset, offset + length, other.bytes, other.offset, other.offset + other.length);
    }

    @Override
    public int compareTo(ByteSlice other) {
        return Arrays.compareUnsigned(bytes, offset, offset + length, other.bytes, other.offset, other.offset + other.length);
    }

    public static ByteSlice deepCopyOf(ByteSlice slice) {
        return new ByteSlice(Arrays.copyOfRange(slice.bytes, slice.offset, slice.offset + slice.length));
    }

    @Override
    public ByteSlice clone() {
        return new ByteSlice(bytes, offset, length);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ByteSlice slice && bytesEquals(slice);
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (int i = offset; i < offset + length; i++)
            hash = 31 * hash + bytes[i];
        return hash;
    }
}
