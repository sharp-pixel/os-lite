/*
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.core.common.bytes;

import org.opensearch.core.util.ArraySizing;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Reusable byte buffer for core serialization. @opensearch.internal */
public final class ByteSliceBuilder {
    private byte[] bytes = ByteSlice.EMPTY_BYTES;
    private int length;

    public void grow(int capacity) {
        if (capacity > bytes.length) bytes = Arrays.copyOf(bytes, ArraySizing.oversize(capacity, 1));
    }

    public byte[] bytes() {
        return bytes;
    }

    public int length() {
        return length;
    }

    public void append(ByteSlice slice) {
        int newLength = Math.addExact(length, slice.length);
        grow(newLength);
        System.arraycopy(slice.bytes, slice.offset, bytes, length, slice.length);
        length = newLength;
    }

    public void copyChars(CharSequence text) {
        byte[] encoded = encode(text);
        grow(encoded.length);
        System.arraycopy(encoded, 0, bytes, 0, encoded.length);
        length = encoded.length;
    }

    public ByteSlice toByteSlice() {
        return new ByteSlice(Arrays.copyOf(bytes, length));
    }

    static byte[] encode(CharSequence text) {
        try {
            ByteBuffer buffer = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE)
                .replaceWith(new byte[] { (byte) 0xef, (byte) 0xbf, (byte) 0xbd })
                .encode(CharBuffer.wrap(text));
            byte[] result = new byte[buffer.remaining()];
            buffer.get(result);
            return result;
        } catch (CharacterCodingException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
