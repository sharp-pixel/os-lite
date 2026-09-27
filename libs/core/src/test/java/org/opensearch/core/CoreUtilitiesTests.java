/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.core;

import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.ByteSlice;
import org.opensearch.core.common.bytes.ByteSliceBuilder;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamInput;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.util.ArraySizing;
import org.opensearch.core.util.MemorySize;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.CRC32;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CoreUtilitiesTests {
    @Test
    public void testSliceOwnershipBoundsAndUnsignedOrdering() {
        byte[] bytes = { 9, 0, 1, (byte) 255, 8 };
        ByteSlice slice = new ByteSlice(bytes, 1, 3);
        ByteSlice copy = ByteSlice.deepCopyOf(slice);
        assertSame(bytes, slice.clone().bytes);
        assertNotSame(bytes, copy.bytes);
        assertTrue(new ByteSlice(new byte[] { (byte) 255 }).compareTo(new ByteSlice(new byte[] { 1 })) > 0);
        assertEquals(copy, slice);
        assertEquals(copy.hashCode(), slice.hashCode());
        assertThrows(IndexOutOfBoundsException.class, () -> new ByteSlice(bytes, Integer.MAX_VALUE, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> new ByteSlice(bytes, 1, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> new ByteSlice(bytes, -1, 1));
        bytes[1] = 7;
        assertEquals(0, copy.bytes[0]);
    }

    @Test
    public void testUtf8ReplacementAndReusableBuffer() {
        ByteSliceBuilder builder = new ByteSliceBuilder();
        assertArrayEquals("�x�".getBytes(StandardCharsets.UTF_8), Strings.toUTF8Bytes("\ud800x\udfff", builder));
        assertArrayEquals("😀é中".getBytes(StandardCharsets.UTF_8), Strings.toUTF8Bytes("😀é中", builder));
        ByteSlice stable = builder.toByteSlice();
        builder.copyChars("next");
        assertEquals("😀é中", stable.utf8ToString());
        assertArrayEquals(new byte[0], Strings.toUTF8Bytes("", builder));
    }

    @Test
    public void testCompositeAcrossEveryUtf8Boundary() throws Exception {
        byte[] bytes = "😀é中 search".getBytes(StandardCharsets.UTF_8);
        BytesReference contiguous = new BytesArray(bytes);
        for (int split = 0; split <= bytes.length; split++) {
            BytesReference composite = CompositeBytesReference.of(
                new BytesArray(bytes, 0, split),
                new BytesArray(bytes, split, bytes.length - split)
            );
            assertEquals(contiguous, composite);
            assertEquals(contiguous.hashCode(), composite.hashCode());
            assertEquals(0, composite.compareTo(contiguous));
            assertEquals("😀é中 search", composite.utf8ToString());
            assertArrayEquals(bytes, BytesReference.toBytes(composite));
            try (StreamInput input = composite.streamInput()) {
                assertArrayEquals(bytes, input.readAllBytes());
            }
        }
    }

    @Test
    public void testCompositeRandomSlicesAgainstJdk() {
        Random random = new Random(7183L);
        for (int round = 0; round < 100; round++) {
            byte[] bytes = new byte[1 + random.nextInt(500)];
            random.nextBytes(bytes);
            int split = random.nextInt(bytes.length + 1);
            BytesReference composite = CompositeBytesReference.of(
                new BytesArray(bytes, 0, split),
                new BytesArray(bytes, split, bytes.length - split)
            );
            int from = random.nextInt(bytes.length + 1);
            int length = random.nextInt(bytes.length - from + 1);
            assertArrayEquals(Arrays.copyOfRange(bytes, from, from + length), BytesReference.toBytes(composite.slice(from, length)));
            byte[] other = bytes.clone();
            other[random.nextInt(other.length)] ^= (byte) 255;
            assertEquals(Integer.signum(Arrays.compareUnsigned(bytes, other)), Integer.signum(composite.compareTo(new BytesArray(other))));
        }
    }

    @Test
    public void testChecksumsAgainstJdkAcrossWritesAndSkips() throws Exception {
        byte[] bytes = new byte[4097];
        new Random(473L).nextBytes(bytes);
        CRC32 expected = new CRC32();
        expected.update(bytes);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (BufferedChecksumStreamOutput output = new BufferedChecksumStreamOutput(new OutputStreamStreamOutput(buffer))) {
            output.writeByte(bytes[0]);
            output.writeBytes(bytes, 1, bytes.length - 1);
            assertEquals(expected.getValue(), output.getChecksum());
            output.resetDigest();
            assertEquals(0, output.getChecksum());
        }
        assertArrayEquals(bytes, buffer.toByteArray());
        try (BufferedChecksumStreamInput input = new BufferedChecksumStreamInput(StreamInput.wrap(bytes), "fixture")) {
            input.readByte();
            input.readShort();
            input.readInt();
            input.readLong();
            input.skip(bytes.length - 15);
            assertEquals(expected.getValue(), input.getChecksum());
            input.resetDigest();
            assertEquals(0, input.getChecksum());
        }
    }

    @Test
    public void testGrowthAndMemoryAccountingBoundaries() {
        for (int width : new int[] { 1, 2, 4, 8 }) {
            for (int size : new int[] { 0, 1, 8, 1024, ArraySizing.MAX_ARRAY_LENGTH - 1, ArraySizing.MAX_ARRAY_LENGTH }) {
                int grown = ArraySizing.oversize(size, width);
                assertTrue(grown >= size);
                assertTrue(grown <= ArraySizing.MAX_ARRAY_LENGTH);
            }
        }
        assertThrows(IllegalArgumentException.class, () -> ArraySizing.oversize(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> ArraySizing.oversize(Integer.MAX_VALUE, 1));
        assertThrows(IllegalArgumentException.class, () -> ArraySizing.oversize(1, 0));
        assertThrows(ArithmeticException.class, () -> MemorySize.alignObjectSize(Long.MAX_VALUE));
        assertTrue(MemorySize.sizeOf(new long[9]) >= 9L * Long.BYTES);
        assertTrue(MemorySize.sizeOf(new Object[9]) >= 9L * Long.BYTES);
        assertTrue(MemorySize.shallowSizeOfInstance(ByteSlice.class) >= Long.BYTES + 2L * Integer.BYTES);
    }
}
