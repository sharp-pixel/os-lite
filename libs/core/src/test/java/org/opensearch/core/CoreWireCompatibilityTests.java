/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.core;

import org.opensearch.Version;
import org.opensearch.core.common.bytes.ByteSlice;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StoreClosedException;
import org.opensearch.core.common.io.stream.StoreCorruptionException;
import org.opensearch.core.common.io.stream.StoreFormatTooNewException;
import org.opensearch.core.common.io.stream.StoreFormatTooOldException;
import org.opensearch.core.common.io.stream.StoreLockException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CoreWireCompatibilityTests {
    private static final int[] INTS = { 0, 1, 127, 128, 16383, 16384, Integer.MAX_VALUE, -1, Integer.MIN_VALUE };
    private static final long[] LONGS = { 0, 1, 127, 128, 16384, Long.MAX_VALUE, -1, Long.MIN_VALUE };
    private static final String[] STRINGS = { "", "ascii", "\u0000", "é中😀", "\ud800x\udfff", "a".repeat(1500) };
    private static final String[] VERSIONS = { "2.0.0", "3.6.0", "3.6.1", "4.0.0" };

    private byte[] fixture(String name) throws IOException {
        try (InputStream input = getClass().getResourceAsStream("/wire/" + name + ".bin")) {
            if (input == null) throw new IOException("missing independent fixture " + name);
            return input.readAllBytes();
        }
    }

    @Test
    public void testDecodeUpstreamValues() throws IOException {
        try (StreamInput input = StreamInput.wrap(fixture("values"))) {
            for (int value : INTS) {
                assertEquals(value, input.readInt());
                assertEquals(value, input.readVInt());
            }
            for (long value : LONGS) {
                assertEquals(value, input.readLong());
                assertEquals(value, input.readZLong());
            }
            for (String value : STRINGS)
                assertEquals(value, input.readString());
            assertArrayEquals(new byte[] { 0, 1, (byte) 255, 2, 3 }, BytesReference.toBytes(input.readBytesReference()));
            assertEquals(new ByteSlice(new byte[] { 0, 1, (byte) 255 }), input.readGenericValue());
            for (String version : VERSIONS)
                assertEquals(version, input.readVersion().toString());
            assertEquals(0, input.available());
        }
    }

    @Test
    public void testEncodeMatchesUpstreamBytes() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (StreamOutput out = new OutputStreamStreamOutput(bytes)) {
            for (int value : INTS) {
                out.writeInt(value);
                out.writeVInt(value);
            }
            for (long value : LONGS) {
                out.writeLong(value);
                out.writeZLong(value);
            }
            for (String value : STRINGS)
                out.writeString(value);
            out.writeBytesReference(
                CompositeBytesReference.of(new BytesArray(new byte[] { 0, 1, (byte) 255 }), new BytesArray(new byte[] { 2, 3 }))
            );
            out.writeGenericValue(new ByteSlice(new byte[] { 9, 0, 1, (byte) 255, 8 }, 1, 3));
            for (String version : VERSIONS)
                out.writeVersion(Version.fromString(version));
        }
        assertArrayEquals(fixture("values"), bytes.toByteArray());
    }

    @Test
    public void testLegacyStorageExceptionsPreserveWireTagsAndPayloads() throws IOException {
        Class<?>[] classes = {
            StoreCorruptionException.class,
            StoreFormatTooNewException.class,
            StoreFormatTooOldException.class,
            StoreFormatTooOldException.class,
            StoreClosedException.class,
            StoreLockException.class };
        for (int i = 0; i < classes.length; i++) {
            byte[] wire = fixture("exception-" + i);
            Exception error;
            try (StreamInput input = StreamInput.wrap(wire)) {
                error = input.readException();
                assertEquals(classes[i], error.getClass());
                assertEquals(0, input.available());
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (StreamOutput output = new OutputStreamStreamOutput(bytes)) {
                output.writeException(error);
            }
            assertArrayEquals(wire, bytes.toByteArray());
            for (int length = 0; length < wire.length; length++) {
                byte[] truncated = Arrays.copyOf(wire, length);
                assertThrows(IOException.class, () -> {
                    try (StreamInput input = StreamInput.wrap(truncated)) {
                        input.readException();
                    }
                });
            }
        }
    }

    @Test
    public void testUtf8TextMatchesUpstreamReplacementSemantics() throws IOException {
        String[] encoded = { "", "plain", "😀é中", "\ud800x\udfff" };
        String[] decoded = { "", "plain", "😀é中", "�x�" };
        byte[] wire = fixture("text");
        try (StreamInput input = StreamInput.wrap(wire)) {
            for (String text : decoded)
                assertEquals(text, input.readText().string());
            assertEquals(0, input.available());
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (StreamOutput output = new OutputStreamStreamOutput(bytes)) {
            for (String text : encoded)
                output.writeText(new org.opensearch.core.common.text.Text(text));
        }
        assertArrayEquals(wire, bytes.toByteArray());
    }

    @Test
    public void testGenericValuesRejectExcessiveNesting() throws IOException {
        // Build the wire payload iteratively, so the test encoder cannot overflow first.
        for (int tag : new int[] { 7, 8, 9, 10, 24, 25 }) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (StreamOutput output = new OutputStreamStreamOutput(bytes)) {
                for (int depth = 0; depth < 1002; depth++) {
                    output.writeByte((byte) tag);
                    output.writeVInt(1);
                    if (tag == 9 || tag == 10) output.writeString("key");
                }
                output.writeByte((byte) -1); // null leaf
            }
            try (StreamInput input = StreamInput.wrap(bytes.toByteArray())) {
                IOException error = assertThrows(IOException.class, input::readGenericValue);
                assertTrue(error.getMessage().contains("Maximum nesting depth"));
            }
        }
    }

    @Test
    public void testCoreHasNoLuceneAtRuntime() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName("org.apache.lucene.util.BytesRef"));
        assertTrue(Version.CURRENT.after(Version.V_3_5_0));
        assertEquals(Version.MASK ^ 3100099, Version.CURRENT.id);
    }
}
