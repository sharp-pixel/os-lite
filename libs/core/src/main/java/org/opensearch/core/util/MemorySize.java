/*
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.core.util;

import org.opensearch.common.SuppressForbidden;

import javax.management.JMException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/** Conservative shallow memory estimates; references are charged at eight bytes. @opensearch.internal */
public final class MemorySize {
    public static final int NUM_BYTES_OBJECT_REF = Long.BYTES;
    public static final int NUM_BYTES_ARRAY_HEADER = 24;
    private static final int ALIGNMENT = alignment();
    private static final ClassValue<Long> SIZES = new ClassValue<>() {
        @Override
        @SuppressForbidden(reason = "Counts declared field metadata without reading values or changing accessibility")
        protected Long computeValue(Class<?> type) {
            if (type.isArray()) throw new IllegalArgumentException("array length is required");
            long size = 16;
            for (Class<?> current = type; current != null; current = current.getSuperclass()) {
                for (Field field : current.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers()) == false) size += Long.BYTES;
                }
            }
            return alignObjectSize(size);
        }
    };

    private MemorySize() {}

    private static int alignment() {
        try {
            var server = ManagementFactory.getPlatformMBeanServer();
            var name = new ObjectName("com.sun.management:type=HotSpotDiagnostic");
            if (server.isRegistered(name)) {
                Object option = server.invoke(
                    name,
                    "getVMOption",
                    new Object[] { "ObjectAlignmentInBytes" },
                    new String[] { "java.lang.String" }
                );
                if (option instanceof CompositeData data) {
                    int alignment = Integer.parseInt((String) data.get("value"));
                    if (alignment >= 8 && alignment <= 256 && Integer.bitCount(alignment) == 1) return alignment;
                }
            }
        } catch (JMException | RuntimeException ignored) { /* Conservative default for other JVMs. */ }
        return 16;
    }

    private static int width(Class<?> type) {
        if (type == boolean.class || type == byte.class) return 1;
        if (type == char.class || type == short.class) return 2;
        if (type == int.class || type == float.class) return 4;
        return 8;
    }

    public static long alignObjectSize(long size) {
        if (size < 0) throw new IllegalArgumentException("negative memory size");
        return Math.multiplyExact(Math.addExact(size, ALIGNMENT - 1L) / ALIGNMENT, ALIGNMENT);
    }

    public static long shallowSizeOfInstance(Class<?> type) {
        return SIZES.get(type);
    }

    public static long sizeOf(Object array) {
        if (array == null) return 0;
        return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Array.getLength(array) * width(array.getClass().getComponentType()));
    }
}
