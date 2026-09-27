/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class PidFileTests extends RandomizedTest {
    public void testRejectsSymbolicLinkWithoutChangingTarget() throws Exception {
        Path directory = newTempDir();
        Path target = directory.resolve("target");
        Files.writeString(target, "unchanged", StandardCharsets.UTF_8);
        Path pid = directory.resolve("service.pid");
        Files.createSymbolicLink(pid, target.getFileName());

        assertThrows(IllegalArgumentException.class, () -> PidFile.create(pid, false, 123L));
        assertEquals("unchanged", Files.readString(target, StandardCharsets.UTF_8));
    }

    public void testWritesExistingRegularFile() throws Exception {
        Path pid = newTempDir().resolve("service.pid");
        Files.writeString(pid, "old", StandardCharsets.UTF_8);

        PidFile.create(pid, false, 123L);

        assertEquals("123", Files.readString(pid, StandardCharsets.UTF_8));
    }
}
