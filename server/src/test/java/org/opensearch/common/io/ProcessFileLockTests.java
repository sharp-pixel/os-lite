/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import java.nio.file.Path;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class ProcessFileLockTests extends RandomizedTest {
    @SuppressWarnings("try") // Repeated close proves ownership is released exactly once.
    public void testCanonicalPathsShareOneGateAndCloseIsIdempotent() throws Exception {
        Path file = newTempDir().resolve("owner.lock");
        try (ProcessFileLock first = ProcessFileLock.tryAcquire(file)) {
            assertNotNull(first);
            assertNull(ProcessFileLock.tryAcquire(file.getParent().resolve(".").resolve("owner.lock")));
            first.close();
            first.close();
            try (ProcessFileLock next = ProcessFileLock.tryAcquire(file)) {
                assertNotNull(next);
            }
        }
    }

    public void testCancelledWaitDoesNotReleaseCurrentOwner() throws Exception {
        Path file = newTempDir().resolve("owner.lock");
        try (ProcessFileLock first = ProcessFileLock.tryAcquire(file)) {
            assertNotNull(first);
            AtomicInteger checks = new AtomicInteger();
            assertThrows(CancellationException.class, () -> ProcessFileLock.acquire(file, true, () -> {
                if (checks.incrementAndGet() > 2) throw new CancellationException("cancelled");
            }));
            assertNull(ProcessFileLock.tryAcquire(file));
        }
        try (ProcessFileLock next = ProcessFileLock.tryAcquire(file)) {
            assertNotNull(next);
        }
    }
}
