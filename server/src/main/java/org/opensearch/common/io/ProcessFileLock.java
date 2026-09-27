/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.LockSupport;

/**
 * A process-wide gate ensures only one channel for a lock file is open at a time.
 * Closing a second channel can otherwise release another channel's OS lock on some platforms.
 * This class lives in the parent classloader so plugin classloaders share the same gates.
 * @opensearch.internal
 */
public final class ProcessFileLock implements AutoCloseable {
    private static final Map<Path, Gate> GATES = new HashMap<>();
    private final Path key;
    private final Gate gate;
    private final FileChannel channel;
    private final FileLock lock;
    private boolean closed;

    private static final class Gate {
        final Semaphore semaphore = new Semaphore(1, true);
        int users;
    }

    private ProcessFileLock(Path key, Gate gate, FileChannel channel, FileLock lock) {
        this.key = key;
        this.gate = gate;
        this.channel = channel;
        this.lock = lock;
    }

    /** Waits cooperatively; check must throw when cancellation or a deadline is reached. */
    public static ProcessFileLock acquire(Path path, boolean create, Runnable check) throws IOException {
        return acquire(path, create, check, false);
    }

    /** Returns null immediately if another cooperating owner holds the lock. */
    public static ProcessFileLock tryAcquire(Path path) throws IOException {
        return acquire(path, true, () -> {}, true);
    }

    private static ProcessFileLock acquire(Path path, boolean create, Runnable check, boolean immediate) throws IOException {
        Path absolute = path.toAbsolutePath().normalize();
        Path key = absolute.getParent().toRealPath().resolve(absolute.getFileName());
        Gate gate;
        synchronized (GATES) {
            gate = GATES.computeIfAbsent(key, ignored -> new Gate());
            gate.users++;
        }
        boolean entered = false, success = false;
        FileChannel channel = null;
        try {
            while (entered == false) {
                check.run();
                entered = gate.semaphore.tryAcquire();
                if (entered == false) {
                    if (immediate) return null;
                    LockSupport.parkNanos(10_000_000L);
                }
            }
            channel = create
                ? FileChannel.open(key, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
                : FileChannel.open(key, StandardOpenOption.WRITE);
            while (true) {
                check.run();
                FileLock lock = channel.tryLock();
                if (lock != null) {
                    ProcessFileLock result = new ProcessFileLock(key, gate, channel, lock);
                    success = true;
                    return result;
                }
                if (immediate) return null;
                LockSupport.parkNanos(10_000_000L);
            }
        } finally {
            if (success == false) {
                try {
                    if (channel != null) channel.close();
                } finally {
                    if (entered) gate.semaphore.release();
                    release(key, gate);
                }
            }
        }
    }

    private static void release(Path key, Gate gate) {
        synchronized (GATES) {
            if (--gate.users == 0) GATES.remove(key, gate);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            lock.close();
        } finally {
            try {
                channel.close();
            } finally {
                gate.semaphore.release();
                release(key, gate);
            }
        }
    }
}
