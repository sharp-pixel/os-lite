/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/** Cooperative cancellation and a monotonic deadline, including time spent queued. @opensearch.experimental */
public final class OperationContext {
    private final long deadline;
    private final AtomicBoolean cancelled = new AtomicBoolean();

    private OperationContext(Duration timeout) {
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofSeconds(30)) > 0) {
            throw new IllegalArgumentException("timeout must be positive and at most 30 seconds");
        }
        deadline = System.nanoTime() + timeout.toNanos();
    }

    public static OperationContext withTimeout(Duration timeout) {
        return new OperationContext(timeout);
    }

    public static OperationContext standard() {
        return withTimeout(Duration.ofSeconds(30));
    }

    public void cancel() {
        cancelled.set(true);
    }

    public boolean expired() {
        return deadline - System.nanoTime() <= 0;
    }

    public boolean cancelled() {
        return cancelled.get() || Thread.currentThread().isInterrupted();
    }

    public void check() {
        if (cancelled()) throw new EngineException(EngineException.Code.CANCELLED, "engine operation cancelled");
        if (expired()) throw new EngineException(EngineException.Code.DEADLINE_EXCEEDED, "engine operation deadline exceeded");
    }
}
