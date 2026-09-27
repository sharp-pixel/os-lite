/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Asynchronous admission and shard ownership supplied by the engine module.
 * Callers chain stages to impose operation ordering. Each call needs a context whose deadline includes queue time.
 * Value/argument validation may fail synchronously; admitted operation failures complete the returned stage exceptionally.
 * Cancelling a returned future requests cooperative cancellation and does not prove a mutation was rolled back.
 *
 * @opensearch.experimental
 */
public interface EngineService {
    enum Mode {
        READ_ONLY,
        WRITE_ONLY,
        READ_WRITE
    }

    Set<String> providers();

    /** Whether the node permits the reader/writer capabilities needed by this mode. */
    boolean supportsMode(Mode mode);

    CompletionStage<Void> openShard(String provider, ShardId id, Schema schema, Mode mode, OperationContext context);

    /** Acknowledges a validated batch only after durable commit; does not refresh readers. */
    CompletionStage<WriteResult> write(ShardId id, List<Mutation> mutations, OperationContext context);

    /** Advances the reader to an available committed snapshot and returns its visible checkpoint. */
    CompletionStage<Checkpoint> refresh(ShardId id, OperationContext context);

    CompletionStage<SearchResult> search(ShardId id, SearchQuery query, int limit, OperationContext context);

    CompletionStage<Optional<EngineDocument>> get(ShardId id, String documentId, OperationContext context);

    /** Creates a writer in private disposable storage for an initial repository publication. */
    default CompletionStage<Void> createSnapshotWriter(String provider, ShardId id, Schema schema, OperationContext context) {
        return java.util.concurrent.CompletableFuture.failedFuture(
            new EngineException(EngineException.Code.UNSUPPORTED, "snapshots are unavailable")
        );
    }

    /** Executes a bounded blocking transfer while the runtime owns the source lease. */
    default <T> CompletionStage<T> withSnapshot(ShardId id, SnapshotOperation<T> transfer, OperationContext context) {
        return java.util.concurrent.CompletableFuture.failedFuture(
            new EngineException(EngineException.Code.UNSUPPORTED, "snapshots are unavailable")
        );
    }

    /** Opens and closes the source on a worker, verifies its files, then installs the exact snapshot atomically. */
    default CompletionStage<Checkpoint> installSnapshot(
        String provider,
        ShardId id,
        Schema schema,
        Mode mode,
        java.util.function.Supplier<SnapshotSource> source,
        OperationContext context
    ) {
        return java.util.concurrent.CompletableFuture.failedFuture(
            new EngineException(EngineException.Code.UNSUPPORTED, "snapshot installation is unavailable")
        );
    }

    @FunctionalInterface
    interface SnapshotOperation<T> {
        T run(SnapshotSource source) throws java.io.IOException;
    }

    CompletionStage<Void> closeShard(ShardId id, OperationContext context);
}
