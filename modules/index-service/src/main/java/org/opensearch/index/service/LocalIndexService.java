/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.service;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.EngineService;
import org.opensearch.engine.api.Mutation;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.env.Environment;
import org.opensearch.index.api.IndexMetadata;
import org.opensearch.index.api.IndexRequest;
import org.opensearch.index.api.IndexResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** One local shard per index, with an independent durable name/schema catalog. @opensearch.internal */
public final class LocalIndexService extends AbstractLifecycleComponent {
    private final EngineService engine;
    private final Settings settings;
    private final IndexCatalog catalog;
    private final PublishedIndices published;
    private final Map<String, Entry> indices = new ConcurrentHashMap<>();
    private volatile ThreadPoolExecutor catalogExecutor;
    private volatile boolean accepting;
    private volatile IOException closeFailure;
    private volatile boolean catalogFailed;

    @Inject
    public LocalIndexService(EngineService engine, Environment environment, RepositoryRegistry registry) {
        this(engine, environment, environment.settings(), registry);
    }

    public LocalIndexService(EngineService engine, Environment environment) {
        this(engine, environment, environment.settings());
    }

    public LocalIndexService(EngineService engine, Environment environment, Settings settings) {
        this(engine, environment, settings, new RepositoryRegistry(Map.of()));
    }

    public LocalIndexService(EngineService engine, Environment environment, Settings settings, RepositoryRegistry registry) {
        this.engine = engine;
        this.settings = settings;
        this.catalog = new IndexCatalog(environment.dataFiles()[0].resolve("index-catalog"));
        this.published = IndexServicePlugin.REPOSITORY.get(settings).isEmpty()
            ? null
            : new PublishedIndices(engine, environment, settings, registry);
    }

    LocalIndexService(EngineService engine, Settings settings, IndexCatalog catalog) {
        this.engine = engine;
        this.settings = settings;
        this.catalog = catalog;
        this.published = null;
    }

    @Override
    protected void doStart() {
        try {
            if (published != null) published.start();
            else catalog.load(IndexServicePlugin.MAX_INDICES.get(settings), engine.supportsMode(EngineService.Mode.WRITE_ONLY))
                .forEach((name, metadata) -> indices.put(name, new Entry(metadata)));
            catalogExecutor = new CatalogExecutor();
            accepting = true;
        } catch (Exception e) {
            try {
                if (published != null) published.close();
                catalog.close();
            } catch (IOException close) {
                e.addSuppressed(close);
            }
            throw new IllegalStateException("cannot load index catalog", e);
        }
    }

    private final class CatalogExecutor extends ThreadPoolExecutor {
        @SuppressForbidden(reason = "Only executes CatalogTask, which rethrows Errors; termination owns the catalog lock")
        CatalogExecutor() {
            super(
                1,
                1,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(64),
                OpenSearchExecutors.daemonThreadFactory(settings, "index-catalog")
            );
        }

        @Override
        protected void terminated() {
            boolean interrupted = Thread.interrupted();
            try {
                if (published != null) published.close();
                catalog.close();
            } catch (IOException e) {
                closeFailure = e;
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    protected void doStop() {
        accepting = false;
        if (catalogExecutor != null) catalogExecutor.shutdown();
    }

    @Override
    protected void doClose() throws IOException {
        doStop();
        ThreadPoolExecutor pool = catalogExecutor;
        if (pool == null) {
            if (published != null) published.close();
            catalog.close();
            return;
        }
        try {
            if (pool.awaitTermination(30, TimeUnit.SECONDS) == false) {
                for (Runnable queued : pool.shutdownNow())
                    ((CatalogTask<?>) queued).reject();
                if (pool.awaitTermination(1, TimeUnit.SECONDS) == false) throw new IOException(
                    "catalog worker still active; cleanup will run on termination"
                );
            }
        } catch (InterruptedException e) {
            for (Runnable queued : pool.shutdownNow())
                ((CatalogTask<?>) queued).reject();
            Thread.currentThread().interrupt();
            throw new IOException("interrupted closing index service", e);
        }
        if (closeFailure != null) throw closeFailure;
    }

    public CompletionStage<IndexResponse.Metadata> create(IndexRequest.Create request, OperationContext context) {
        if (published != null) return submit(context, () -> published.create(request, context));
        return submit(context, () -> {
            if (catalogFailed) throw new EngineException(
                EngineException.Code.UNAVAILABLE,
                "catalog publication failed; restart to reconcile durable metadata"
            );
            if (indices.containsKey(request.index())) throw new OpenSearchStatusException(
                "index already exists [" + request.index() + "]",
                RestStatus.CONFLICT
            );
            if (indices.size() >= IndexServicePlugin.MAX_INDICES.get(settings)) throw new EngineException(
                EngineException.Code.RESOURCE_LIMIT,
                "index limit reached"
            );
            if (engine.supportsMode(EngineService.Mode.WRITE_ONLY) == false) throw new EngineException(
                EngineException.Code.UNSUPPORTED,
                "index creation requires a local writer"
            );
            if (engine.providers().contains(request.engine()) == false) throw new EngineException(
                EngineException.Code.UNSUPPORTED,
                "engine provider not installed: " + request.engine()
            );
            IndexMetadata metadata = new IndexMetadata(request.index(), UUID.randomUUID(), request.engine(), request.schema());
            Entry entry = new Entry(metadata);
            engine.openShard(metadata.engine(), metadata.shard(), metadata.schema(), mode(), context).toCompletableFuture().get();
            try {
                // Once publication begins, complete it even if the caller cancels: cancellation cannot imply rollback.
                catalog.save(metadata);
                entry.opened = CompletableFuture.completedFuture(null);
                indices.put(metadata.name(), entry);
                return new IndexResponse.Metadata(metadata);
            } catch (Exception e) {
                catalogFailed = true;
                try {
                    engine.closeShard(metadata.shard(), OperationContext.standard()).toCompletableFuture().get();
                } catch (Exception close) {
                    e.addSuppressed(close);
                }
                throw new OpenSearchStatusException(
                    "index creation outcome is unknown; restart and inspect catalog before retrying",
                    RestStatus.INTERNAL_SERVER_ERROR
                );
            }
        });
    }

    public CompletionStage<IndexResponse.Metadata> describe(IndexRequest.Describe request, OperationContext context) {
        if (published != null) return submit(context, () -> published.describe(request, context));
        try {
            context.check();
            return CompletableFuture.completedFuture(new IndexResponse.Metadata(entry(request.index()).metadata));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletionStage<IndexResponse.Mutation> put(IndexRequest.Put request, OperationContext context) {
        if (published != null) return submit(context, () -> published.put(request, context));
        return use(
            request.index(),
            context,
            entry -> engine.write(entry.metadata.shard(), List.of(new Mutation.Put(request.document())), context)
                .thenApply(result -> new IndexResponse.Mutation(request.index(), request.document().id(), result.checkpoint()))
        );
    }

    public CompletionStage<IndexResponse.Mutation> delete(IndexRequest.Delete request, OperationContext context) {
        if (published != null) return submit(context, () -> published.delete(request, context));
        return use(
            request.index(),
            context,
            entry -> engine.write(entry.metadata.shard(), List.of(new Mutation.Delete(request.id())), context)
                .thenApply(result -> new IndexResponse.Mutation(request.index(), request.id(), result.checkpoint()))
        );
    }

    public CompletionStage<IndexResponse.Refreshed> refresh(IndexRequest.Refresh request, OperationContext context) {
        if (published != null) return submit(context, () -> published.refresh(request, context));
        return use(
            request.index(),
            context,
            entry -> engine.refresh(entry.metadata.shard(), context)
                .thenApply(checkpoint -> new IndexResponse.Refreshed(request.index(), checkpoint))
        );
    }

    public CompletionStage<IndexResponse.Document> get(IndexRequest.Get request, OperationContext context) {
        if (published != null) return submit(context, () -> published.get(request, context));
        return use(
            request.index(),
            context,
            entry -> engine.get(entry.metadata.shard(), request.id(), context)
                .thenApply(document -> new IndexResponse.Document(request.index(), request.id(), document))
        );
    }

    public CompletionStage<IndexResponse.Search> search(IndexRequest.Search request, OperationContext context) {
        if (published != null) return submit(context, () -> published.search(request, context));
        if (request.minimum() != null) return submit(context, () -> {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(request.waitMillis());
            while (true) {
                context.check();
                var checkpoint = await(use(request.index(), context, entry -> engine.refresh(entry.metadata.shard(), context)));
                if (checkpoint.covers(request.minimum())) break;
                if (request.waitMillis() == 0 || System.nanoTime() - deadline >= 0) throw new EngineException(
                    EngineException.Code.DEADLINE_EXCEEDED,
                    "minimum checkpoint is not committed before the wait deadline"
                );
                java.util.concurrent.locks.LockSupport.parkNanos(Math.min(50_000_000L, Math.max(1, deadline - System.nanoTime())));
            }
            return await(searchCurrent(request, context));
        });
        return searchCurrent(request, context);
    }

    private CompletionStage<IndexResponse.Search> searchCurrent(IndexRequest.Search request, OperationContext context) {
        return use(
            request.index(),
            context,
            entry -> engine.search(entry.metadata.shard(), request.query(), request.limit(), context)
                .thenApply(result -> new IndexResponse.Search(request.index(), result))
        );
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        try {
            return stage.toCompletableFuture().get();
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof Exception cause) throw cause;
            if (e.getCause() instanceof Error cause) throw cause;
            throw e;
        }
    }

    public CompletionStage<IndexResponse.Metadata> claim(IndexRequest.Claim request, OperationContext context) {
        if (published == null) return CompletableFuture.failedFuture(
            new EngineException(EngineException.Code.UNSUPPORTED, "writer assignment requires a repository")
        );
        return submit(context, () -> published.claim(request, context));
    }

    private Entry entry(String name) {
        if (accepting == false) throw new EngineException(EngineException.Code.CLOSED, "index service is not started");
        Entry entry = indices.get(name);
        if (entry == null) throw new OpenSearchStatusException("index not found [" + name + "]", RestStatus.NOT_FOUND);
        return entry;
    }

    private EngineService.Mode mode() {
        if (engine.supportsMode(EngineService.Mode.READ_WRITE)) return EngineService.Mode.READ_WRITE;
        if (engine.supportsMode(EngineService.Mode.WRITE_ONLY)) return EngineService.Mode.WRITE_ONLY;
        return EngineService.Mode.READ_ONLY;
    }

    private <T> CompletionStage<T> use(String name, OperationContext context, Function<Entry, CompletionStage<T>> operation) {
        try {
            context.check();
            Entry entry = entry(name);
            CompletableFuture<Void> opened;
            synchronized (entry) {
                if (entry.opened == null || entry.opened.isCompletedExceptionally()) {
                    entry.opened = engine.openShard(
                        entry.metadata.engine(),
                        entry.metadata.shard(),
                        entry.metadata.schema(),
                        mode(),
                        OperationContext.standard()
                    ).toCompletableFuture();
                }
                opened = entry.opened;
            }
            return opened.thenCompose(ignored -> {
                context.check();
                return operation.apply(entry);
            });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static final class Entry {
        final IndexMetadata metadata;
        CompletableFuture<Void> opened;

        Entry(IndexMetadata metadata) {
            this.metadata = metadata;
        }
    }

    private <T> CompletionStage<T> submit(OperationContext context, Callable<T> work) {
        if (accepting == false) return CompletableFuture.failedFuture(
            new EngineException(EngineException.Code.CLOSED, "index service is not started")
        );
        CatalogTask<T> task = new CatalogTask<>(context, work);
        try {
            catalogExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            task.reject();
        }
        return task.future;
    }

    private static final class CatalogTask<T> implements Runnable {
        final OperationContext context;
        final Callable<T> work;
        final CompletableFuture<T> future = new CompletableFuture<>();

        CatalogTask(OperationContext context, Callable<T> work) {
            this.context = context;
            this.work = work;
            future.whenComplete((value, error) -> { if (future.isCancelled()) context.cancel(); });
        }

        @Override
        public void run() {
            try {
                context.check();
                future.complete(work.call());
            } catch (Throwable e) {
                future.completeExceptionally(e);
                if (e instanceof Error error) throw error;
            }
        }

        void reject() {
            future.completeExceptionally(new EngineException(EngineException.Code.RESOURCE_LIMIT, "catalog queue is full or closed"));
        }
    }
}
