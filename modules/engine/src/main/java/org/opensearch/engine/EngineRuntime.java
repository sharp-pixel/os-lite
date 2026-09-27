/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.engine.api.Checkpoint;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.EngineProvider;
import org.opensearch.engine.api.EngineService;
import org.opensearch.engine.api.Mutation;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.ReadView;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;
import org.opensearch.engine.api.SearchResult;
import org.opensearch.engine.api.ShardId;
import org.opensearch.engine.api.ShardReader;
import org.opensearch.engine.api.ShardSpec;
import org.opensearch.engine.api.ShardWriter;
import org.opensearch.engine.api.SnapshotManifest;
import org.opensearch.engine.api.SnapshotSource;
import org.opensearch.engine.api.WriteResult;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static org.opensearch.engine.api.EngineException.Code.CLOSED;
import static org.opensearch.engine.api.EngineException.Code.CONFLICT;
import static org.opensearch.engine.api.EngineException.Code.INCOMPATIBLE;
import static org.opensearch.engine.api.EngineException.Code.IO_ERROR;
import static org.opensearch.engine.api.EngineException.Code.RESOURCE_LIMIT;
import static org.opensearch.engine.api.EngineException.Code.UNAVAILABLE;
import static org.opensearch.engine.api.EngineException.Code.UNSUPPORTED;

/** Bounded execution and exactly one owner for every opened shard handle. @opensearch.internal */
public final class EngineRuntime extends AbstractLifecycleComponent implements EngineService {
    private final Map<String, EngineProvider> providers;
    private final Path root;
    private final Settings settings;
    private final Map<ShardId, ManagedShard> shards = new HashMap<>();
    private final Object snapshotInstallLock = new Object();
    private final AtomicLong pendingBytes = new AtomicLong();
    private final long byteLimit;
    private final Set<String> roles;
    private volatile ThreadPoolExecutor executor;
    private volatile boolean accepting;
    private volatile IOException closeFailure;

    public EngineRuntime(Map<String, EngineProvider> providers, Path root, Settings settings) {
        this.providers = Map.copyOf(providers);
        this.root = root.toAbsolutePath().normalize();
        this.settings = settings;
        byteLimit = EnginePlugin.PENDING_BYTES.get(settings);
        roles = Set.copyOf(EnginePlugin.ROLES.get(settings));
    }

    @Override
    public Set<String> providers() {
        return providers.keySet();
    }

    @Override
    public boolean supportsMode(Mode mode) {
        Objects.requireNonNull(mode);
        return (mode == Mode.READ_ONLY || roles.contains("writer")) && (mode == Mode.WRITE_ONLY || roles.contains("reader"));
    }

    @Override
    protected synchronized void doStart() {
        if (executor != null) throw new IllegalStateException("engine runtime cannot be restarted");
        int workers = EnginePlugin.WORKERS.get(settings);
        executor = new EngineExecutor(workers);
        accepting = true;
    }

    private final class EngineExecutor extends ThreadPoolExecutor {
        @SuppressForbidden(reason = "Only executes EngineTask, which rethrows Errors; termination owns shard cleanup")
        EngineExecutor(int workers) {
            super(
                workers,
                workers,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(EnginePlugin.QUEUE.get(settings)),
                OpenSearchExecutors.daemonThreadFactory(settings, "engine")
            );
        }

        @Override
        protected void terminated() {
            // shutdown can interrupt the last worker. Cleanup must still be able to perform blocking I/O.
            boolean interrupted = Thread.interrupted();
            try {
                closeAllShards();
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    protected void doStop() {
        accepting = false;
        if (executor != null) executor.shutdown();
    }

    @Override
    protected void doClose() throws IOException {
        doStop();
        ThreadPoolExecutor pool = executor;
        if (pool != null) {
            try {
                if (pool.awaitTermination(30, TimeUnit.SECONDS) == false) {
                    for (Runnable queued : pool.shutdownNow())
                        ((EngineTask<?>) queued).reject(new EngineException(CLOSED, "engine runtime closed"));
                    if (pool.awaitTermination(1, TimeUnit.SECONDS) == false) {
                        throw new IOException("engine workers have not drained; shard cleanup will run when they terminate");
                    }
                }
            } catch (InterruptedException e) {
                for (Runnable queued : pool.shutdownNow())
                    ((EngineTask<?>) queued).reject(new EngineException(CLOSED, "engine runtime interrupted"));
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while closing engine runtime", e);
            }
        }
        if (closeFailure != null) throw closeFailure;
    }

    @Override
    public CompletionStage<Void> openShard(String provider, ShardId id, Schema schema, Mode mode, OperationContext context) {
        Objects.requireNonNull(mode);
        Objects.requireNonNull(id);
        Objects.requireNonNull(schema);
        return submit(1024, context, () -> {
            try {
                open(provider, id, schema, mode);
            } catch (IOException e) {
                throw new EngineException(IO_ERROR, "cannot open shard storage", e);
            }
            return null;
        });
    }

    private synchronized void open(String providerId, ShardId id, Schema schema, Mode mode) throws IOException {
        open(providerId, id, schema, mode, null);
    }

    private synchronized void open(String providerId, ShardId id, Schema schema, Mode mode, Path privateDirectory) throws IOException {
        if (shards.containsKey(id)) throw new EngineException(CONFLICT, "shard already open: " + id);
        if (shards.size() >= EnginePlugin.SHARDS.get(settings)) throw new EngineException(RESOURCE_LIMIT, "too many open shards");
        EngineProvider provider = providers.get(providerId);
        if (provider == null) throw new EngineException(UNSUPPORTED, "engine provider not installed: " + providerId);
        if (provider.descriptor().fieldTypes().containsAll(schema.fields().values()) == false) throw new EngineException(
            UNSUPPORTED,
            "unsupported schema"
        );
        boolean writerMode = mode != Mode.READ_ONLY;
        boolean readerMode = mode != Mode.WRITE_ONLY;
        if (supportsMode(mode) == false) {
            throw new EngineException(UNSUPPORTED, "requested shard execution role is disabled on this node");
        }
        Path directory = privateDirectory == null
            ? root.resolve(id.indexId().toString()).resolve(Integer.toString(id.shard()))
            : privateDirectory;
        Path marker = directory.resolve("engine.id");
        if (writerMode) {
            createDirectoriesDurably(directory);
            try (FileChannel file = FileChannel.open(marker, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                Channels.newOutputStream(file).write(providerId.getBytes(StandardCharsets.UTF_8));
                file.force(true);
            } catch (FileAlreadyExistsException ignored) {
                // Never overwrite a persisted provider choice.
            }
            syncDirectory(directory);
        }
        if (Files.exists(marker) == false) throw new EngineException(UNAVAILABLE, "shard has not been created");
        if (Files.readString(marker, StandardCharsets.UTF_8).equals(providerId) == false) throw new EngineException(
            INCOMPATIBLE,
            "persisted engine provider differs"
        );
        ShardSpec spec = new ShardSpec(id, directory, schema);
        ShardWriter writer = writerMode ? provider.openWriter(spec) : null;
        ShardReader reader = null;
        boolean success = false;
        try {
            reader = readerMode ? provider.openReader(spec) : null;
            shards.put(id, new ManagedShard(writer, reader, privateDirectory));
            success = true;
        } finally {
            if (success == false) {
                closeQuietly(reader);
                closeQuietly(writer);
            }
        }
    }

    @Override
    public CompletionStage<WriteResult> write(ShardId id, List<Mutation> mutations, OperationContext context) {
        if (mutations.isEmpty() || mutations.size() > 1024) return CompletableFuture.failedFuture(
            new EngineException(RESOURCE_LIMIT, "batch must contain 1 to 1024 operations")
        );
        List<Mutation> owned = List.copyOf(mutations);
        long bytes = 0;
        for (Mutation mutation : owned)
            bytes += mutation instanceof Mutation.Put put ? put.document().estimatedBytes() : 2048;
        if (owned.isEmpty() || owned.size() > 1024 || bytes > 4L << 20) return CompletableFuture.failedFuture(
            new EngineException(RESOURCE_LIMIT, "batch exceeds limits")
        );
        return submit(bytes, context, () -> use(id, shard -> {
            if (shard.writer == null) throw new EngineException(UNSUPPORTED, "shard is read-only");
            return shard.writer.write(owned, context);
        }));
    }

    @Override
    public CompletionStage<Checkpoint> refresh(ShardId id, OperationContext context) {
        return submit(1024, context, () -> use(id, shard -> shard.reader().refresh(context)));
    }

    @Override
    public CompletionStage<SearchResult> search(ShardId id, SearchQuery query, int limit, OperationContext context) {
        return submit(SearchQuery.estimatedBytes(query), context, () -> use(id, shard -> {
            try (ReadView view = shard.reader().acquireView()) {
                return view.search(query, limit, context);
            }
        }));
    }

    @Override
    public CompletionStage<Optional<EngineDocument>> get(ShardId id, String documentId, OperationContext context) {
        EngineDocument.validateId(documentId);
        return submit(2048, context, () -> use(id, shard -> {
            try (ReadView view = shard.reader().acquireView()) {
                return view.get(documentId, context);
            }
        }));
    }

    @Override
    public CompletionStage<Void> createSnapshotWriter(String provider, ShardId id, Schema schema, OperationContext context) {
        return submit(1024, context, () -> {
            synchronized (snapshotInstallLock) {
                EngineProvider factory = providers.get(provider);
                if (factory == null || factory.snapshotFormats().isEmpty()) throw new EngineException(
                    UNSUPPORTED,
                    "provider does not support snapshot publication"
                );
                Path directory = root.resolve(".snapshots")
                    .resolve(id.indexId().toString())
                    .resolve(Integer.toString(id.shard()))
                    .resolve(java.util.UUID.randomUUID().toString());
                boolean success = false;
                try {
                    open(provider, id, schema, Mode.WRITE_ONLY, directory);
                    success = true;
                    return null;
                } finally {
                    if (success == false) SnapshotStorage.delete(directory);
                }
            }
        });
    }

    @Override
    public <T> CompletionStage<T> withSnapshot(ShardId id, SnapshotOperation<T> transfer, OperationContext context) {
        return submit(65536, context, () -> use(id, shard -> {
            if (shard.writer == null) throw new EngineException(UNSUPPORTED, "snapshot export requires a writer");
            try (SnapshotSource source = shard.writer.snapshot(context)) {
                return transfer.run(source);
            } catch (IOException e) {
                throw new EngineException(IO_ERROR, "snapshot transfer failed", e);
            }
        }));
    }

    @Override
    public CompletionStage<Checkpoint> installSnapshot(
        String providerId,
        ShardId id,
        Schema schema,
        Mode mode,
        java.util.function.Supplier<SnapshotSource> source,
        OperationContext context
    ) {
        Objects.requireNonNull(source);
        return submit(65536, context, () -> {
            synchronized (snapshotInstallLock) {
                context.check();
                if (mode == Mode.READ_WRITE || supportsMode(mode) == false) throw new EngineException(
                    UNSUPPORTED,
                    "snapshot installation requires one enabled execution role"
                );
                EngineProvider provider = providers.get(providerId);
                if (provider == null) throw new EngineException(UNSUPPORTED, "snapshot provider is not installed");
                ManagedShard previous;
                synchronized (this) {
                    previous = shards.get(id);
                    if (previous != null && (previous.writer != null || previous.snapshotDirectory == null || mode != Mode.READ_ONLY)) {
                        throw new EngineException(CONFLICT, "only snapshot readers can be refreshed in place");
                    }
                    if (previous == null && shards.size() >= EnginePlugin.SHARDS.get(settings)) {
                        throw new EngineException(RESOURCE_LIMIT, "too many open shards");
                    }
                }
                Path parent = root.resolve(".snapshots").resolve(id.indexId().toString()).resolve(Integer.toString(id.shard()));
                Files.createDirectories(parent);
                // The repository profile holds a node-storage lock before using this private cache.
                try (var children = Files.newDirectoryStream(parent)) {
                    for (Path child : children) {
                        if (previous == null || child.equals(previous.snapshotDirectory) == false) SnapshotStorage.delete(child);
                    }
                }
                Path directory = parent.resolve(java.util.UUID.randomUUID().toString());
                ShardWriter writer = null;
                ShardReader reader = null;
                boolean installed = false;
                try (SnapshotSource snapshot = source.get()) {
                    SnapshotManifest manifest = snapshot.manifest();
                    if (manifest.provider().equals(providerId) == false
                        || manifest.schema().equals(schema) == false
                        || manifest.checkpoint().shard().equals(id) == false
                        || provider.snapshotFormats().contains(manifest.format()) == false) {
                        throw new EngineException(INCOMPATIBLE, "snapshot provider, format, schema or shard differs");
                    }
                    SnapshotStorage.copy(snapshot, directory, context);
                    context.check();
                    ShardSpec spec = new ShardSpec(id, directory, schema);
                    Checkpoint actual;
                    if (mode == Mode.WRITE_ONLY) {
                        writer = provider.openWriter(spec);
                        actual = writer.checkpoint();
                    } else {
                        reader = provider.openReader(spec);
                        try (ReadView view = reader.acquireView()) {
                            actual = view.checkpoint();
                        }
                    }
                    if (actual.equals(manifest.checkpoint()) == false) throw new EngineException(
                        INCOMPATIBLE,
                        "snapshot checkpoint differs"
                    );
                    context.check();
                    synchronized (this) {
                        if (shards.get(id) != previous) throw new EngineException(CONFLICT, "shard changed during snapshot installation");
                        if (previous == null) {
                            if (shards.size() >= EnginePlugin.SHARDS.get(settings)) throw new EngineException(
                                RESOURCE_LIMIT,
                                "too many open shards"
                            );
                            shards.put(id, new ManagedShard(writer, reader, directory));
                        } else {
                            previous.replaceReader(reader, directory);
                        }
                        installed = true;
                    }
                    return actual;
                } finally {
                    if (installed == false) {
                        closeQuietly(reader);
                        closeQuietly(writer);
                        SnapshotStorage.delete(directory);
                    }
                }
            }
        });
    }

    @Override
    public CompletionStage<Void> closeShard(ShardId id, OperationContext context) {
        return submit(1024, context, () -> {
            synchronized (this) {
                ManagedShard shard = shards.get(id);
                if (shard != null) {
                    try {
                        shard.close();
                    } finally {
                        shards.remove(id);
                    }
                }
            }
            return null;
        });
    }

    private <T> T use(ShardId id, Function<ManagedShard, T> operation) {
        ManagedShard shard;
        synchronized (this) {
            shard = shards.get(id);
        }
        if (shard == null) throw new EngineException(UNAVAILABLE, "shard is not open: " + id);
        shard.lock.readLock().lock();
        try {
            if (shard.closed) throw new EngineException(CLOSED, "shard is closed");
            return operation.apply(shard);
        } finally {
            shard.lock.readLock().unlock();
        }
    }

    private <T> CompletionStage<T> submit(long bytes, OperationContext context, Callable<T> operation) {
        Objects.requireNonNull(context);
        if (accepting == false) return CompletableFuture.failedFuture(new EngineException(CLOSED, "engine runtime is not started"));
        if (pendingBytes.addAndGet(bytes) > byteLimit) {
            pendingBytes.addAndGet(-bytes);
            return CompletableFuture.failedFuture(new EngineException(RESOURCE_LIMIT, "engine pending-byte budget exceeded"));
        }
        EngineTask<T> task = new EngineTask<>(bytes, context, operation);
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            task.reject(new EngineException(RESOURCE_LIMIT, "engine worker queue is full or closed", e));
        }
        return task.future;
    }

    private final class EngineTask<T> implements Runnable {
        private final long bytes;
        private final OperationContext context;
        private final Callable<T> operation;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        EngineTask(long bytes, OperationContext context, Callable<T> operation) {
            this.bytes = bytes;
            this.context = context;
            this.operation = operation;
            future.whenComplete((value, error) -> { if (future.isCancelled()) context.cancel(); });
        }

        @Override
        public void run() {
            try {
                context.check();
                future.complete(operation.call());
            } catch (Throwable e) {
                future.completeExceptionally(e);
                if (e instanceof Error error) throw error;
            } finally {
                pendingBytes.addAndGet(-bytes);
            }
        }

        void reject(EngineException error) {
            pendingBytes.addAndGet(-bytes);
            future.completeExceptionally(error);
        }
    }

    private synchronized void closeAllShards() {
        for (ManagedShard shard : shards.values()) {
            try {
                shard.close();
            } catch (Exception e) {
                if (closeFailure == null) closeFailure = new IOException("failed to close shard resources", e);
                else closeFailure.addSuppressed(e);
            }
        }
        shards.clear();
    }

    private final class ManagedShard {
        final ShardWriter writer;
        ShardReader reader;
        Path snapshotDirectory;
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        boolean closed;

        ManagedShard(ShardWriter writer, ShardReader reader) {
            this(writer, reader, null);
        }

        ManagedShard(ShardWriter writer, ShardReader reader, Path snapshotDirectory) {
            this.writer = writer;
            this.reader = reader;
            this.snapshotDirectory = snapshotDirectory;
        }

        void replaceReader(ShardReader replacement, Path directory) {
            lock.writeLock().lock();
            try {
                if (closed) throw new EngineException(CLOSED, "shard is closed");
                ShardReader previous = reader;
                Path previousDirectory = snapshotDirectory;
                reader = replacement;
                snapshotDirectory = directory;
                try {
                    previous.close();
                    SnapshotStorage.delete(previousDirectory);
                } catch (Exception e) {
                    closeFailure = new IOException("old snapshot cleanup failed", e);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        ShardReader reader() {
            if (reader == null) throw new EngineException(UNSUPPORTED, "shard is write-only");
            return reader;
        }

        void close() {
            lock.writeLock().lock();
            try {
                if (closed) return;
                closed = true;
                RuntimeException failure = null;
                try {
                    if (reader != null) reader.close();
                } catch (RuntimeException e) {
                    failure = e;
                }
                try {
                    if (writer != null) writer.close();
                } catch (RuntimeException e) {
                    if (failure == null) failure = e;
                    else failure.addSuppressed(e);
                }
                if (failure != null) throw failure;
                try {
                    SnapshotStorage.delete(snapshotDirectory);
                } catch (IOException e) {
                    throw new EngineException(IO_ERROR, "cannot remove private snapshot copy", e);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ignored) { /* Keep the opening failure. */ }
        }
    }

    private static void createDirectoriesDurably(Path directory) throws IOException {
        List<Path> missing = new ArrayList<>();
        for (Path path = directory; Files.exists(path) == false; path = path.getParent())
            missing.add(path);
        for (int i = missing.size() - 1; i >= 0; i--) {
            Path path = missing.get(i);
            try {
                Files.createDirectory(path);
            } catch (FileAlreadyExistsException ignored) {
                if (Files.isDirectory(path) == false) throw ignored;
            }
            syncDirectory(path.getParent());
        }
    }

    private static void syncDirectory(Path path) throws IOException {
        try (FileChannel directory = FileChannel.open(path, StandardOpenOption.READ)) {
            directory.force(true);
        }
    }
}
