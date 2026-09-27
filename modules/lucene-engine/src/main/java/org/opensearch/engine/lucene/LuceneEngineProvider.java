/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.engine.api.Checkpoint;
import org.opensearch.engine.api.EngineDescriptor;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.EngineProvider;
import org.opensearch.engine.api.Mutation;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.ReadView;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;
import org.opensearch.engine.api.SearchResult;
import org.opensearch.engine.api.ShardReader;
import org.opensearch.engine.api.ShardSpec;
import org.opensearch.engine.api.ShardWriter;
import org.opensearch.engine.api.SnapshotManifest;
import org.opensearch.engine.api.SnapshotSource;
import org.opensearch.engine.api.WriteResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.opensearch.engine.api.EngineException.Code.CLOSED;
import static org.opensearch.engine.api.EngineException.Code.DEADLINE_EXCEEDED;
import static org.opensearch.engine.api.EngineException.Code.INCOMPATIBLE;
import static org.opensearch.engine.api.EngineException.Code.INVALID_ARGUMENT;
import static org.opensearch.engine.api.EngineException.Code.IO_ERROR;
import static org.opensearch.engine.api.EngineException.Code.RESOURCE_LIMIT;
import static org.opensearch.engine.api.EngineException.Code.UNAVAILABLE;
import static org.opensearch.engine.api.EngineException.Code.WRITE_OUTCOME_UNKNOWN;

/** Lucene types and persistence semantics are confined to this provider. @opensearch.internal */
public final class LuceneEngineProvider implements EngineProvider {
    @FunctionalInterface
    interface DirectoryFactory {
        Directory open(java.nio.file.Path path) throws IOException;
    }

    private final DirectoryFactory directories;

    public LuceneEngineProvider() {
        this(FSDirectory::open);
    }

    LuceneEngineProvider(DirectoryFactory directories) {
        LuceneRuntime.initialize();
        this.directories = directories;
    }

    private static final String ID = "_engine_id";
    private static final String SOURCE = "_engine_source";

    @Override
    public EngineDescriptor descriptor() {
        return new EngineDescriptor(
            "lucene",
            EngineDescriptor.CURRENT_API_VERSION,
            Set.of(Schema.FieldType.KEYWORD, Schema.FieldType.TEXT)
        );
    }

    @Override
    public Set<String> snapshotFormats() {
        return Set.of("lucene-1");
    }

    @Override
    public ShardWriter openWriter(ShardSpec shard) {
        return new Writer(shard, directories);
    }

    @Override
    public ShardReader openReader(ShardSpec shard) {
        return new Reader(shard, directories);
    }

    private static EngineException io(String message, Exception cause) {
        return new EngineException(IO_ERROR, message, cause);
    }

    private static Map<String, String> metadata(ShardSpec spec, Checkpoint checkpoint) {
        return Map.of(
            "engine",
            "lucene",
            "format",
            "1",
            "index",
            spec.id().indexId().toString(),
            "shard",
            Integer.toString(spec.id().shard()),
            "schema",
            spec.schema().signature(),
            "history",
            checkpoint.history().toString(),
            "sequence",
            Long.toString(checkpoint.sequence())
        );
    }

    private static Checkpoint readCheckpoint(ShardSpec spec, Map<String, String> metadata) {
        if ("lucene".equals(metadata.get("engine")) == false
            || "1".equals(metadata.get("format")) == false
            || spec.id().indexId().toString().equals(metadata.get("index")) == false
            || Integer.toString(spec.id().shard()).equals(metadata.get("shard")) == false
            || spec.schema().signature().equals(metadata.get("schema")) == false) {
            throw new EngineException(INCOMPATIBLE, "shard identity, schema, or engine format does not match persisted metadata");
        }
        try {
            return new Checkpoint(spec.id(), UUID.fromString(metadata.get("history")), Long.parseLong(metadata.get("sequence")));
        } catch (RuntimeException e) {
            throw new EngineException(INCOMPATIBLE, "invalid persisted checkpoint", e);
        }
    }

    private static Checkpoint checkpoint(ShardSpec spec, IndexSearcher searcher) throws IOException {
        return readCheckpoint(spec, ((DirectoryReader) searcher.getIndexReader()).getIndexCommit().getUserData());
    }

    private static final class Writer implements ShardWriter {
        private final ShardSpec spec;
        private final Directory directory;
        private final StandardAnalyzer analyzer;
        private final IndexWriter writer;
        private final SnapshotDeletionPolicy snapshots = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        private int leases;
        private boolean resourcesClosed;
        private Checkpoint durable;
        private boolean failed;
        private boolean closed;

        Writer(ShardSpec spec, DirectoryFactory directories) {
            this.spec = spec;
            Directory openedDirectory = null;
            StandardAnalyzer openedAnalyzer = new StandardAnalyzer();
            IndexWriter openedWriter = null;
            boolean success = false;
            try {
                openedDirectory = directories.open(spec.directory());
                if (DirectoryReader.indexExists(openedDirectory) == false) {
                    for (String file : openedDirectory.listAll()) {
                        if (file.equals("engine.id") == false && file.equals("write.lock") == false) {
                            throw new EngineException(INCOMPATIBLE, "refusing to initialize a nonempty directory without a valid index");
                        }
                    }
                }
                openedWriter = new IndexWriter(
                    openedDirectory,
                    new IndexWriterConfig(openedAnalyzer).setRAMBufferSizeMB(16).setIndexDeletionPolicy(snapshots)
                );
                Map<String, String> persisted = new HashMap<>();
                openedWriter.getLiveCommitData().forEach(entry -> persisted.put(entry.getKey(), entry.getValue()));
                if (DirectoryReader.indexExists(openedDirectory)) {
                    durable = readCheckpoint(spec, persisted);
                } else {
                    durable = new Checkpoint(spec.id(), UUID.randomUUID(), 0);
                    openedWriter.setLiveCommitData(metadata(spec, durable).entrySet());
                    openedWriter.commit();
                }
                directory = openedDirectory;
                analyzer = openedAnalyzer;
                writer = openedWriter;
                success = true;
            } catch (IOException e) {
                throw io("cannot open Lucene writer", e);
            } finally {
                if (success == false) {
                    if (openedWriter != null) {
                        try {
                            openedWriter.rollback();
                        } catch (Exception ignored) { /* Preserve open failure. */ }
                    }
                    closeQuietly(openedAnalyzer);
                    closeQuietly(openedDirectory);
                }
            }
        }

        private void ensureReady() {
            if (closed) throw new EngineException(CLOSED, "writer is closed");
            if (failed) throw new EngineException(UNAVAILABLE, "writer failed; close and recover the shard before retrying");
        }

        @Override
        public synchronized WriteResult write(List<Mutation> mutations, OperationContext context) {
            ensureReady();
            context.check();
            if (mutations.isEmpty() || mutations.size() > 1024) throw new EngineException(
                RESOURCE_LIMIT,
                "batch must contain 1 to 1024 operations"
            );
            List<Mutation> batch = List.copyOf(mutations);
            long bytes = 0;
            for (Mutation mutation : batch) {
                if (mutation instanceof Mutation.Put put) {
                    bytes += put.document().estimatedBytes();
                    for (Map.Entry<String, String> field : put.document().fields().entrySet()) {
                        Schema.FieldType type = spec.schema().fields().get(field.getKey());
                        if (type == null) throw new EngineException(INVALID_ARGUMENT, "unknown field: " + field.getKey());
                        if (type == Schema.FieldType.KEYWORD && new BytesRef(field.getValue()).length > IndexWriter.MAX_TERM_LENGTH) {
                            throw new EngineException(INVALID_ARGUMENT, "keyword exceeds Lucene term length limit");
                        }
                    }
                } else bytes += 2048;
            }
            if (bytes > 4L << 20) throw new EngineException(RESOURCE_LIMIT, "batch byte limit exceeded");
            if (durable.sequence() > Long.MAX_VALUE - batch.size()) throw new EngineException(RESOURCE_LIMIT, "sequence number exhausted");
            context.check();
            boolean applying = false;
            try {
                for (Mutation mutation : batch) {
                    context.check();
                    applying = true;
                    if (mutation instanceof Mutation.Put put) {
                        EngineDocument source = put.document();
                        Document document = new Document();
                        document.add(new StringField(ID, source.id(), Field.Store.YES));
                        document.add(new StoredField(SOURCE, source.source()));
                        source.fields()
                            .forEach(
                                (name, value) -> document.add(
                                    spec.schema().fields().get(name) == Schema.FieldType.KEYWORD
                                        ? new StringField(name, value, Field.Store.YES)
                                        : new TextField(name, value, Field.Store.YES)
                                )
                            );
                        writer.updateDocument(new Term(ID, source.id()), document);
                    } else writer.deleteDocuments(new Term(ID, ((Mutation.Delete) mutation).id()));
                }
                context.check();
                Checkpoint next = new Checkpoint(spec.id(), durable.history(), durable.sequence() + batch.size());
                writer.setLiveCommitData(metadata(spec, next).entrySet());
                writer.commit();
                durable = next;
                return new WriteResult(batch.size(), next);
            } catch (Exception e) {
                if (applying == false && e instanceof EngineException engineFailure) throw engineFailure;
                failed = true;
                throw new EngineException(WRITE_OUTCOME_UNKNOWN, "write failed after application began; recover before retrying", e);
            }
        }

        @Override
        public synchronized Checkpoint checkpoint() {
            ensureReady();
            return durable;
        }

        @Override
        public synchronized SnapshotSource snapshot(OperationContext context) {
            ensureReady();
            context.check();
            if (leases >= 16) throw new EngineException(RESOURCE_LIMIT, "too many snapshot leases");
            IndexCommit commit = null;
            try {
                commit = snapshots.snapshot();
                if (commit.getFileNames().size() > SnapshotManifest.MAX_FILES) throw new EngineException(
                    RESOURCE_LIMIT,
                    "too many snapshot files"
                );
                List<SnapshotManifest.File> files = new ArrayList<>();
                long total = 0;
                byte[] buffer = new byte[65536];
                for (String name : new java.util.TreeSet<>(commit.getFileNames())) {
                    context.check();
                    long length = directory.fileLength(name);
                    if (length > SnapshotManifest.MAX_BYTES - total) throw new EngineException(
                        RESOURCE_LIMIT,
                        "snapshot exceeds byte limit"
                    );
                    total += length;
                    java.security.MessageDigest digest = sha256();
                    try (IndexInput input = directory.openInput(name, IOContext.READONCE)) {
                        long remaining = length;
                        while (remaining > 0) {
                            context.check();
                            int count = (int) Math.min(remaining, buffer.length);
                            input.readBytes(buffer, 0, count);
                            digest.update(buffer, 0, count);
                            remaining -= count;
                        }
                    }
                    files.add(new SnapshotManifest.File(name, length, java.util.HexFormat.of().formatHex(digest.digest())));
                }
                SnapshotManifest manifest = new SnapshotManifest(
                    "lucene",
                    "lucene-1",
                    spec.schema(),
                    readCheckpoint(spec, commit.getUserData()),
                    files
                );
                leases++;
                return new Lease(commit, manifest);
            } catch (Exception e) {
                if (commit != null) {
                    try {
                        snapshots.release(commit);
                    } catch (IOException suppressed) {
                        e.addSuppressed(suppressed);
                    }
                }
                if (e instanceof EngineException failure) throw failure;
                throw io("cannot export committed snapshot", e);
            }
        }

        private static java.security.MessageDigest sha256() {
            try {
                return java.security.MessageDigest.getInstance("SHA-256");
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 unavailable", e);
            }
        }

        private final class Lease implements SnapshotSource {
            private final IndexCommit commit;
            private final SnapshotManifest manifest;
            private final Set<IndexInput> inputs = new java.util.HashSet<>();
            private boolean released;

            Lease(IndexCommit commit, SnapshotManifest manifest) {
                this.commit = commit;
                this.manifest = manifest;
            }

            @Override
            public synchronized SnapshotManifest manifest() {
                if (released) throw new EngineException(CLOSED, "snapshot lease is closed");
                return manifest;
            }

            @Override
            public synchronized java.io.InputStream open(String name) throws IOException {
                if (released) throw new IOException("snapshot lease is closed");
                if (manifest.files().stream().noneMatch(file -> file.name().equals(name))) throw new IOException(
                    "file is outside snapshot"
                );
                IndexInput input = directory.openInput(name, IOContext.READONCE);
                inputs.add(input);
                return new java.io.InputStream() {
                    private boolean streamClosed;

                    @Override
                    public int read() throws IOException {
                        synchronized (Lease.this) {
                            if (streamClosed || released) throw new IOException("snapshot stream is closed");
                            return input.getFilePointer() == input.length() ? -1 : input.readByte() & 255;
                        }
                    }

                    @Override
                    public int read(byte[] bytes, int offset, int length) throws IOException {
                        java.util.Objects.checkFromIndexSize(offset, length, bytes.length);
                        synchronized (Lease.this) {
                            if (streamClosed || released) throw new IOException("snapshot stream is closed");
                            if (length == 0) return 0;
                            int count = (int) Math.min(length, input.length() - input.getFilePointer());
                            if (count == 0) return -1;
                            input.readBytes(bytes, offset, count);
                            return count;
                        }
                    }

                    @Override
                    public void close() throws IOException {
                        synchronized (Lease.this) {
                            if (streamClosed) return;
                            streamClosed = true;
                            if (inputs.remove(input)) input.close();
                        }
                    }
                };
            }

            @Override
            public synchronized void close() {
                if (released) return;
                released = true;
                RuntimeException failure = null;
                for (IndexInput input : inputs) {
                    try {
                        input.close();
                    } catch (IOException e) {
                        if (failure == null) failure = io("cannot close snapshot stream", e);
                        else failure.addSuppressed(e);
                    }
                }
                inputs.clear();
                synchronized (Writer.this) {
                    try {
                        snapshots.release(commit);
                    } catch (IOException e) {
                        if (failure == null) failure = io("cannot release snapshot", e);
                        else failure.addSuppressed(e);
                    } finally {
                        leases--;
                        if (closed && leases == 0) {
                            try {
                                closeResources();
                            } catch (RuntimeException e) {
                                if (failure == null) failure = e;
                                else failure.addSuppressed(e);
                            }
                        }
                    }
                }
                if (failure != null) throw failure;
            }
        }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            if (leases == 0) closeResources();
        }

        private void closeResources() {
            if (resourcesClosed) return;
            resourcesClosed = true;
            // Every acknowledged batch is committed already. Never commit an uncertain batch during cleanup.
            RuntimeException failure = null;
            try {
                writer.rollback();
            } catch (Exception e) {
                failure = io("cannot close Lucene writer", e);
            }
            try {
                analyzer.close();
            } catch (RuntimeException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            }
            try {
                directory.close();
            } catch (Exception e) {
                if (failure == null) failure = io("cannot close writer directory", e);
                else failure.addSuppressed(e);
            }
            if (failure != null) throw failure;
        }
    }

    private static final class Reader implements ShardReader {
        private final ShardSpec spec;
        private final Directory directory;
        private final SearcherManager manager;
        private int views;
        private boolean closed;
        private boolean directoryClosed;

        Reader(ShardSpec spec, DirectoryFactory directories) {
            this.spec = spec;
            Directory openedDirectory = null;
            SearcherManager openedManager = null;
            boolean success = false;
            try {
                if (java.nio.file.Files.isDirectory(spec.directory()) == false) throw new EngineException(
                    UNAVAILABLE,
                    "shard directory does not exist"
                );
                openedDirectory = directories.open(spec.directory());
                openedManager = new SearcherManager(openedDirectory, null);
                IndexSearcher searcher = openedManager.acquire();
                try {
                    checkpoint(spec, searcher);
                } finally {
                    openedManager.release(searcher);
                }
                directory = openedDirectory;
                manager = openedManager;
                success = true;
            } catch (IOException e) {
                throw io("cannot open committed Lucene reader", e);
            } finally {
                if (success == false) {
                    closeQuietly(openedManager);
                    closeQuietly(openedDirectory);
                }
            }
        }

        @Override
        public synchronized ReadView acquireView() {
            if (closed) throw new EngineException(CLOSED, "reader is closed");
            try {
                IndexSearcher searcher = manager.acquire();
                boolean success = false;
                try {
                    Checkpoint checkpoint = checkpoint(spec, searcher);
                    View view = new View(this, searcher, checkpoint);
                    views++;
                    success = true;
                    return view;
                } finally {
                    if (success == false) manager.release(searcher);
                }
            } catch (IOException e) {
                throw io("cannot acquire committed view", e);
            }
        }

        @Override
        public synchronized Checkpoint refresh(OperationContext context) {
            if (closed) throw new EngineException(CLOSED, "reader is closed");
            context.check();
            try {
                manager.maybeRefreshBlocking();
                try (ReadView view = acquireView()) {
                    return view.checkpoint();
                }
            } catch (IOException e) {
                throw io("cannot refresh committed reader", e);
            }
        }

        synchronized void release(IndexSearcher searcher) {
            try {
                manager.release(searcher);
            } catch (IOException e) {
                throw io("cannot release committed view", e);
            } finally {
                views--;
                closeDirectoryIfUnused();
            }
        }

        private void closeDirectoryIfUnused() {
            if (closed && views == 0 && directoryClosed == false) {
                directoryClosed = true;
                try {
                    directory.close();
                } catch (IOException e) {
                    throw io("cannot close reader directory", e);
                }
            }
        }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            try {
                manager.close();
            } catch (IOException e) {
                throw io("cannot close reader", e);
            } finally {
                closeDirectoryIfUnused();
            }
        }
    }

    private static final class View implements ReadView {
        private final Reader owner;
        private final IndexSearcher acquired;
        private final Checkpoint checkpoint;
        private boolean closed;

        View(Reader owner, IndexSearcher acquired, Checkpoint checkpoint) {
            this.owner = owner;
            this.acquired = acquired;
            this.checkpoint = checkpoint;
        }

        private void ensureOpen() {
            if (closed) throw new EngineException(CLOSED, "read view is closed");
        }

        @Override
        public synchronized SearchResult search(SearchQuery query, int limit, OperationContext context) {
            ensureOpen();
            context.check();
            if (limit < 1 || limit > 1000) throw new EngineException(RESOURCE_LIMIT, "search limit must be 1 to 1000");
            SearchQuery.estimatedBytes(query);
            try (StandardAnalyzer analyzer = new StandardAnalyzer()) {
                IndexSearcher searcher = new IndexSearcher(acquired.getIndexReader());
                searcher.setTimeout(() -> context.cancelled() || context.expired());
                TopDocs docs = searcher.search(LuceneQueries.compile(query, owner.spec.schema(), analyzer), limit);
                context.check();
                if (searcher.timedOut()) throw new EngineException(DEADLINE_EXCEEDED, "search deadline exceeded");
                List<SearchResult.Hit> hits = new ArrayList<>();
                long bytes = 0;
                for (ScoreDoc hit : docs.scoreDocs) {
                    context.check();
                    EngineDocument document = readDocument(searcher, hit.doc);
                    bytes += document.estimatedBytes();
                    if (bytes > 8L << 20) throw new EngineException(RESOURCE_LIMIT, "search response exceeds byte limit");
                    hits.add(new SearchResult.Hit(document, hit.score));
                }
                return new SearchResult(hits, docs.totalHits.value(), docs.totalHits.relation() == TotalHits.Relation.EQUAL_TO, checkpoint);
            } catch (IOException e) {
                throw io("Lucene search failed", e);
            }
        }

        @Override
        public synchronized Optional<EngineDocument> get(String id, OperationContext context) {
            ensureOpen();
            EngineDocument.validateId(id);
            context.check();
            try {
                TopDocs hits = acquired.search(new TermQuery(new Term(ID, id)), 1);
                context.check();
                return hits.scoreDocs.length == 0 ? Optional.empty() : Optional.of(readDocument(acquired, hits.scoreDocs[0].doc));
            } catch (IOException e) {
                throw io("Lucene document retrieval failed", e);
            }
        }

        private EngineDocument readDocument(IndexSearcher searcher, int id) throws IOException {
            Document stored = searcher.storedFields().document(id);
            Map<String, String> fields = new HashMap<>();
            for (String field : owner.spec.schema().fields().keySet()) {
                String value = stored.get(field);
                if (value != null) fields.put(field, value);
            }
            BytesRef source = stored.getBinaryValue(SOURCE);
            return new EngineDocument(
                stored.get(ID),
                fields,
                Arrays.copyOfRange(source.bytes, source.offset, source.offset + source.length)
            );
        }

        @Override
        public synchronized Checkpoint checkpoint() {
            ensureOpen();
            return checkpoint;
        }

        @Override
        public synchronized void close() {
            if (closed == false) {
                closed = true;
                owner.release(acquired);
            }
        }
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ignored) { /* Preserve the original failure. */ }
        }
    }
}
