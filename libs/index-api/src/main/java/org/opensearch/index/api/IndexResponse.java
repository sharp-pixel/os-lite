/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.api;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.engine.api.Checkpoint;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.SearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Owned action responses with bounded binary encoding and common JSON rendering. @opensearch.experimental */
public abstract class IndexResponse extends ActionResponse implements ToXContentObject {
    private final String index;

    protected IndexResponse(String index) {
        IndexMetadata.validateName(index);
        this.index = index;
    }

    protected IndexResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readByte() != 2) throw new IOException("unsupported response wire version");
        index = IndexWire.string(in, 128);
    }

    public final String index() {
        return index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) 2);
        IndexWire.string(out, index);
    }

    private static void checkpoint(XContentBuilder builder, Checkpoint checkpoint) throws IOException {
        builder.startObject("checkpoint")
            .field("index_uuid", checkpoint.shard().indexId().toString())
            .field("shard", checkpoint.shard().shard())
            .field("history", checkpoint.history().toString())
            .field("sequence", checkpoint.sequence())
            .endObject();
    }

    public static final class Metadata extends IndexResponse {
        private final IndexMetadata metadata;
        private final long writerEpoch;
        private final Checkpoint published;

        public Metadata(IndexMetadata metadata) {
            this(metadata, 0, null);
        }

        public Metadata(IndexMetadata metadata, long writerEpoch, Checkpoint published) {
            super(metadata.name());
            this.metadata = metadata;
            if (writerEpoch < 0
                || (writerEpoch == 0) != (published == null)
                || (published != null && published.shard().equals(metadata.shard()) == false)) throw new IllegalArgumentException(
                    "invalid publication metadata"
                );
            this.writerEpoch = writerEpoch;
            this.published = published;
        }

        public long writerEpoch() {
            return writerEpoch;
        }

        public Checkpoint published() {
            return published;
        }

        public Metadata(StreamInput in) throws IOException {
            super(in);
            metadata = IndexWire.metadata(in);
            if (metadata.name().equals(index()) == false) throw new IOException("index identity mismatch");
            writerEpoch = in.readVLong();
            published = in.readBoolean() ? IndexWire.checkpoint(in) : null;
            if ((writerEpoch == 0) != (published == null) || (published != null && published.shard().equals(metadata.shard()) == false)) {
                throw new IOException("invalid publication metadata");
            }
        }

        public IndexMetadata metadata() {
            return metadata;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            IndexWire.metadata(out, metadata);
            out.writeVLong(writerEpoch);
            out.writeBoolean(published != null);
            if (published != null) IndexWire.checkpoint(out, published);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject()
                .field("acknowledged", true)
                .field("index", index())
                .field("uuid", metadata.id().toString())
                .field("engine", metadata.engine())
                .field("number_of_shards", 1)
                .startObject("mappings")
                .startObject("properties");
            for (var field : new java.util.TreeMap<>(metadata.schema().fields()).entrySet())
                builder.startObject(field.getKey()).field("type", field.getValue().name().toLowerCase(java.util.Locale.ROOT)).endObject();
            builder.endObject().endObject();
            if (published != null) {
                builder.field("writer_epoch", writerEpoch);
                checkpoint(builder, published);
            }
            return builder.endObject();
        }
    }

    public static final class Mutation extends IndexResponse {
        private final String id;
        private final Checkpoint checkpoint;

        public Mutation(String index, String id, Checkpoint checkpoint) {
            super(index);
            this.id = id;
            this.checkpoint = checkpoint;
        }

        public Mutation(StreamInput in) throws IOException {
            super(in);
            id = IndexWire.string(in, 1024);
            checkpoint = IndexWire.checkpoint(in);
        }

        public String id() {
            return id;
        }

        public Checkpoint checkpoint() {
            return checkpoint;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            IndexWire.string(out, id);
            IndexWire.checkpoint(out, checkpoint);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field("acknowledged", true).field("_index", index()).field("_id", id);
            IndexResponse.checkpoint(builder, checkpoint);
            return builder.endObject();
        }
    }

    public static final class Refreshed extends IndexResponse {
        private final Checkpoint checkpoint;

        public Refreshed(String index, Checkpoint checkpoint) {
            super(index);
            this.checkpoint = checkpoint;
        }

        public Refreshed(StreamInput in) throws IOException {
            super(in);
            checkpoint = IndexWire.checkpoint(in);
        }

        public Checkpoint checkpoint() {
            return checkpoint;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            IndexWire.checkpoint(out, checkpoint);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field("_index", index());
            IndexResponse.checkpoint(builder, checkpoint);
            return builder.endObject();
        }
    }

    public static final class Document extends IndexResponse {
        private final String id;
        private final Optional<EngineDocument> document;

        public Document(String index, String id, Optional<EngineDocument> document) {
            super(index);
            this.id = id;
            this.document = document;
        }

        public Document(StreamInput in) throws IOException {
            super(in);
            id = IndexWire.string(in, 1024);
            document = in.readBoolean() ? Optional.of(IndexWire.document(in)) : Optional.empty();
        }

        public Optional<EngineDocument> document() {
            return document;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            IndexWire.string(out, id);
            out.writeBoolean(document.isPresent());
            if (document.isPresent()) IndexWire.document(out, document.get());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field("_index", index()).field("_id", id).field("found", document.isPresent());
            if (document.isPresent()) builder.field("_source", document.get().fields());
            return builder.endObject();
        }
    }

    public static final class Search extends IndexResponse {
        private final SearchResult result;

        public Search(String index, SearchResult result) {
            super(index);
            this.result = result;
        }

        public Search(StreamInput in) throws IOException {
            super(in);
            Checkpoint checkpoint = IndexWire.checkpoint(in);
            long total = in.readVLong();
            boolean exact = in.readBoolean();
            int count = IndexWire.count(in, 1000);
            List<SearchResult.Hit> hits = new ArrayList<>(count);
            long bytes = 0;
            for (int i = 0; i < count; i++) {
                EngineDocument document = IndexWire.document(in);
                bytes += document.estimatedBytes();
                if (bytes > 8L << 20) throw new IOException("search response exceeds limit");
                hits.add(new SearchResult.Hit(document, in.readFloat()));
            }
            result = new SearchResult(hits, total, exact, checkpoint);
        }

        public SearchResult result() {
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            IndexWire.checkpoint(out, result.checkpoint());
            out.writeVLong(result.totalHits());
            out.writeBoolean(result.exactTotal());
            out.writeVInt(result.hits().size());
            for (SearchResult.Hit hit : result.hits()) {
                IndexWire.document(out, hit.document());
                out.writeFloat(hit.score());
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            checkpoint(builder, result.checkpoint());
            builder.startObject("hits")
                .startObject("total")
                .field("value", result.totalHits())
                .field("relation", result.exactTotal() ? "eq" : "gte")
                .endObject()
                .startArray("hits");
            for (SearchResult.Hit hit : result.hits())
                builder.startObject()
                    .field("_index", index())
                    .field("_id", hit.document().id())
                    .field("_score", hit.score())
                    .field("_source", hit.document().fields())
                    .endObject();
            return builder.endArray().endObject().endObject();
        }
    }
}
