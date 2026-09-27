/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.api;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/** Typed requests; no REST or backend objects cross the action boundary. @opensearch.experimental */
public abstract class IndexRequest extends ActionRequest {
    private final String index;

    protected IndexRequest(String index) {
        IndexMetadata.validateName(index);
        this.index = index;
    }

    protected IndexRequest(StreamInput in) throws IOException {
        super(in);
        if (in.readByte() != 1) throw new IOException("unsupported index action wire version");
        index = IndexWire.string(in, 128);
        try {
            IndexMetadata.validateName(index);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid index name", e);
        }
    }

    public final String index() {
        return index;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte((byte) 1);
        IndexWire.string(out, index);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parent, Map<String, String> headers) {
        return new IndexTask(id, type, action, index, parent, headers);
    }

    public static final class IndexTask extends CancellableTask {
        private final OperationContext context = OperationContext.standard();

        IndexTask(long id, String type, String action, String index, TaskId parent, Map<String, String> headers) {
            super(id, type, action, "index [" + index + "]", parent, headers);
        }

        public OperationContext context() {
            return context;
        }

        @Override
        protected void onCancelled() {
            context.cancel();
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return true;
        }
    }

    public static final class Create extends IndexRequest {
        private final String engine;
        private final Schema schema;

        public Create(String index, String engine, Schema schema) {
            super(index);
            new IndexMetadata(index, new java.util.UUID(0, 0), engine, schema);
            this.engine = engine;
            this.schema = schema;
        }

        public Create(StreamInput in) throws IOException {
            super(in);
            engine = IndexWire.string(in, 64);
            schema = IndexWire.schema(in);
            if (engine.matches("[a-z][a-z0-9-]{0,63}") == false) throw new IOException("invalid engine ID");
        }

        public String engine() {
            return engine;
        }

        public Schema schema() {
            return schema;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            IndexWire.string(out, engine);
            IndexWire.schema(out, schema);
        }
    }

    public static final class Describe extends IndexRequest {
        public Describe(String index) {
            super(index);
        }

        public Describe(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static final class Refresh extends IndexRequest {
        public Refresh(String index) {
            super(index);
        }

        public Refresh(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static final class Put extends IndexRequest {
        private final EngineDocument document;

        public Put(String index, EngineDocument document) {
            super(index);
            this.document = Objects.requireNonNull(document);
        }

        public Put(StreamInput in) throws IOException {
            super(in);
            document = IndexWire.document(in);
        }

        public EngineDocument document() {
            return document;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            IndexWire.document(out, document);
        }
    }

    public abstract static class DocumentId extends IndexRequest {
        private final String id;

        protected DocumentId(String index, String id) {
            super(index);
            EngineDocument.validateId(id);
            this.id = id;
        }

        protected DocumentId(StreamInput in) throws IOException {
            super(in);
            id = IndexWire.string(in, 1024);
            try {
                EngineDocument.validateId(id);
            } catch (IllegalArgumentException e) {
                throw new IOException("invalid document ID", e);
            }
        }

        public final String id() {
            return id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            IndexWire.string(out, id);
        }
    }

    public static final class Delete extends DocumentId {
        public Delete(String index, String id) {
            super(index, id);
        }

        public Delete(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static final class Get extends DocumentId {
        public Get(String index, String id) {
            super(index, id);
        }

        public Get(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static final class Search extends IndexRequest {
        private final SearchQuery query;
        private final int limit;

        public Search(String index, SearchQuery query, int limit) {
            super(index);
            SearchQuery.estimatedBytes(query);
            if (limit < 1 || limit > 1000) throw new IllegalArgumentException("size must be 1 to 1000");
            this.query = query;
            this.limit = limit;
        }

        public Search(StreamInput in) throws IOException {
            super(in);
            query = IndexWire.query(in);
            limit = IndexWire.count(in, 1000);
            if (limit == 0) throw new IOException("size must be positive");
        }

        public SearchQuery query() {
            return query;
        }

        public int limit() {
            return limit;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            IndexWire.query(out, query);
            out.writeVInt(limit);
        }
    }
}
