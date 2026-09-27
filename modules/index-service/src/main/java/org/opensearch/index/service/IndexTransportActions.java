/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.service;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.index.api.IndexActions;
import org.opensearch.index.api.IndexRequest;
import org.opensearch.index.api.IndexResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.concurrent.CompletionStage;

/** Action filters, cancellation and thread context apply equally to REST and transport callers. @opensearch.internal */
public final class IndexTransportActions {
    private IndexTransportActions() {}

    private abstract static class Base<R extends IndexRequest, S extends IndexResponse> extends HandledTransportAction<R, S> {
        final LocalIndexService indices;
        private final TransportService transport;

        Base(
            ActionType<S> action,
            Writeable.Reader<R> reader,
            TransportService transport,
            ActionFilters filters,
            LocalIndexService indices
        ) {
            super(action.name(), transport, filters, reader);
            this.indices = indices;
            this.transport = transport;
        }

        abstract CompletionStage<S> perform(R request, OperationContext context);

        @Override
        protected final void doExecute(Task task, R request, ActionListener<S> listener) {
            ActionListener<S> preserved = ContextPreservingActionListener.wrapPreservingContext(
                listener,
                transport.getThreadPool().getThreadContext()
            );
            OperationContext context = task instanceof IndexRequest.IndexTask indexTask ? indexTask.context() : OperationContext.standard();
            try {
                perform(request, context).whenComplete((response, error) -> {
                    if (error == null) preserved.onResponse(response);
                    else preserved.onFailure(IndexFailures.action(error));
                });
            } catch (Exception e) {
                preserved.onFailure(IndexFailures.action(e));
            }
        }
    }

    public static final class Claim extends Base<IndexRequest.Claim, IndexResponse.Metadata> {
        @Inject
        public Claim(TransportService transport, ActionFilters filters, LocalIndexService indices) {
            super(IndexActions.CLAIM, IndexRequest.Claim::new, transport, filters, indices);
        }

        @Override
        CompletionStage<IndexResponse.Metadata> perform(IndexRequest.Claim request, OperationContext context) {
            return indices.claim(request, context);
        }
    }

    public static final class Create extends Base<IndexRequest.Create, IndexResponse.Metadata> {
        @Inject
        public Create(TransportService transport, ActionFilters filters, LocalIndexService indices) {
            super(IndexActions.CREATE, IndexRequest.Create::new, transport, filters, indices);
        }

        @Override
        CompletionStage<IndexResponse.Metadata> perform(IndexRequest.Create request, OperationContext context) {
            return indices.create(request, context);
        }
    }

    public static final class Describe extends Base<IndexRequest.Describe, IndexResponse.Metadata> {
        @Inject
        public Describe(TransportService transport, ActionFilters filters, LocalIndexService indices) {
            super(IndexActions.DESCRIBE, IndexRequest.Describe::new, transport, filters, indices);
        }

        @Override
        CompletionStage<IndexResponse.Metadata> perform(IndexRequest.Describe request, OperationContext context) {
            return indices.describe(request, context);
        }
    }

    public static final class Put extends Base<IndexRequest.Put, IndexResponse.Mutation> {
        @Inject
        public Put(TransportService transport, ActionFilters filters, LocalIndexService indices) {
            super(IndexActions.PUT, IndexRequest.Put::new, transport, filters, indices);
        }

        @Override
        CompletionStage<IndexResponse.Mutation> perform(IndexRequest.Put request, OperationContext context) {
            return indices.put(request, context);
        }
    }

    public static final class Delete extends Base<IndexRequest.Delete, IndexResponse.Mutation> {
        @Inject
        public Delete(TransportService transport, ActionFilters filters, LocalIndexService indices) {
            super(IndexActions.DELETE, IndexRequest.Delete::new, transport, filters, indices);
        }

        @Override
        CompletionStage<IndexResponse.Mutation> perform(IndexRequest.Delete request, OperationContext context) {
            return indices.delete(request, context);
        }
    }

    public static final class Refresh extends Base<IndexRequest.Refresh, IndexResponse.Refreshed> {
        @Inject
        public Refresh(TransportService transport, ActionFilters filters, LocalIndexService indices) {
            super(IndexActions.REFRESH, IndexRequest.Refresh::new, transport, filters, indices);
        }

        @Override
        CompletionStage<IndexResponse.Refreshed> perform(IndexRequest.Refresh request, OperationContext context) {
            return indices.refresh(request, context);
        }
    }

    public static final class Get extends Base<IndexRequest.Get, IndexResponse.Document> {
        @Inject
        public Get(TransportService transport, ActionFilters filters, LocalIndexService indices) {
            super(IndexActions.GET, IndexRequest.Get::new, transport, filters, indices);
        }

        @Override
        CompletionStage<IndexResponse.Document> perform(IndexRequest.Get request, OperationContext context) {
            return indices.get(request, context);
        }
    }

    public static final class Search extends Base<IndexRequest.Search, IndexResponse.Search> {
        @Inject
        public Search(TransportService transport, ActionFilters filters, LocalIndexService indices) {
            super(IndexActions.SEARCH, IndexRequest.Search::new, transport, filters, indices);
        }

        @Override
        CompletionStage<IndexResponse.Search> perform(IndexRequest.Search request, OperationContext context) {
            return indices.search(request, context);
        }
    }
}
