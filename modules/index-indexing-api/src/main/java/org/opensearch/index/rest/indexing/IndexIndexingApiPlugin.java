/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.rest.indexing;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.http.HttpRequest;
import org.opensearch.index.api.IndexActions;
import org.opensearch.index.api.IndexExtension;
import org.opensearch.index.api.IndexRequest;
import org.opensearch.index.api.IndexResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.spi.BaseRestHandler;
import org.opensearch.rest.spi.BytesRestResponse;
import org.opensearch.rest.spi.RestBuilderListener;
import org.opensearch.rest.spi.RestHandler;
import org.opensearch.rest.spi.RestHandlerPlugin;
import org.opensearch.rest.spi.RestJson;
import org.opensearch.rest.spi.RestOperationCategory;
import org.opensearch.rest.spi.RestRequest;
import org.opensearch.rest.spi.RestResponse;
import org.opensearch.transport.client.node.NodeClient;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Full document replacement and deletion routes with owned request values. @opensearch.internal */
public final class IndexIndexingApiPlugin extends Plugin implements IndexExtension, RestHandlerPlugin {
    @Override
    public List<RestHandler> getRestHandlers() {
        return List.of(new Handler());
    }

    public static final class Handler extends BaseRestHandler {
        @Override
        public String getName() {
            return "local_index_documents";
        }

        @Override
        public RestOperationCategory operationCategory() {
            return RestOperationCategory.INDEXING;
        }

        @Override
        public List<Route> routes() {
            return List.of(
                new Route(HttpRequest.Method.PUT, "/{index}/_doc/{id}"),
                new Route(HttpRequest.Method.DELETE, "/{index}/_doc/{id}")
            );
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            String index = request.param("index");
            String id = request.param("id");
            if (request.method() == HttpRequest.Method.DELETE) {
                RestJson.noBody(request);
                IndexRequest.Delete action = new IndexRequest.Delete(index, id);
                return channel -> client.execute(IndexActions.DELETE, action, new RestBuilderListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(IndexResponse.Mutation response, XContentBuilder builder) throws Exception {
                        response.toXContent(builder, request);
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
            }
            Map<String, Object> body = RestJson.object(request, 1 << 20);
            if (body.size() > 128) throw new IllegalArgumentException("document exceeds field limit");
            Map<String, String> fields = new LinkedHashMap<>();
            for (var entry : body.entrySet())
                fields.put(entry.getKey(), RestJson.string(entry.getValue(), "document field"));
            IndexRequest.Put action = new IndexRequest.Put(
                index,
                new EngineDocument(id, fields, BytesReference.toBytes(request.getHttpRequest().content()))
            );
            return channel -> client.execute(IndexActions.PUT, action, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(IndexResponse.Mutation response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, request);
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        }
    }
}
