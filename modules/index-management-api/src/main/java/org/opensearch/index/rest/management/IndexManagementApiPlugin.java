/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.rest.management;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.engine.api.Schema;
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
import java.util.Set;

/** Index creation, metadata and reader refresh routes. @opensearch.internal */
public final class IndexManagementApiPlugin extends Plugin implements IndexExtension, RestHandlerPlugin {
    @Override
    public List<RestHandler> getRestHandlers() {
        return List.of(new Handler());
    }

    public static final class Handler extends BaseRestHandler {
        @Override
        public String getName() {
            return "local_index_management";
        }

        @Override
        public RestOperationCategory operationCategory() {
            return RestOperationCategory.MANAGEMENT;
        }

        @Override
        public List<Route> routes() {
            return List.of(
                new Route(HttpRequest.Method.PUT, "/{index}"),
                new Route(HttpRequest.Method.GET, "/{index}"),
                new Route(HttpRequest.Method.POST, "/{index}/_refresh")
            );
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            String index = request.param("index");
            if (request.method() == HttpRequest.Method.POST) {
                RestJson.noBody(request);
                IndexRequest.Refresh action = new IndexRequest.Refresh(index);
                return channel -> client.execute(IndexActions.REFRESH, action, new RestBuilderListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(IndexResponse.Refreshed response, XContentBuilder builder) throws Exception {
                        response.toXContent(builder, request);
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
            }
            if (request.method() == HttpRequest.Method.GET) {
                RestJson.noBody(request);
                IndexRequest.Describe action = new IndexRequest.Describe(index);
                return channel -> client.execute(IndexActions.DESCRIBE, action, new RestBuilderListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(IndexResponse.Metadata response, XContentBuilder builder) throws Exception {
                        response.toXContent(builder, request);
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
            }
            Map<String, Object> body = RestJson.object(request, 65536);
            RestJson.fields(body, Set.of("engine", "mappings"));
            String engine = body.containsKey("engine") ? RestJson.string(body.get("engine"), "engine") : "lucene";
            Map<String, Object> mapping = RestJson.object(body.get("mappings"), "mappings");
            RestJson.fields(mapping, Set.of("properties"));
            Map<String, Object> properties = RestJson.object(mapping.get("properties"), "properties");
            if (properties.isEmpty() || properties.size() > 128) throw new IllegalArgumentException("mappings require 1 to 128 fields");
            Map<String, Schema.FieldType> fields = new LinkedHashMap<>();
            for (var entry : properties.entrySet()) {
                Map<String, Object> field = RestJson.object(entry.getValue(), "mapping field");
                RestJson.fields(field, Set.of("type"));
                Schema.FieldType type = switch (RestJson.string(field.get("type"), "field type")) {
                    case "text" -> Schema.FieldType.TEXT;
                    case "keyword" -> Schema.FieldType.KEYWORD;
                    default -> throw new IllegalArgumentException("supported field types are text and keyword");
                };
                fields.put(entry.getKey(), type);
            }
            IndexRequest.Create action = new IndexRequest.Create(index, engine, new Schema(1, fields));
            return channel -> client.execute(IndexActions.CREATE, action, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(IndexResponse.Metadata response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, request);
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        }
    }
}
