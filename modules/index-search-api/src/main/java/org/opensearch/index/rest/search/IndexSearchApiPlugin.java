/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.rest.search;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.engine.api.SearchQuery;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Committed document retrieval and a bounded subset of the query DSL. @opensearch.internal */
public final class IndexSearchApiPlugin extends Plugin implements IndexExtension, RestHandlerPlugin {
    @Override
    public List<RestHandler> getRestHandlers() {
        return List.of(new GetHandler(), new Handler());
    }

    public static final class GetHandler extends BaseRestHandler {
        @Override
        public String getName() {
            return "local_index_get";
        }

        @Override
        public RestOperationCategory operationCategory() {
            return RestOperationCategory.SEARCH;
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(HttpRequest.Method.GET, "/{index}/_doc/{id}"));
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            String index = request.param("index");
            RestJson.noBody(request);
            IndexRequest.Get action = new IndexRequest.Get(index, request.param("id"));
            return channel -> client.execute(IndexActions.GET, action, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(IndexResponse.Document response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, request);
                    return new BytesRestResponse(response.document().isPresent() ? RestStatus.OK : RestStatus.NOT_FOUND, builder);
                }
            });
        }
    }

    public static final class Handler extends BaseRestHandler {
        @Override
        public String getName() {
            return "local_index_search";
        }

        @Override
        public RestOperationCategory operationCategory() {
            return RestOperationCategory.SEARCH;
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(HttpRequest.Method.GET, "/{index}/_search"), new Route(HttpRequest.Method.POST, "/{index}/_search"));
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            String index = request.param("index");
            Map<String, Object> body = request.getHttpRequest().content().length() == 0 ? Map.of() : RestJson.object(request, 65536);
            RestJson.fields(body, Set.of("query", "size"));
            int size = body.containsKey("size") ? RestJson.integer(body.get("size"), "size", 1, 1000) : 10;
            SearchQuery query = body.containsKey("query") ? query(body.get("query"), 0, new int[1]) : new SearchQuery.All();
            IndexRequest.Search action = new IndexRequest.Search(index, query, size);
            return channel -> client.execute(IndexActions.SEARCH, action, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(IndexResponse.Search response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, request);
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        }

        private static SearchQuery query(Object value, int depth, int[] count) {
            if (depth > 16 || ++count[0] > 128) throw new IllegalArgumentException("query exceeds complexity limit");
            Map<String, Object> query = RestJson.object(value, "query");
            if (query.size() != 1) throw new IllegalArgumentException("query requires exactly one operator");
            var operator = query.entrySet().iterator().next();
            Map<String, Object> body = RestJson.object(operator.getValue(), operator.getKey());
            return switch (operator.getKey()) {
                case "match_all" -> {
                    RestJson.fields(body, Set.of());
                    yield new SearchQuery.All();
                }
                case "term", "match" -> {
                    if (body.size() != 1) throw new IllegalArgumentException("term/match requires exactly one field");
                    var field = body.entrySet().iterator().next();
                    String text = RestJson.string(field.getValue(), "query value");
                    yield operator.getKey().equals("term")
                        ? new SearchQuery.Term(field.getKey(), text)
                        : new SearchQuery.Match(field.getKey(), text);
                }
                case "bool" -> {
                    RestJson.fields(body, Set.of("must", "should", "must_not", "minimum_should_match"));
                    List<SearchQuery> must = queries(body.get("must"), body.containsKey("must"), depth, count);
                    List<SearchQuery> should = queries(body.get("should"), body.containsKey("should"), depth, count);
                    List<SearchQuery> mustNot = queries(body.get("must_not"), body.containsKey("must_not"), depth, count);
                    int minimum = body.containsKey("minimum_should_match")
                        ? RestJson.integer(body.get("minimum_should_match"), "minimum_should_match", 0, should.size())
                        : 0;
                    yield new SearchQuery.Bool(must, should, mustNot, minimum);
                }
                default -> throw new IllegalArgumentException("unsupported query operator: " + operator.getKey());
            };
        }

        private static List<SearchQuery> queries(Object value, boolean present, int depth, int[] count) {
            if (present == false) return List.of();
            List<?> values = value instanceof List<?> list ? list : java.util.Collections.singletonList(value);
            if (values.size() > 128) throw new IllegalArgumentException("too many Boolean clauses");
            List<SearchQuery> result = new ArrayList<>();
            for (Object child : values)
                result.add(query(child, depth + 1, count));
            return List.copyOf(result);
        }
    }
}
