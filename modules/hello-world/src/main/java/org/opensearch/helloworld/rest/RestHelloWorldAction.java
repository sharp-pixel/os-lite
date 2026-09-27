/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.helloworld.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.helloworld.action.HelloWorldAction;
import org.opensearch.helloworld.action.HelloWorldRequest;
import org.opensearch.helloworld.action.HelloWorldResponse;
import org.opensearch.http.HttpRequest;
import org.opensearch.rest.spi.BaseRestHandler;
import org.opensearch.rest.spi.BytesRestResponse;
import org.opensearch.rest.spi.RestBuilderListener;
import org.opensearch.rest.spi.RestOperationCategory;
import org.opensearch.rest.spi.RestRequest;
import org.opensearch.rest.spi.RestResponse;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

public class RestHelloWorldAction extends BaseRestHandler {
    @Override
    public RestOperationCategory operationCategory() {
        return RestOperationCategory.MANAGEMENT;
    }

    @Override
    public String getName() {
        return "hello_world";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> client.execute(HelloWorldAction.INSTANCE, new HelloWorldRequest(), new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(HelloWorldResponse helloWorldResponse, XContentBuilder builder) throws Exception {
                if (request.hasParam("pretty") == false) {
                    builder.prettyPrint().lfAtEnd();
                }
                helloWorldResponse.toXContent(builder, request);
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(HttpRequest.Method.GET, "/"));
    }
}
