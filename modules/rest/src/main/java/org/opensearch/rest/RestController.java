/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.path.PathTrie;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.Streams;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.CorsHandler;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpChunk;
import org.opensearch.http.HttpHandlingSettings;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.StreamingHttpChannel;
import org.opensearch.http.UrlUtils;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.transport.client.node.NodeClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.core.rest.RestStatus.METHOD_NOT_ALLOWED;
import static org.opensearch.core.rest.RestStatus.NOT_ACCEPTABLE;
import static org.opensearch.core.rest.RestStatus.NOT_FOUND;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.rest.spi.BytesRestResponse.TEXT_CONTENT_TYPE;

/**
 * OpenSearch REST controller
 *
 * @opensearch.api
 */
public class RestController implements HttpServerTransport.Dispatcher {

    private static final Logger logger = LogManager.getLogger(RestController.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestController.class);
    private static final String OPENSEARCH_PRODUCT_ORIGIN_HTTP_HEADER = "X-opensearch-product-origin";

    private static final BytesReference FAVICON_RESPONSE;

    static {
        try (InputStream stream = RestController.class.getResourceAsStream("/config/favicon.ico")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(stream, out);
            FAVICON_RESPONSE = new BytesArray(out.toByteArray());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private final PathTrie<RestMethodHandlers> handlers = new PathTrie<>(UrlUtils.REST_DECODER);

    private final UnaryOperator<org.opensearch.rest.spi.RestHandler> handlerWrapper;

    private final NodeClient client;

    private final CircuitBreakerService circuitBreakerService;

    /** Rest headers that are copied to internal requests made during a rest request. */
    private final Set<RestHeaderDefinition> headersToCopy;

    private final NamedXContentRegistry xContentRegistry;
    private final BigArrays bigArrays;
    private final HttpHandlingSettings handlingSettings;
    private final CorsHandler corsHandler;
    private final RestTracer httpTracer;
    private final Tracer tracer;
    private final Set<org.opensearch.rest.spi.RestOperationCategory> enabledCategories;

    public RestController(
        Set<RestHeaderDefinition> headersToCopy,
        UnaryOperator<org.opensearch.rest.spi.RestHandler> handlerWrapper,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        BigArrays bigArrays,
        HttpHandlingSettings handlingSettings,
        CorsHandler corsHandler,
        RestTracer httpTracer,
        Tracer tracer
    ) {
        this(
            headersToCopy,
            handlerWrapper,
            client,
            circuitBreakerService,
            xContentRegistry,
            bigArrays,
            handlingSettings,
            corsHandler,
            httpTracer,
            tracer,
            java.util.EnumSet.allOf(org.opensearch.rest.spi.RestOperationCategory.class)
        );
    }

    public RestController(
        Set<RestHeaderDefinition> headersToCopy,
        UnaryOperator<org.opensearch.rest.spi.RestHandler> handlerWrapper,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        BigArrays bigArrays,
        HttpHandlingSettings handlingSettings,
        CorsHandler corsHandler,
        RestTracer httpTracer,
        Tracer tracer,
        Set<org.opensearch.rest.spi.RestOperationCategory> enabledCategories
    ) {
        this.enabledCategories = Set.copyOf(enabledCategories);
        this.headersToCopy = headersToCopy;
        if (handlerWrapper == null) {
            handlerWrapper = h -> h; // passthrough if no wrapper set
        }

        this.handlerWrapper = handlerWrapper;
        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
        this.xContentRegistry = xContentRegistry;
        this.bigArrays = bigArrays;
        this.handlingSettings = handlingSettings;
        this.corsHandler = corsHandler;
        this.httpTracer = httpTracer;
        this.tracer = tracer;
        registerHandlerNoWrap(
            HttpRequest.Method.GET,
            "/favicon.ico",
            (request, channel, clnt) -> channel.sendResponse(
                new org.opensearch.rest.spi.BytesRestResponse(RestStatus.OK, "image/x-icon", FAVICON_RESPONSE)
            )
        );
    }

    /**
     * Returns an iterator over registered REST method handlers.
     * @return {@link Iterator} of {@link MethodHandlers}
     */
    public Iterator<MethodHandlers> getAllHandlers() {
        List<MethodHandlers> methodHandlers = new ArrayList<>();
        handlers.retrieveAll().forEachRemaining(methodHandlers::add);
        return methodHandlers.iterator();
    }

    /**
     * Registers a REST handler to be executed when the provided {@code method} and {@code path} match the request.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g., "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     * @param deprecationMessage The message to log and send as a header in the response
     */
    protected void registerAsDeprecatedHandler(
        HttpRequest.Method method,
        String path,
        org.opensearch.rest.spi.RestHandler handler,
        String deprecationMessage
    ) {
        assert (handler instanceof DeprecationRestHandler) == false;

        registerHandler(method, path, new DeprecationRestHandler(handler, deprecationMessage, deprecationLogger));
    }

    /**
     * Registers a REST handler to be executed when the provided {@code method} and {@code path} match the request, or when provided
     * with {@code deprecatedMethod} and {@code deprecatedPath}. Expected usage:
     * <pre><code>
     * // remove deprecation in next major release
     * controller.registerWithDeprecatedHandler(POST, "/_forcemerge", this,
     *                                          POST, "/_optimize", deprecationLogger);
     * controller.registerWithDeprecatedHandler(POST, "/{index}/_forcemerge", this,
     *                                          POST, "/{index}/_optimize", deprecationLogger);
     * </code></pre>
     * <p>
     * The registered REST handler ({@code method} with {@code path}) is a normal REST handler that is not deprecated and it is
     * replacing the deprecated REST handler ({@code deprecatedMethod} with {@code deprecatedPath}) that is using the <em>same</em>
     * {@code handler}.
     * <p>
     * Deprecated REST handlers without a direct replacement should be deprecated directly using {@link #registerAsDeprecatedHandler}
     * and a specific message.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g., "/_forcemerge")
     * @param handler The handler to actually execute
     * @param deprecatedMethod GET, POST, etc.
     * @param deprecatedPath <em>Deprecated</em> path to handle (e.g., "/_optimize")
     */
    protected void registerWithDeprecatedHandler(
        HttpRequest.Method method,
        String path,
        org.opensearch.rest.spi.RestHandler handler,
        HttpRequest.Method deprecatedMethod,
        String deprecatedPath
    ) {
        // e.g., [POST /_optimize] is deprecated! Use [POST /_forcemerge] instead.
        final String deprecationMessage = "["
            + deprecatedMethod.name()
            + " "
            + deprecatedPath
            + "] is deprecated! Use ["
            + method.name()
            + " "
            + path
            + "] instead.";

        registerHandler(method, path, handler);
        registerAsDeprecatedHandler(deprecatedMethod, deprecatedPath, handler, deprecationMessage);
    }

    /**
     * Registers a REST handler to be executed when one of the provided methods and path match the request.
     *
     * @param path Path to handle (e.g., "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     * @param method GET, POST, etc.
     */
    protected void registerHandler(HttpRequest.Method method, String path, org.opensearch.rest.spi.RestHandler handler) {
        registerHandlerNoWrap(method, path, handlerWrapper.apply(handler));
    }

    private void registerHandlerNoWrap(HttpRequest.Method method, String path, org.opensearch.rest.spi.RestHandler maybeWrappedHandler) {
        // Disabled APIs are absent from both dispatch and route discovery, including OPTIONS and Allow.
        if (enabledCategories.contains(maybeWrappedHandler.operationCategory()) == false) {
            return;
        }
        handlers.insertOrUpdate(
            path,
            new RestMethodHandlers(path, maybeWrappedHandler, method),
            (mHandlers, newMHandler) -> mHandlers.addMethods(maybeWrappedHandler, method)
        );
    }

    /**
     * Registers a REST handler with the controller. The REST handler declares the {@code method}
     * and {@code path} combinations.
     */
    public void registerHandler(final org.opensearch.rest.spi.RestHandler restHandler) {
        restHandler.routes().forEach(route -> registerHandler(route.getMethod(), route.getPath(), restHandler));
        restHandler.deprecatedRoutes()
            .forEach(route -> registerAsDeprecatedHandler(route.getMethod(), route.getPath(), restHandler, route.getDeprecationMessage()));
        restHandler.replacedRoutes()
            .forEach(
                route -> registerWithDeprecatedHandler(
                    route.getMethod(),
                    route.getPath(),
                    restHandler,
                    route.getDeprecatedMethod(),
                    route.getDeprecatedPath()
                )
            );
    }

    @Override
    public void dispatchRequest(HttpRequest httpRequest, HttpChannel httpChannel, ThreadContext threadContext) {
        dispatchHttpRequest(httpRequest, httpChannel, threadContext, null);
    }

    @Override
    public void dispatchBadRequest(HttpRequest httpRequest, HttpChannel httpChannel, ThreadContext threadContext, Throwable cause) {
        final Exception e;
        if (cause == null) {
            e = new OpenSearchException("unknown cause");
        } else if (cause instanceof Exception) {
            e = (Exception) cause;
        } else {
            e = new OpenSearchException(cause);
        }
        dispatchHttpRequest(httpRequest, httpChannel, threadContext, e);
    }

    private void dispatchHttpRequest(
        final HttpRequest httpRequest,
        final HttpChannel httpChannel,
        final ThreadContext threadContext,
        final Exception initialBadRequestCause
    ) {
        final Exception originalException = initialBadRequestCause;
        Exception badRequestCause = initialBadRequestCause;

        /*
         * We want to create a REST request from the incoming request. However, creating this request could fail if there
         * are incorrectly encoded parameters, or the Content-Type header is invalid. If one of these specific failures occurs, we
         * attempt to create a REST request again without the input that caused the exception (e.g., we remove the Content-Type header,
         * or skip decoding the parameters). Once we have a request in hand, we then dispatch the request as a bad request with the
         * underlying exception that caused us to treat the request as bad.
         */
        final RestRequest restRequest;
        {
            RestRequest innerRestRequest;
            try {
                innerRestRequest = RestRequest.request(xContentRegistry, httpRequest, httpChannel);
            } catch (final RestRequest.ContentTypeHeaderException e) {
                badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
                innerRestRequest = requestWithoutContentTypeHeader(httpRequest, httpChannel, badRequestCause);
            } catch (final RestRequest.BadParameterException e) {
                badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
                innerRestRequest = RestRequest.requestWithoutParameters(xContentRegistry, httpRequest, httpChannel);
            }
            restRequest = innerRestRequest;
        }

        final RestTracer trace = httpTracer.maybeTraceRequest(restRequest, originalException);

        /*
         * We now want to create a channel used to send the response on. However, creating this channel can fail if there are invalid
         * parameter values for any of the filter_path, human, or pretty parameters. We detect these specific failures via an
         * IllegalArgumentException from the channel constructor and then attempt to create a new channel that bypasses parsing of these
         * parameter values.
         */
        final org.opensearch.rest.spi.RestChannel channel;
        {
            org.opensearch.rest.spi.RestChannel innerChannel;
            try {
                if (httpChannel instanceof StreamingHttpChannel) {
                    innerChannel = new DefaultStreamingRestChannel(
                        (StreamingHttpChannel) httpChannel,
                        httpRequest,
                        restRequest,
                        bigArrays,
                        handlingSettings,
                        threadContext,
                        corsHandler,
                        trace
                    );
                } else {
                    innerChannel = new DefaultRestChannel(
                        httpChannel,
                        httpRequest,
                        restRequest,
                        bigArrays,
                        handlingSettings,
                        threadContext,
                        corsHandler,
                        trace
                    );
                }
            } catch (final IllegalArgumentException e) {
                badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
                final RestRequest innerRequest = RestRequest.requestWithoutParameters(xContentRegistry, httpRequest, httpChannel);

                if (httpChannel instanceof StreamingHttpChannel) {
                    innerChannel = new DefaultStreamingRestChannel(
                        (StreamingHttpChannel) httpChannel,
                        httpRequest,
                        innerRequest,
                        bigArrays,
                        handlingSettings,
                        threadContext,
                        corsHandler,
                        trace
                    );
                } else {
                    innerChannel = new DefaultRestChannel(
                        httpChannel,
                        httpRequest,
                        innerRequest,
                        bigArrays,
                        handlingSettings,
                        threadContext,
                        corsHandler,
                        trace
                    );
                }
            }
            channel = innerChannel;
        }

        final Span span = tracer.startSpan(SpanBuilder.from(restRequest.getHttpRequest()));
        try (final SpanScope spanScope = tracer.withSpanInScope(span)) {
            final org.opensearch.rest.spi.RestChannel traceableChannel = TraceableRestChannel.create(channel, span, tracer);
            if (badRequestCause != null) {
                sendBadRequestResponse(traceableChannel, threadContext, badRequestCause);
            } else {
                try {
                    tryAllHandlers(restRequest, traceableChannel, threadContext);
                } catch (Exception e) {
                    try {
                        traceableChannel.sendResponse(new org.opensearch.rest.spi.BytesRestResponse(traceableChannel, e));
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.error(
                            () -> new ParameterizedMessage("failed to send failure response for uri [{}]", restRequest.uri()),
                            inner
                        );
                    }
                }
            }
        }
    }

    private RestRequest requestWithoutContentTypeHeader(HttpRequest httpRequest, HttpChannel httpChannel, Exception badRequestCause) {
        HttpRequest httpRequestWithoutContentType = httpRequest.removeHeader("Content-Type");
        try {
            return RestRequest.request(xContentRegistry, httpRequestWithoutContentType, httpChannel);
        } catch (final RestRequest.BadParameterException e) {
            badRequestCause.addSuppressed(e);
            return RestRequest.requestWithoutParameters(xContentRegistry, httpRequestWithoutContentType, httpChannel);
        }
    }

    private void sendBadRequestResponse(
        final org.opensearch.rest.spi.RestChannel channel,
        final ThreadContext threadContext,
        final Throwable cause
    ) {
        try {
            final Exception e;
            if (cause == null) {
                e = new OpenSearchException("unknown cause");
            } else if (cause instanceof Exception) {
                e = (Exception) cause;
            } else {
                e = new OpenSearchException(cause);
            }
            channel.sendResponse(new org.opensearch.rest.spi.BytesRestResponse(channel, BAD_REQUEST, e));
        } catch (final IOException e) {
            if (cause != null) {
                e.addSuppressed(cause);
            }
            logger.warn("failed to send bad request response", e);
            channel.sendResponse(
                new org.opensearch.rest.spi.BytesRestResponse(
                    INTERNAL_SERVER_ERROR,
                    org.opensearch.rest.spi.BytesRestResponse.TEXT_CONTENT_TYPE,
                    BytesArray.EMPTY
                )
            );
        }
    }

    private void dispatchRequest(
        RestRequest request,
        org.opensearch.rest.spi.RestChannel channel,
        org.opensearch.rest.spi.RestHandler handler
    ) throws Exception {
        final int contentLength = request.content().length();
        final MediaType mediaType = request.getMediaType();
        if (contentLength > 0) {
            if (mediaType == null) {
                sendContentTypeErrorMessage(request.getAllHeaderValues("Content-Type"), channel);
                return;
            }
            if (handler.supportsContentStream() && mediaType != MediaTypeRegistry.JSON && mediaType != XContentType.SMILE) {
                channel.sendResponse(
                    org.opensearch.rest.spi.BytesRestResponse.createSimpleErrorResponse(
                        channel,
                        RestStatus.NOT_ACCEPTABLE,
                        "Content-Type [" + mediaType + "] does not support stream parsing. Use JSON or SMILE instead"
                    )
                );
                return;
            }
        }

        org.opensearch.rest.spi.RestChannel responseChannel = channel;
        try {
            if (handler.canTripCircuitBreaker()) {
                inFlightRequestsBreaker(circuitBreakerService).addEstimateBytesAndMaybeBreak(contentLength, "<http_request>");
            } else {
                inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(contentLength);
            }

            if (handler.supportsStreaming()) {
                // The handler may support streaming but not the engine, in this case we fail with the bad request
                if (channel instanceof org.opensearch.rest.spi.StreamingRestChannel) {
                    responseChannel = new StreamHandlingHttpChannel(
                        (org.opensearch.rest.spi.StreamingRestChannel) channel,
                        circuitBreakerService,
                        contentLength
                    );
                } else {
                    throw new IllegalStateException(
                        "The engine does not support HTTP streaming, unable to serve uri ["
                            + request.getHttpRequest().uri()
                            + "] and method ["
                            + request.getHttpRequest().method()
                            + "]"
                    );
                }

                if (mediaType == null) {
                    sendContentTypeErrorMessage(request.getAllHeaderValues("Content-Type"), responseChannel);
                    return;
                }
            } else {
                // if we could reserve bytes for the request we need to send the response also over this channel
                responseChannel = new ResourceHandlingHttpChannel(channel, circuitBreakerService, contentLength);
            }

            // TODO: Count requests double in the circuit breaker if they need copying?
            if (handler.allowsUnsafeBuffers() == false) {
                request.ensureSafeBuffers();
            }
            if (handler.allowSystemIndexAccessByDefault() == false && request.header(OPENSEARCH_PRODUCT_ORIGIN_HTTP_HEADER) == null) {
                // The OPENSEARCH_PRODUCT_ORIGIN_HTTP_HEADER indicates that the request is coming from an OpenSearch product with a plan
                // to move away from direct access to system indices, and thus deprecation warnings should not be emitted.
                // This header is intended for internal use only.
                // client.threadPool().getThreadContext().putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.FALSE.toString());
            }

            handler.handleRequest(request, responseChannel, client);
        } catch (Exception e) {
            responseChannel.sendResponse(new org.opensearch.rest.spi.BytesRestResponse(responseChannel, e));
        }
    }

    private boolean handleNoHandlerFound(
        String rawPath,
        HttpRequest.Method method,
        String uri,
        org.opensearch.rest.spi.RestChannel channel
    ) {
        // Get the map of matching handlers for a request, for the full set of HTTP methods.
        final Set<HttpRequest.Method> validMethodSet = getValidHandlerMethodSet(rawPath);
        if (validMethodSet.contains(method) == false) {
            if (method == HttpRequest.Method.OPTIONS && validMethodSet.isEmpty() == false) {
                handleOptionsRequest(channel, validMethodSet);
                return true;
            }
            if (validMethodSet.isEmpty() == false) {
                // If an alternative handler for an explicit path is registered to a
                // different HTTP method than the one supplied - return a 405 Method
                // Not Allowed error.
                handleUnsupportedHttpMethod(uri, method, channel, validMethodSet, null);
                return true;
            }
        }
        return false;
    }

    private void sendContentTypeErrorMessage(@Nullable List<String> contentTypeHeader, org.opensearch.rest.spi.RestChannel channel)
        throws IOException {
        final String errorMessage;
        if (contentTypeHeader == null) {
            errorMessage = "Content-Type header is missing";
        } else {
            errorMessage = "Content-Type header [" + Strings.collectionToCommaDelimitedString(contentTypeHeader) + "] is not supported";
        }

        channel.sendResponse(org.opensearch.rest.spi.BytesRestResponse.createSimpleErrorResponse(channel, NOT_ACCEPTABLE, errorMessage));
    }

    private void tryAllHandlers(
        final RestRequest request,
        final org.opensearch.rest.spi.RestChannel channel,
        final ThreadContext threadContext
    ) throws Exception {
        for (final RestHeaderDefinition restHeader : headersToCopy) {
            final String name = restHeader.getName();
            final List<String> headerValues = request.getAllHeaderValues(name);
            if (headerValues != null && headerValues.isEmpty() == false) {
                final List<String> distinctHeaderValues = headerValues.stream().distinct().collect(Collectors.toList());
                if (restHeader.isMultiValueAllowed() == false && distinctHeaderValues.size() > 1) {
                    channel.sendResponse(
                        org.opensearch.rest.spi.BytesRestResponse.createSimpleErrorResponse(
                            channel,
                            BAD_REQUEST,
                            "multiple values for single-valued header [" + name + "]."
                        )
                    );
                    return;
                } else {
                    threadContext.putHeader(name, String.join(",", distinctHeaderValues));
                }
            }
        }
        // error_trace cannot be used when we disable detailed errors
        // we consume the error_trace parameter first to ensure that it is always consumed
        if (request.paramAsBoolean("error_trace", false) && channel.detailedErrorsEnabled() == false) {
            channel.sendResponse(
                org.opensearch.rest.spi.BytesRestResponse.createSimpleErrorResponse(
                    channel,
                    BAD_REQUEST,
                    "error traces in responses are disabled."
                )
            );
            return;
        }

        final String rawPath = request.rawPath();
        final String uri = request.uri();
        final HttpRequest.Method requestMethod;
        try {
            // Resolves the HTTP method and fails if the method is invalid
            requestMethod = request.method();
            // Loop through all possible handlers, attempting to dispatch the request
            Iterator<RestMethodHandlers> allHandlers = getAllRestMethodHandlers(request.params(), rawPath);
            while (allHandlers.hasNext()) {
                final org.opensearch.rest.spi.RestHandler handler;
                final RestMethodHandlers handlers = allHandlers.next();
                if (handlers == null) {
                    handler = null;
                } else {
                    handler = handlers.getHandler(requestMethod);
                }
                if (handler == null) {
                    if (handleNoHandlerFound(rawPath, requestMethod, uri, channel)) {
                        return;
                    }
                } else {
                    dispatchRequest(request, channel, handler);
                    return;
                }
            }
        } catch (final IllegalArgumentException e) {
            handleUnsupportedHttpMethod(uri, null, channel, getValidHandlerMethodSet(rawPath), e);
            return;
        }
        // No enabled route matched the path.
        handleNotFound(uri, requestMethod, channel);
    }

    Iterator<RestMethodHandlers> getAllRestMethodHandlers(@Nullable Map<String, String> requestParamsRef, String rawPath) {
        final Supplier<Map<String, String>> paramsSupplier;
        if (requestParamsRef == null) {
            paramsSupplier = () -> null;
        } else {
            // Between retrieving the correct path, we need to reset the parameters,
            // otherwise parameters are parsed out of the URI that aren't actually handled.
            final Map<String, String> originalParams = new HashMap<>(requestParamsRef);
            paramsSupplier = () -> {
                // PathTrie modifies the request, so reset the params between each iteration
                requestParamsRef.clear();
                requestParamsRef.putAll(originalParams);
                return requestParamsRef;
            };
        }
        // we use rawPath since we don't want to decode it while processing the path resolution
        // so we can handle things like:
        // my_index/my_type/http%3A%2F%2Fwww.google.com
        return handlers.retrieveAll(rawPath, paramsSupplier);
    }

    /**
     * Handle requests to a valid REST endpoint using an unsupported HTTP
     * method. A 405 HTTP response code is returned, and the response 'Allow'
     * header includes a list of valid HTTP methods for the endpoint (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-10.4.6">HTTP/1.1 -
     * 10.4.6 - 405 Method Not Allowed</a>).
     */
    private void handleUnsupportedHttpMethod(
        String uri,
        @Nullable HttpRequest.Method method,
        final org.opensearch.rest.spi.RestChannel channel,
        final Set<HttpRequest.Method> validMethodSet,
        @Nullable final IllegalArgumentException exception
    ) {
        try {
            final StringBuilder msg = new StringBuilder();
            if (exception == null) {
                msg.append("Incorrect HTTP method for uri [").append(uri);
                msg.append("] and method [").append(method).append("]");
            } else {
                // Not using the error message directly from 'exception.getMessage()' to avoid unescaped HTML special characters,
                // in case false-positive cross site scripting vulnerability is detected by common security scanners.
                msg.append("Unexpected HTTP method");
            }
            if (validMethodSet.isEmpty() == false) {
                msg.append(", allowed: ").append(validMethodSet);
            }
            org.opensearch.rest.spi.BytesRestResponse bytesRestResponse = org.opensearch.rest.spi.BytesRestResponse
                .createSimpleErrorResponse(channel, METHOD_NOT_ALLOWED, msg.toString());
            if (validMethodSet.isEmpty() == false) {
                bytesRestResponse.addHeader("Allow", Strings.collectionToDelimitedString(validMethodSet, ","));
            }
            channel.sendResponse(bytesRestResponse);
        } catch (final IOException e) {
            logger.warn("failed to send bad request response", e);
            channel.sendResponse(
                new org.opensearch.rest.spi.BytesRestResponse(
                    INTERNAL_SERVER_ERROR,
                    org.opensearch.rest.spi.BytesRestResponse.TEXT_CONTENT_TYPE,
                    BytesArray.EMPTY
                )
            );
        }
    }

    /**
     * Handle HTTP OPTIONS requests to a valid REST endpoint. A 200 HTTP
     * response code is returned, and the response 'Allow' header includes a
     * list of valid HTTP methods for the endpoint (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-9.2">HTTP/1.1 - 9.2
     * - Options</a>).
     */
    private void handleOptionsRequest(org.opensearch.rest.spi.RestChannel channel, Set<HttpRequest.Method> validMethodSet) {
        org.opensearch.rest.spi.BytesRestResponse bytesRestResponse = new org.opensearch.rest.spi.BytesRestResponse(
            OK,
            TEXT_CONTENT_TYPE,
            BytesArray.EMPTY
        );
        if (validMethodSet.isEmpty() == false) {
            bytesRestResponse.addHeader("Allow", Strings.collectionToDelimitedString(validMethodSet, ","));
        }
        channel.sendResponse(bytesRestResponse);
    }

    /**
     * Handle requests with no enabled candidate handlers (return a 404 Not Found error).
     */
    private void handleNotFound(String uri, HttpRequest.Method method, org.opensearch.rest.spi.RestChannel channel) throws IOException {
        try (XContentBuilder builder = channel.newErrorBuilder()) {
            builder.startObject();
            {
                try {
                    // Validate input URI to filter out HTML special characters in the error message,
                    // in case false-positive cross site scripting vulnerability is detected by common security scanners.
                    uri = new URI(uri).getPath();
                    builder.field("error", "no handler found for uri [" + uri + "] and method [" + method + "]");
                } catch (Exception e) {
                    builder.field("error", "invalid uri has been requested");
                }
            }
            builder.endObject();
            channel.sendResponse(new org.opensearch.rest.spi.BytesRestResponse(NOT_FOUND, builder));
        }
    }

    /**
     * Get the valid set of HTTP methods for a REST request.
     */
    private Set<HttpRequest.Method> getValidHandlerMethodSet(String rawPath) {
        Set<HttpRequest.Method> validMethods = new HashSet<>();
        Iterator<RestMethodHandlers> allHandlers = getAllRestMethodHandlers(null, rawPath);
        while (allHandlers.hasNext()) {
            final MethodHandlers methodHandlers = allHandlers.next();
            if (methodHandlers != null) {
                validMethods.addAll(methodHandlers.getValidMethods());
            }
        }
        return validMethods;
    }

    private static final class ResourceHandlingHttpChannel implements org.opensearch.rest.spi.RestChannel {
        private final org.opensearch.rest.spi.RestChannel delegate;
        private final CircuitBreakerService circuitBreakerService;
        private final int contentLength;
        private final AtomicBoolean closed = new AtomicBoolean();

        ResourceHandlingHttpChannel(
            org.opensearch.rest.spi.RestChannel delegate,
            CircuitBreakerService circuitBreakerService,
            int contentLength
        ) {
            this.delegate = delegate;
            this.circuitBreakerService = circuitBreakerService;
            this.contentLength = contentLength;
        }

        @Override
        public XContentBuilder newBuilder() throws IOException {
            return delegate.newBuilder();
        }

        @Override
        public XContentBuilder newErrorBuilder() throws IOException {
            return delegate.newErrorBuilder();
        }

        @Override
        public XContentBuilder newBuilder(@Nullable MediaType mediaType, boolean useFiltering) throws IOException {
            return delegate.newBuilder(mediaType, useFiltering);
        }

        @Override
        public XContentBuilder newBuilder(MediaType mediaType, MediaType responseContentType, boolean useFiltering) throws IOException {
            return delegate.newBuilder(mediaType, responseContentType, useFiltering);
        }

        @Override
        public BytesStreamOutput bytesOutput() {
            return delegate.bytesOutput();
        }

        @Override
        public org.opensearch.rest.spi.RestRequest request() {
            return delegate.request();
        }

        @Override
        public boolean detailedErrorsEnabled() {
            return delegate.detailedErrorsEnabled();
        }

        @Override
        public boolean detailedErrorStackTraceEnabled() {
            return delegate.detailedErrorStackTraceEnabled();
        }

        @Override
        public void sendResponse(org.opensearch.rest.spi.RestResponse response) {
            close();
            delegate.sendResponse(response);
        }

        private void close() {
            // attempt to close once atomically
            if (closed.compareAndSet(false, true) == false) {
                throw new IllegalStateException("Channel is already closed");
            }
            inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(-contentLength);
        }
    }

    private static final class StreamHandlingHttpChannel implements org.opensearch.rest.spi.StreamingRestChannel {
        private final org.opensearch.rest.spi.StreamingRestChannel delegate;
        private final CircuitBreakerService circuitBreakerService;
        private final int contentLength;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean subscribed = new AtomicBoolean();

        StreamHandlingHttpChannel(
            org.opensearch.rest.spi.StreamingRestChannel delegate,
            CircuitBreakerService circuitBreakerService,
            int contentLength
        ) {
            this.delegate = delegate;
            this.circuitBreakerService = circuitBreakerService;
            this.contentLength = contentLength;
        }

        @Override
        public XContentBuilder newBuilder() throws IOException {
            return delegate.newBuilder();
        }

        @Override
        public XContentBuilder newErrorBuilder() throws IOException {
            return delegate.newErrorBuilder();
        }

        @Override
        public XContentBuilder newBuilder(@Nullable MediaType mediaType, boolean useFiltering) throws IOException {
            return delegate.newBuilder(mediaType, useFiltering);
        }

        @Override
        public XContentBuilder newBuilder(MediaType mediaType, MediaType responseContentType, boolean useFiltering) throws IOException {
            return delegate.newBuilder(mediaType, responseContentType, useFiltering);
        }

        @Override
        public BytesStreamOutput bytesOutput() {
            return delegate.bytesOutput();
        }

        @Override
        public org.opensearch.rest.spi.RestRequest request() {
            return delegate.request();
        }

        @Override
        public boolean detailedErrorsEnabled() {
            return delegate.detailedErrorsEnabled();
        }

        @Override
        public boolean detailedErrorStackTraceEnabled() {
            return delegate.detailedErrorStackTraceEnabled();
        }

        @Override
        public void sendResponse(org.opensearch.rest.spi.RestResponse response) {
            close();

            // Check if subscribe() is already called, the headers and status are going to be sent
            // over so we need to populate those **before** that, if possible.
            if (subscribed.get() == false) {
                prepareResponse(response.status(), Map.of("Content-Type", List.of(response.contentType())));
            }

            Mono.from(this).ignoreElement().then(Mono.just(response)).subscribe(delegate::sendResponse);
        }

        @Override
        public void sendChunk(HttpChunk chunk) {
            delegate.sendChunk(chunk);
        }

        @Override
        public void prepareResponse(RestStatus status, Map<String, List<String>> headers) {
            delegate.prepareResponse(status, headers);
        }

        @Override
        public void subscribe(Subscriber<? super HttpChunk> subscriber) {
            subscribed.set(true);
            delegate.subscribe(subscriber);
        }

        private void close() {
            // attempt to close once atomically
            if (closed.compareAndSet(false, true) == false) {
                throw new IllegalStateException("Channel is already closed");
            }
            inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(-contentLength);
        }

        @Override
        public boolean isReadable() {
            return delegate.isReadable();
        }

        @Override
        public boolean isWritable() {
            return delegate.isWritable();
        }
    }

    private static CircuitBreaker inFlightRequestsBreaker(CircuitBreakerService circuitBreakerService) {
        // We always obtain a fresh breaker to reflect changes to the breaker configuration.
        return circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }
}
