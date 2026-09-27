/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

/** Accounts decoded HTTP content while Netty is aggregating a request. */
final class Netty4HttpContentCircuitBreakerHandler extends ChannelInboundHandlerAdapter {
    private final CircuitBreakerService circuitBreakerService;
    private final long requestTimeoutMillis;
    private CircuitBreaker activeBreaker;
    private ScheduledFuture<?> requestTimeoutFuture;
    private long reservedBytes;
    private boolean requestInProgress;

    Netty4HttpContentCircuitBreakerHandler(CircuitBreakerService circuitBreakerService, long requestTimeoutMillis) {
        this.circuitBreakerService = circuitBreakerService;
        this.requestTimeoutMillis = requestTimeoutMillis;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
        if (message instanceof HttpRequest) {
            if (requestInProgress) {
                ReferenceCountUtil.release(message);
                releaseReservation();
                cancelRequestTimeout();
                requestInProgress = false;
                throw new IllegalStateException("received a new HTTP request before the previous request body completed");
            }
            requestInProgress = true;
            if (requestTimeoutMillis > 0) {
                requestTimeoutFuture = context.executor().schedule(() -> context.close(), requestTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        }

        if (message instanceof HttpContent content) {
            final int readableBytes = content.content().readableBytes();
            if (readableBytes > 0) {
                if (activeBreaker == null) {
                    activeBreaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
                }
                try {
                    activeBreaker.addEstimateBytesAndMaybeBreak(readableBytes, "<http_request_aggregation>");
                    reservedBytes += readableBytes;
                } catch (RuntimeException exception) {
                    ReferenceCountUtil.release(message);
                    releaseReservation();
                    requestInProgress = false;
                    cancelRequestTimeout();
                    throw exception;
                }
            }
        }

        if (message instanceof LastHttpContent) {
            try {
                context.fireChannelRead(message);
            } finally {
                releaseReservation();
                requestInProgress = false;
                cancelRequestTimeout();
            }
        } else {
            context.fireChannelRead(message);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        releaseReservation();
        requestInProgress = false;
        cancelRequestTimeout();
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext context) throws Exception {
        releaseReservation();
        requestInProgress = false;
        cancelRequestTimeout();
        super.handlerRemoved(context);
    }

    private void releaseReservation() {
        if (reservedBytes != 0) {
            activeBreaker.addWithoutBreaking(-reservedBytes);
            reservedBytes = 0;
        }
        activeBreaker = null;
    }

    private void cancelRequestTimeout() {
        if (requestTimeoutFuture != null) {
            FutureUtils.cancel(requestTimeoutFuture);
            requestTimeoutFuture = null;
        }
    }
}
