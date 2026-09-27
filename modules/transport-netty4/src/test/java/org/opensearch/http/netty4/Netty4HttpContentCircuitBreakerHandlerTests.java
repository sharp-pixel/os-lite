/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.indices.breaker.CircuitBreakerService;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class Netty4HttpContentCircuitBreakerHandlerTests extends RandomizedTest {
    public void testAccountsContentUntilAggregationCompletes() {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        when(breaker.addEstimateBytesAndMaybeBreak(anyLong(), anyString())).thenReturn(4.0);
        EmbeddedChannel channel = channel(breaker);

        channel.writeInbound(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/"));
        channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[4])));
        channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

        verify(breaker).addEstimateBytesAndMaybeBreak(4, "<http_request_aggregation>");
        verify(breaker).addWithoutBreaking(-4);
        channel.finishAndReleaseAll();
    }

    public void testReleasesReservationWhenChannelClosesMidRequest() {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        when(breaker.addEstimateBytesAndMaybeBreak(anyLong(), anyString())).thenReturn(3.0);
        EmbeddedChannel channel = channel(breaker);

        channel.writeInbound(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/"));
        channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[3])));
        channel.close();

        verify(breaker).addWithoutBreaking(-3);
        channel.finishAndReleaseAll();
    }

    public void testReleasesRejectedContentAndPreviousReservation() {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        when(breaker.addEstimateBytesAndMaybeBreak(anyLong(), anyString())).thenReturn(3.0)
            .thenThrow(new CircuitBreakingException("full", CircuitBreaker.Durability.TRANSIENT));
        EmbeddedChannel channel = channel(breaker);
        channel.writeInbound(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/"));
        channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[3])));
        ByteBuf rejected = Unpooled.wrappedBuffer(new byte[2]);

        assertThrows(CircuitBreakingException.class, () -> channel.writeInbound(new DefaultHttpContent(rejected)));

        assertEquals(0, rejected.refCnt());
        verify(breaker).addWithoutBreaking(-3);
        channel.finishAndReleaseAll();
    }

    public void testClosesRequestThatDoesNotCompleteBeforeDeadline() {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        EmbeddedChannel channel = channel(breaker, 10);
        channel.writeInbound(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/"));

        channel.advanceTimeBy(10, TimeUnit.MILLISECONDS);
        channel.runScheduledPendingTasks();

        assertFalse(channel.isActive());
        channel.finishAndReleaseAll();
    }

    private static EmbeddedChannel channel(CircuitBreaker breaker) {
        return channel(breaker, 30_000);
    }

    private static EmbeddedChannel channel(CircuitBreaker breaker, long requestTimeoutMillis) {
        CircuitBreakerService service = mock(CircuitBreakerService.class);
        when(service.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS)).thenReturn(breaker);
        return new EmbeddedChannel(new Netty4HttpContentCircuitBreakerHandler(service, requestTimeoutMillis));
    }
}
