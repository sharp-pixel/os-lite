/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.common.settings.Settings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class HttpServerTransportTests extends RandomizedTest {
    public void testSecuritySensitiveDefaults() {
        assertFalse(HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED.get(Settings.EMPTY));
        assertEquals(30_000L, HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT.get(Settings.EMPTY).millis());
    }

    public void testFallbackReleasesRequestAndClosesChannel() {
        HttpRequest request = mock(HttpRequest.class);
        HttpChannel channel = mock(HttpChannel.class);
        HttpServerTransport.NO_OP_DISPATCHER.dispatchRequest(request, channel, null);
        verify(request).release();
        verify(channel).close();
    }

    public void testFallbackReleasesBadRequestAndClosesChannel() {
        HttpRequest request = mock(HttpRequest.class);
        HttpChannel channel = mock(HttpChannel.class);
        HttpServerTransport.NO_OP_DISPATCHER.dispatchBadRequest(request, channel, null, new IllegalArgumentException("malformed"));
        verify(request).release();
        verify(channel).close();
    }

    public void testFallbackClosesChannelEvenIfReleaseFails() {
        HttpRequest request = mock(HttpRequest.class);
        HttpChannel channel = mock(HttpChannel.class);
        doThrow(new IllegalStateException("release failed")).when(request).release();
        assertThrows(IllegalStateException.class, () -> HttpServerTransport.NO_OP_DISPATCHER.dispatchRequest(request, channel, null));
        verify(channel).close();
    }
}
