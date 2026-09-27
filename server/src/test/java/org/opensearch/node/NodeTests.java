/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginResources;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class NodeTests extends RandomizedTest {
    private Environment environment;

    @Before
    public void setup() throws Exception {
        TestPlugin.latest = null;
        TestPlugin.failHttp = false;
        TestPlugin.failModules = false;
        TestPlugin.failInjector = false;
        TestPlugin.failCleanup = false;
        Settings settings = Settings.builder()
            .put("path.home", newTempDir().toString())
            .put("path.data", newTempDir().toString())
            .put("node.name", "lifecycle-test")
            .put("transport.type.default", "test")
            .put("http.type.default", "test")
            .build();
        environment = new Environment(settings, null);
    }

    @After
    public void cleanup() throws Exception {
        if (TestPlugin.latest != null && TestPlugin.latest.threadPool != null) {
            assertTrue(ThreadPool.terminate(TestPlugin.latest.threadPool, 5, TimeUnit.SECONDS));
        }
    }

    public void testSuccessfulConstructionKeepsComponentsOpenUntilNodeClose() throws Exception {
        try (Node node = new Node(environment, List.of(pluginInfo()), true)) {
            verify(TestPlugin.latest.component, never()).close();
            assertFalse(TestPlugin.latest.threadPool.scheduler().isShutdown());
        }
        verify(TestPlugin.latest.component).close();
        assertTrue(TestPlugin.latest.threadPool.scheduler().isShutdown());
        assertTrue(TestPlugin.latest.closedWithPoolRunning);
    }

    public void testConstructionFailureShutsDownThreadPoolAndClosesAllocatedTransport() throws Exception {
        TestPlugin.failHttp = true;
        OpenSearchException failure = assertThrows(OpenSearchException.class, () -> new Node(environment, List.of(pluginInfo()), true));
        assertTrue(failure.getCause().getMessage().contains("HTTP creation failed"));
        assertTrue(TestPlugin.latest.threadPool.scheduler().isShutdown());
        verify(TestPlugin.latest.transport).close();
        verify(TestPlugin.latest.component).close();
        assertTrue(TestPlugin.latest.closed);
    }

    public void testFailureBeforeInjectorCreationClosesReturnedComponents() throws Exception {
        TestPlugin.failModules = true;
        OpenSearchException failure = assertThrows(OpenSearchException.class, () -> new Node(environment, List.of(pluginInfo()), true));
        assertTrue(failure.getCause().getMessage().contains("module creation failed"));
        verify(TestPlugin.latest.component).close();
        assertTrue(TestPlugin.latest.threadPool.scheduler().isShutdown());
        assertTrue(TestPlugin.latest.closed);
    }

    public void testInjectorFailureClosesHttpAndTransportExactlyOnce() throws Exception {
        TestPlugin.failInjector = true;
        OpenSearchException failure = assertThrows(OpenSearchException.class, () -> new Node(environment, List.of(pluginInfo()), true));
        assertTrue(failure.getCause().getMessage().contains("injector creation failed"));
        verify(TestPlugin.latest.httpTransport).close();
        verify(TestPlugin.latest.transport).close();
        verify(TestPlugin.latest.component).close();
        assertTrue(TestPlugin.latest.closedWithPoolRunning);
        assertTrue(TestPlugin.latest.threadPool.scheduler().isShutdown());
    }

    public void testCleanupFailurePreservesConstructionErrorAndStillShutsDownPool() throws Exception {
        TestPlugin.failHttp = true;
        TestPlugin.failCleanup = true;
        OpenSearchException failure = assertThrows(OpenSearchException.class, () -> new Node(environment, List.of(pluginInfo()), true));
        assertTrue(failure.getCause().getMessage().contains("HTTP creation failed"));
        verify(TestPlugin.latest.component).close();
        assertTrue(TestPlugin.latest.closedWithPoolRunning);
        assertTrue(TestPlugin.latest.threadPool.scheduler().isShutdown());
    }

    public void testStartupFailureClosesAllocatedServices() throws Exception {
        Node node = new Node(environment, List.of(pluginInfo()), true);
        IllegalStateException failure = new IllegalStateException("component startup failed");
        doThrow(failure).when(TestPlugin.latest.component).start();
        try {
            assertSame(failure, assertThrows(IllegalStateException.class, node::start));
            verify(TestPlugin.latest.component).close();
            verify(TestPlugin.latest.httpTransport).close();
            verify(TestPlugin.latest.transport).close();
            assertTrue(TestPlugin.latest.closed);
            assertTrue(TestPlugin.latest.threadPool.scheduler().isShutdown());
        } finally {
            node.close();
        }
    }

    public void testHttpStartupFailurePreservesErrorAndClosesServices() throws Exception {
        Node node = new Node(environment, List.of(pluginInfo()), true);
        IllegalStateException failure = new IllegalStateException("HTTP startup failed");
        doThrow(failure).when(TestPlugin.latest.httpTransport).start();
        doThrow(new IllegalStateException("HTTP stop failed")).when(TestPlugin.latest.httpTransport).stop();
        try {
            assertSame(failure, assertThrows(IllegalStateException.class, node::start));
            verify(TestPlugin.latest.component).stop();
            verify(TestPlugin.latest.component).close();
            verify(TestPlugin.latest.transport).close();
            verify(TestPlugin.latest.httpTransport).close();
            assertTrue(TestPlugin.latest.threadPool.scheduler().isShutdown());
            assertTrue(failure.getSuppressed().length > 0);
        } finally {
            node.close();
        }
    }

    public void testStopFailureStillClosesEveryService() throws Exception {
        Node node = new Node(environment, List.of(pluginInfo()), true);
        node.start();
        IllegalStateException failure = new IllegalStateException("HTTP stop failed");
        doThrow(failure).when(TestPlugin.latest.httpTransport).stop();
        try {
            assertSame(failure, assertThrows(IllegalStateException.class, node::close));
            verify(TestPlugin.latest.transport).stop();
            verify(TestPlugin.latest.component).stop();
            verify(TestPlugin.latest.httpTransport).close();
            verify(TestPlugin.latest.transport).close();
            verify(TestPlugin.latest.component).close();
            assertTrue(TestPlugin.latest.closedWithPoolRunning);
            assertTrue(TestPlugin.latest.threadPool.scheduler().isShutdown());
        } finally {
            node.close();
        }
        verify(TestPlugin.latest.component).close();
    }

    private static PluginInfo pluginInfo() {
        return new PluginInfo("test", "test plugin", "1", Version.CURRENT, "25", TestPlugin.class.getName(), null, List.of(), false);
    }

    public static class TestPlugin extends Plugin implements NetworkPlugin {
        static TestPlugin latest;
        static boolean failHttp;
        static boolean failModules;
        static boolean failInjector;
        static boolean failCleanup;

        final LifecycleComponent component = mock(LifecycleComponent.class);
        final Transport transport = mock(Transport.class);
        final HttpServerTransport httpTransport = mock(HttpServerTransport.class);
        ThreadPool threadPool;
        boolean closed;
        boolean closedWithPoolRunning;

        public TestPlugin() throws IOException {
            latest = this;
            TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
            when(transport.boundAddress()).thenReturn(new BoundTransportAddress(new TransportAddress[] { address }, address));
            when(transport.getResponseHandlers()).thenReturn(new Transport.ResponseHandlers());
            if (failCleanup) {
                doThrow(new IllegalStateException("component cleanup failed")).when(component).close();
            }
        }

        @Override
        public Collection<Object> createComponents(PluginResources resources) {
            threadPool = resources.threadPool();
            return List.of(component);
        }

        @Override
        public Collection<org.opensearch.common.inject.Module> createGuiceModules() {
            if (failModules) {
                throw new IllegalStateException("module creation failed");
            }
            if (failInjector) {
                return List.of(
                    binder -> binder.bind(String.class)
                        .toProvider(() -> { throw new IllegalStateException("injector creation failed"); })
                        .asEagerSingleton()
                );
            }
            return List.of();
        }

        @Override
        public Map<String, Supplier<Transport>> getTransports(
            Settings settings,
            ThreadPool pool,
            PageCacheRecycler recycler,
            CircuitBreakerService breakers,
            NamedWriteableRegistry registry,
            NetworkService network,
            Tracer tracer
        ) {
            return Map.of("test", () -> transport);
        }

        @Override
        public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
            Settings settings,
            ThreadPool pool,
            BigArrays arrays,
            PageCacheRecycler recycler,
            CircuitBreakerService breakers,
            NamedXContentRegistry registry,
            NetworkService network,
            HttpServerTransport.Dispatcher dispatcher,
            ClusterSettings clusterSettings,
            Tracer tracer
        ) {
            return Map.of("test", () -> {
                if (failHttp) {
                    throw new IllegalStateException("HTTP creation failed");
                }
                return httpTransport;
            });
        }

        @Override
        public void close() {
            closed = true;
            closedWithPoolRunning = threadPool != null && threadPool.scheduler().isShutdown() == false;
            if (failCleanup) {
                throw new IllegalStateException("plugin cleanup failed");
            }
        }
    }
}
