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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Build;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionModule;
import org.opensearch.bootstrap.BootstrapCheck;
import org.opensearch.bootstrap.BootstrapContext;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.Randomness;
import org.opensearch.common.SetOnce;
import org.opensearch.common.StopWatch;
import org.opensearch.common.UUIDs;
import org.opensearch.common.breaker.BreakerSettings;
import org.opensearch.common.breaker.HierarchyCircuitBreakerService;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.Key;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.SettingUpgrader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.util.Platform;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.CircuitBreakerPlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginResources;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SecureSettingsFactory;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellationService;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.RunnableTaskExecutionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Node implements Closeable {
    private final LocalNodeFactory localNodeFactory;

    private static class LocalNodeFactory implements Function<BoundTransportAddress, DiscoveryNode> {
        private final SetOnce<DiscoveryNode> localNode = new SetOnce<>();
        private final String persistentNodeId;
        private final Settings settings;

        private LocalNodeFactory(Settings settings, String persistentNodeId) {
            this.persistentNodeId = persistentNodeId;
            this.settings = settings;
        }

        @Override
        public DiscoveryNode apply(BoundTransportAddress boundTransportAddress) {
            final DiscoveryNode discoveryNode = DiscoveryNode.createLocal(
                settings,
                boundTransportAddress.publishAddress(),
                persistentNodeId
            );

            localNode.set(discoveryNode);
            return localNode.get();
        }

        DiscoveryNode getNode() {
            assert localNode.get() != null;
            return localNode.get();
        }
    }

    /**
     * controls whether the node is allowed to persist things like metadata to disk
     */
    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING = Setting.boolSetting(
        "node.local_storage",
        true,
        Setting.Property.Deprecated,
        Setting.Property.NodeScope
    );

    private final Logger logger = LogManager.getLogger(Node.class);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Node.class);
    private final Lifecycle lifecycle = new Lifecycle();
    private final Collection<LifecycleComponent> pluginLifecycleComponents;
    private final PluginsService pluginsService;
    private final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener;
    private final Environment environment;
    private final NodeClient client;
    private final Injector injector;

    public Node(Environment environment) {
        this(environment, Collections.emptyList(), true);
    }

    protected Node(final Environment initialEnvironment, Collection<PluginInfo> classpathPlugins, boolean forbidPrivateIndexSettings) {
        final List<Closeable> resourcesToClose = new ArrayList<>();
        boolean success = false;
        try {
            Settings tmpSettings = Settings.builder().put(initialEnvironment.settings()).build();

            final JvmInfo jvmInfo = JvmInfo.jvmInfo();
            logger.info(
                "version[{}], pid[{}], build[{}/{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]",
                Build.CURRENT.getQualifiedVersion(),
                jvmInfo.pid(),
                Build.CURRENT.type().displayName(),
                Build.CURRENT.hash(),
                Build.CURRENT.date(),
                Platform.OS_NAME,
                Platform.OS_VERSION,
                Platform.OS_ARCH,
                Platform.JVM_VENDOR,
                Platform.JVM_NAME,
                System.getProperty("java.version"),
                Runtime.version().toString()
            );
            if (jvmInfo.getBundledJdk()) {
                logger.info("JVM home [{}], using bundled JDK/JRE [{}]", System.getProperty("java.home"), jvmInfo.getUsingBundledJdk());
            } else {
                logger.info("JVM home [{}]", System.getProperty("java.home"));
                deprecationLogger.deprecate(
                    "no-jdk",
                    "no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release"
                );
            }
            logger.info("JVM arguments {}", Arrays.toString(jvmInfo.getInputArguments()));
            if (Build.CURRENT.isProductionRelease() == false) {
                logger.warn(
                    "version [{}] is a pre-release version of OpenSearch and is not suitable for production",
                    Build.CURRENT.getQualifiedVersion()
                );
            }

            this.pluginsService = new PluginsService(
                tmpSettings,
                initialEnvironment.configDir(),
                initialEnvironment.modulesDir(),
                initialEnvironment.pluginsDir(),
                classpathPlugins
            );
            resourcesToClose.addAll(pluginsService.filterPlugins(Plugin.class));
            final Settings settings = pluginsService.updatedSettings();
            this.environment = new Environment(settings, initialEnvironment.configDir(), Node.NODE_LOCAL_STORAGE_SETTING.get(settings));

            final List<ExecutorBuilder<?>> executorBuilders = pluginsService.getExecutorBuilders(settings);
            runnableTaskListener = new AtomicReference<>();
            final ThreadPool threadPool = new ThreadPool(settings, runnableTaskListener, executorBuilders.toArray(new ExecutorBuilder[0]));
            // Keep the pool alive until all other resources have been closed on construction failure.
            resourcesToClose.add(0, threadPool::shutdownNow);
            String nodeId = generateNodeId(settings);
            localNodeFactory = new LocalNodeFactory(settings, nodeId);
            final Set<SettingUpgrader<?>> settingsUpgraders = pluginsService.filterPlugins(Plugin.class)
                .stream()
                .map(Plugin::getSettingUpgraders)
                .flatMap(List::stream)
                .collect(Collectors.toSet());
            final List<Setting<?>> additionalSettings = new ArrayList<>(pluginsService.getPluginSettings());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            client = new NodeClient(settings, threadPool);
            final List<String> additionalSettingsFilter = new ArrayList<>(pluginsService.getPluginSettingsFilter());
            final SettingsModule settingsModule = new SettingsModule(
                settings,
                additionalSettings,
                additionalSettingsFilter,
                settingsUpgraders
            );
            threadPool.registerClusterSettingsListeners(settingsModule.getClusterSettings());
            final TaskResourceTrackingService taskResourceTrackingService = new TaskResourceTrackingService(
                settings,
                settingsModule.getClusterSettings(),
                threadPool
            );
            final NetworkService networkService = new NetworkService(
                getCustomNameResolvers(pluginsService.filterPlugins(NetworkPlugin.class))
            );

            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(
                pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.getNamedWriteables().stream())
            ).flatMap(Function.identity()).collect(Collectors.toList());
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
                Stream.of(pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.getNamedXContent().stream()))
                    .flatMap(Function.identity())
                    .collect(toList())
            );

            List<LifecycleComponent> pluginLifecycleComponents = new ArrayList<>();
            PluginResources pluginResources = new PluginResources(
                xContentRegistry,
                namedWriteableRegistry,
                environment,
                threadPool,
                client
            );
            for (Plugin plugin : pluginsService.filterPlugins(Plugin.class)) {
                for (Object component : plugin.createComponents(pluginResources)) {
                    if (component instanceof LifecycleComponent lifecycleComponent) {
                        pluginLifecycleComponents.add(lifecycleComponent);
                        resourcesToClose.add(lifecycleComponent);
                    }
                }
            }
            ModulesBuilder modules = new ModulesBuilder();
            // plugin modules must be added here, before others or we can get crazy injection errors...
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            List<BreakerSettings> pluginCircuitBreakers = pluginsService.filterPlugins(CircuitBreakerPlugin.class)
                .stream()
                .map(plugin -> plugin.getCircuitBreaker(settings))
                .collect(Collectors.toList());
            final CircuitBreakerService circuitBreakerService = createCircuitBreakerService(
                settingsModule.getSettings(),
                pluginCircuitBreakers,
                settingsModule.getClusterSettings()
            );
            resourcesToClose.add(circuitBreakerService);
            PageCacheRecycler pageCacheRecycler = createPageCacheRecycler(settings);
            BigArrays bigArrays = createBigArrays(pageCacheRecycler, circuitBreakerService);

            ActionModule actionModule = new ActionModule(pluginsService.filterPlugins(ActionPlugin.class));
            modules.add(actionModule);

            final Tracer tracer = NoopTracer.INSTANCE;
            final Collection<SecureSettingsFactory> secureSettingsFactories = pluginsService.filterPlugins(Plugin.class)
                .stream()
                .map(p -> p.getSecureSettingFactory(settings))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
            final NetworkModule networkModule = new NetworkModule(
                settings,
                pluginsService.filterPlugins(NetworkPlugin.class),
                threadPool,
                bigArrays,
                pageCacheRecycler,
                circuitBreakerService,
                namedWriteableRegistry,
                xContentRegistry,
                networkService,
                settingsModule.getClusterSettings(),
                tracer,
                Collections.emptyList(),
                secureSettingsFactories
            );
            final Transport transport = networkModule.getTransportSupplier().get();
            resourcesToClose.add(transport);
            final Supplier<Transport> streamTransportSupplier = networkModule.getStreamTransportSupplier();
            final Transport streamTransport = (streamTransportSupplier != null ? streamTransportSupplier.get() : null);
            if (streamTransport != null && streamTransport != transport) {
                resourcesToClose.add(streamTransport);
            }
            Set<String> taskHeaders = Stream.concat(
                pluginsService.filterPlugins(ActionPlugin.class).stream().flatMap(p -> p.getTaskHeaders().stream()),
                Stream.of(Task.X_OPAQUE_ID)
            ).collect(Collectors.toSet());
            final TransportService transportService = newTransportService(
                settings,
                transport,
                streamTransport,
                threadPool,
                networkModule.getTransportInterceptor(),
                localNodeFactory,
                settingsModule.getClusterSettings(),
                taskHeaders,
                tracer
            );
            // TransportService now owns the primary transport.
            resourcesToClose.remove(transport);
            resourcesToClose.add(transportService);
            final HttpServerTransport httpServerTransport = newHttpTransport(networkModule);
            resourcesToClose.add(httpServerTransport);
            modules.add(b -> {
                b.bind(Node.class).toInstance(this);
                b.bind(NamedXContentRegistry.class).toInstance(xContentRegistry);
                b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                b.bind(PluginsService.class).toInstance(pluginsService);
                b.bind(NodeClient.class).toInstance(client);
                b.bind(ThreadPool.class).toInstance(threadPool);
                b.bind(Environment.class).toInstance(environment);
                b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                b.bind(Tracer.class).toInstance(tracer);
                b.bind(Transport.class).toInstance(transport);
                b.bind(TransportService.class).toInstance(transportService);
                b.bind(NetworkService.class).toInstance(networkService);
                b.bind(BigArrays.class).toInstance(bigArrays);
                b.bind(PageCacheRecycler.class).toInstance(pageCacheRecycler);
                b.bind(HttpServerTransport.class).toInstance(httpServerTransport);
                b.bind(TaskResourceTrackingService.class).toInstance(taskResourceTrackingService);
            });

            injector = modules.createInjector();
            for (Class<? extends LifecycleComponent> serviceClass : pluginsService.getGuiceServiceClasses()) {
                LifecycleComponent service = injector.getInstance(serviceClass);
                pluginLifecycleComponents.add(service);
                resourcesToClose.add(service);
            }
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);
            ActionModule.DynamicActionRegistry dynamicActionRegistry = actionModule.getDynamicActionRegistry();
            dynamicActionRegistry.registerUnmodifiableActionMap(injector.getInstance(new Key<>() {
            }));
            client.initialize(dynamicActionRegistry, () -> nodeId, namedWriteableRegistry);
            logger.info("initialized");
            success = true;
        } catch (Exception ex) {
            throw new OpenSearchException("failed to bind service", ex);
        } finally {
            if (!success) {
                Collections.reverse(resourcesToClose);
                IOUtils.closeWhileHandlingException(resourcesToClose);
            }
        }
    }

    public static final Setting<String> BREAKER_TYPE_KEY = new Setting<>("indices.breaker.type", "hierarchy", (s) -> switch (s) {
        case "hierarchy", "none" -> s;
        default -> throw new IllegalArgumentException("indices.breaker.type must be one of [hierarchy, none] but was: " + s);
    }, Setting.Property.NodeScope);

    /**
     * Creates a new {@link CircuitBreakerService} based on the settings provided.
     * @see #BREAKER_TYPE_KEY
     */
    public static CircuitBreakerService createCircuitBreakerService(
        Settings settings,
        List<BreakerSettings> breakerSettings,
        ClusterSettings clusterSettings
    ) {
        String type = BREAKER_TYPE_KEY.get(settings);
        if (type.equals("hierarchy")) {
            return new HierarchyCircuitBreakerService(settings, breakerSettings, clusterSettings);
        } else if (type.equals("none")) {
            return new NoneCircuitBreakerService();
        } else {
            throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
        }
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    PageCacheRecycler createPageCacheRecycler(Settings settings) {
        return new PageCacheRecycler(settings);
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    BigArrays createBigArrays(PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService) {
        return new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
    }

    /**
     * The settings that are used by this node. Contains original settings as well as additional settings provided by plugins.
     */
    public Settings settings() {
        return this.environment.settings();
    }

    /**
     * Get Custom Name Resolvers list based on a Discovery Plugins list
     * @param networkPlugins Discovery plugins list
     */
    private List<NetworkService.CustomNameResolver> getCustomNameResolvers(List<NetworkPlugin> networkPlugins) {
        List<NetworkService.CustomNameResolver> customNameResolvers = new ArrayList<>();
        for (NetworkPlugin networkPlugin : networkPlugins) {
            NetworkService.CustomNameResolver customNameResolver = networkPlugin.getCustomNameResolver(settings());
            if (customNameResolver != null) {
                customNameResolvers.add(customNameResolver);
            }
        }
        return customNameResolvers;
    }

    protected TransportService newTransportService(
        Settings settings,
        Transport transport,
        @Nullable Transport streamTransport,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        ClusterSettings clusterSettings,
        Set<String> taskHeaders,
        Tracer tracer
    ) {
        return new TransportService(
            settings,
            transport,
            streamTransport,
            threadPool,
            interceptor,
            localNodeFactory,
            clusterSettings,
            taskHeaders,
            tracer
        );
    }

    /** Constructs a {@link org.opensearch.http.HttpServerTransport} which may be mocked for tests. */
    protected HttpServerTransport newHttpTransport(NetworkModule networkModule) {
        return networkModule.getHttpServerTransportSupplier().get();
    }

    @Override
    public synchronized void close() throws IOException {
        List<Closeable> toClose = new ArrayList<>();
        synchronized (lifecycle) {
            if (lifecycle.started()) {
                lifecycle.moveToStopped();
                logger.info("stopping ...");
                // Stop failures must not prevent the remaining services from stopping and closing.
                toClose.add(injector.getInstance(HttpServerTransport.class)::stop);
                toClose.add(injector.getInstance(TransportService.class)::stop);
                for (LifecycleComponent plugin : pluginLifecycleComponents) {
                    toClose.add(plugin::stop);
                }
            }
            if (!lifecycle.moveToClosed()) {
                return;
            }
        }
        logger.info("closing ...");
        StopWatch stopWatch = new StopWatch("node_close");
        toClose.add(() -> stopWatch.start("http"));
        toClose.add(injector.getInstance(HttpServerTransport.class));
        toClose.add(() -> stopWatch.stop().start("transport"));
        toClose.add(injector.getInstance(TransportService.class));
        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Plugin.class));
        toClose.add(injector.getInstance(CircuitBreakerService.class));
        toClose.add(() -> stopWatch.stop().start("thread_pool"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdown());
        toClose.add(stopWatch::stop);
        if (logger.isTraceEnabled()) {
            toClose.add(() -> logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint()));
        }
        IOUtils.close(toClose);
        logger.info("closed");
    }

    /**
     * Seed for determining a persisted unique uuid of this node. If the node has already a persisted uuid on disk,
     * this seed will be ignored and the uuid from disk will be reused.
     */
    public static final Setting<Long> NODE_ID_SEED_SETTING = Setting.longSetting(
        "node.id.seed",
        0L,
        Long.MIN_VALUE,
        Setting.Property.NodeScope
    );

    public static String generateNodeId(Settings settings) {
        Random random = NODE_ID_SEED_SETTING.exists(settings) ? new Random(NODE_ID_SEED_SETTING.get(settings)) : Randomness.get();
        return UUIDs.randomBase64UUID(random);
    }

    /**
     * Start the node. If the node is already started, this method is no-op.
     */
    public synchronized void start() throws NodeValidationException {
        if (!lifecycle.moveToStarted()) {
            return;
        }

        try {
            logger.info("starting ...");
            pluginLifecycleComponents.forEach(LifecycleComponent::start);
            TransportService transportService = injector.getInstance(TransportService.class);
            transportService.getTaskManager().setTaskCancellationService(new TaskCancellationService(transportService));

            TaskResourceTrackingService taskResourceTrackingService = injector.getInstance(TaskResourceTrackingService.class);
            transportService.getTaskManager().setTaskResourceTrackingService(taskResourceTrackingService);
            runnableTaskListener.set(taskResourceTrackingService);
            transportService.start();
            assert localNodeFactory.getNode() != null;
            assert transportService.getLocalNode().equals(localNodeFactory.getNode())
                : "transportService has a different local node than the factory provided";
            validateNodeBeforeAcceptingRequests(
                new BootstrapContext(environment),
                transportService.boundAddress(),
                pluginsService.filterPlugins(Plugin.class)
                    .stream()
                    .flatMap(p -> p.getBootstrapChecks().stream())
                    .collect(Collectors.toList())
            );
            transportService.acceptIncomingRequests();
            injector.getInstance(HttpServerTransport.class).start();
            logger.info("started");
        } catch (NodeValidationException | RuntimeException | Error failure) {
            try {
                close();
            } catch (IOException | RuntimeException | Error cleanupFailure) {
                if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /**
     * Wait for this node to be effectively closed.
     */
    // synchronized to prevent running concurrently with close()
    public synchronized boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (lifecycle.closed() == false) {
            // We don't want to shutdown the threadpool or interrupt threads on a node that is not
            // closed yet.
            throw new IllegalStateException("Call close() first");
        }

        ThreadPool threadPool = injector.getInstance(ThreadPool.class);
        final boolean terminated = ThreadPool.terminate(threadPool, timeout, timeUnit);
        /*
        if (terminated) {
            // All threads terminated successfully. Because search, recovery and all other operations
            // that run on shards run in the threadpool, indices should be effectively closed by now.
            if (nodeService.awaitClose(0, TimeUnit.MILLISECONDS) == false) {
                throw new IllegalStateException(
                        "Some shards are still open after the threadpool terminated. "
                                + "Something is leaking index readers or store references."
                );
            }
        }
         */
        return terminated;
    }

    /**
     * Hook for validating the node after network
     * services are started but before the cluster service is started
     * and before the network service starts accepting incoming network
     * requests.
     *
     * @param context               the bootstrap context for this node
     * @param boundTransportAddress the network addresses the node is
     *                              bound and publishing to
     */
    @SuppressWarnings("unused")
    protected void validateNodeBeforeAcceptingRequests(
        final BootstrapContext context,
        final BoundTransportAddress boundTransportAddress,
        List<BootstrapCheck> bootstrapChecks
    ) throws NodeValidationException {}
}
