/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.http.CorsHandler;
import org.opensearch.http.HttpHandlingSettings;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginResources;
import org.opensearch.rest.spi.RestHandler;
import org.opensearch.rest.spi.RestHandlerPlugin;
import org.opensearch.rest.spi.RestOperationCategory;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.Tracer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Plugin that provides the REST layer dispatcher for handling HTTP requests.
 *
 * @opensearch.internal
 */
public class RestPlugin extends Plugin implements NetworkPlugin, ExtensiblePlugin {
    /** Node-local API capabilities; all categories are enabled by default. */
    public static final Setting<List<RestOperationCategory>> API_CATEGORIES = Setting.listSetting(
        "rest.api.categories",
        List.of("search", "indexing", "management"),
        RestOperationCategory::fromString,
        Setting.Property.NodeScope
    );

    private PluginResources pluginResources;
    private final List<RestHandlerPlugin> restHandlerPlugins = new ArrayList<>();

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(API_CATEGORIES);
    }

    @Override
    public Collection<Object> createComponents(PluginResources pluginResources) {
        this.pluginResources = Objects.requireNonNull(pluginResources, "pluginResources must not be null");
        return Collections.emptyList();
    }

    @Override
    public Optional<HttpServerTransport.Dispatcher> getHttpServerTransportDispatcher(
        BigArrays bigArrays,
        Settings settings,
        CircuitBreakerService circuitBreakerService,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        RestController restController = new RestController(
            Set.of(new RestHeaderDefinition(Task.X_OPAQUE_ID, false)),
            UnaryOperator.identity(),
            pluginResources.nodeClient(),
            circuitBreakerService,
            pluginResources.namedXContentRegistry(),
            bigArrays,
            HttpHandlingSettings.fromSettings(settings),
            CorsHandler.fromSettings(settings),
            new RestTracer(settings, clusterSettings),
            tracer,
            Set.copyOf(API_CATEGORIES.get(settings))
        );
        for (RestHandlerPlugin restHandlerPlugin : restHandlerPlugins) {
            for (RestHandler restHandler : restHandlerPlugin.getRestHandlers()) {
                restController.registerHandler(restHandler);
            }
        }
        return Optional.of(restController);
    }

    @Override
    public void accept(Plugin plugin) {
        if (plugin instanceof RestHandlerPlugin == false) {
            throw new IllegalArgumentException("REST extensions must implement RestHandlerPlugin: " + plugin.getClass().getName());
        }
        restHandlerPlugins.add((RestHandlerPlugin) plugin);
    }
}
