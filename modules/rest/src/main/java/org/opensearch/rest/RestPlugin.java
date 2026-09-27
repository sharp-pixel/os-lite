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
import org.opensearch.rest.spi.RestHeaderDefinition;
import org.opensearch.rest.spi.RestOperationCategory;
import org.opensearch.rest.spi.RestSecurityExtension;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.Tracer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
    private final List<RestSecurityExtension> securityExtensions = new ArrayList<>();

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
        final Map<String, RestHeaderDefinition> headersByName = new LinkedHashMap<>();
        addHeaderDefinition(headersByName, new RestHeaderDefinition(Task.X_OPAQUE_ID, false));
        for (RestSecurityExtension extension : securityExtensions) {
            for (RestHeaderDefinition header : extension.getRestHeaders()) {
                addHeaderDefinition(headersByName, Objects.requireNonNull(header, "REST header definition must not be null"));
            }
        }
        final Set<RestHeaderDefinition> headersToCopy = Set.copyOf(headersByName.values());
        UnaryOperator<RestHandler> handlerWrapper = null;
        for (RestSecurityExtension extension : securityExtensions) {
            UnaryOperator<RestHandler> candidate = extension.getRestHandlerWrapper(
                Objects.requireNonNull(pluginResources.threadPool(), "thread pool is required by REST security extensions")
                    .getThreadContext(),
                headersToCopy
            );
            if (candidate != null) {
                if (handlerWrapper != null) {
                    throw new IllegalArgumentException("more than one REST security extension provided a handler wrapper");
                }
                handlerWrapper = candidate;
            }
        }
        RestController restController = new RestController(
            headersToCopy,
            handlerWrapper,
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
        boolean accepted = false;
        if (plugin instanceof RestHandlerPlugin restHandlerPlugin) {
            restHandlerPlugins.add(restHandlerPlugin);
            accepted = true;
        }
        if (plugin instanceof RestSecurityExtension securityExtension) {
            securityExtensions.add(securityExtension);
            accepted = true;
        }
        if (accepted == false) {
            throw new IllegalArgumentException(
                "REST extensions must implement RestHandlerPlugin or RestSecurityExtension: " + plugin.getClass().getName()
            );
        }
    }

    private static void addHeaderDefinition(Map<String, RestHeaderDefinition> headersByName, RestHeaderDefinition header) {
        final String normalizedName = header.getName().toLowerCase(Locale.ROOT);
        RestHeaderDefinition existing = headersByName.putIfAbsent(normalizedName, header);
        if (existing != null && existing.isMultiValueAllowed() != header.isMultiValueAllowed()) {
            throw new IllegalArgumentException("conflicting REST header definitions for [" + header.getName() + "]");
        }
    }
}
