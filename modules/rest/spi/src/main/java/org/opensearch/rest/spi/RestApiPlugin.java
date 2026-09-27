/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.spi;

import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Extension hub for an API category. Feature plugins extend one hub instead of the REST dispatcher.
 *
 * @opensearch.api
 */
public abstract class RestApiPlugin extends Plugin implements RestHandlerPlugin, ExtensiblePlugin {
    private final RestOperationCategory category;
    private final List<RestHandlerPlugin> extensions = new ArrayList<>();

    protected RestApiPlugin(RestOperationCategory category) {
        this.category = Objects.requireNonNull(category);
    }

    /** Returns the category served by this API plugin. */
    public final RestOperationCategory operationCategory() {
        return category;
    }

    @Override
    public final void accept(Plugin plugin) {
        if (plugin instanceof RestHandlerPlugin == false) {
            throw new IllegalArgumentException("API extensions must implement RestHandlerPlugin: " + plugin.getClass().getName());
        }
        extensions.add((RestHandlerPlugin) plugin);
    }

    @Override
    public final List<RestHandler> getRestHandlers() {
        List<RestHandler> handlers = new ArrayList<>();
        for (RestHandlerPlugin extension : extensions) {
            for (RestHandler handler : extension.getRestHandlers()) {
                if (handler.operationCategory() != category) {
                    throw new IllegalArgumentException(
                        "handler [" + handler + "] belongs to [" + handler.operationCategory() + "] but extends the [" + category + "] API"
                    );
                }
                handlers.add(handler);
            }
        }
        return List.copyOf(handlers);
    }
}
