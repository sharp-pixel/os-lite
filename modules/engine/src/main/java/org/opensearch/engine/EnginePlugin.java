/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine;

import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.engine.api.EngineDescriptor;
import org.opensearch.engine.api.EngineExtension;
import org.opensearch.engine.api.EngineProvider;
import org.opensearch.engine.api.EngineService;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginResources;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Provider discovery and a single lifecycle owner for engine services. @opensearch.internal */
public final class EnginePlugin extends Plugin implements ExtensiblePlugin {
    public static final Setting<Integer> WORKERS = Setting.intSetting("engine.workers", 2, 1, 32, Setting.Property.NodeScope);
    public static final Setting<Integer> QUEUE = Setting.intSetting("engine.queue_capacity", 64, 1, 8192, Setting.Property.NodeScope);
    public static final Setting<Integer> SHARDS = Setting.intSetting("engine.max_open_shards", 16, 1, 1024, Setting.Property.NodeScope);
    public static final Setting<Long> PENDING_BYTES = Setting.longSetting(
        "engine.max_pending_bytes",
        64L << 20,
        1L << 20,
        1L << 30,
        Setting.Property.NodeScope
    );
    public static final Setting<List<String>> ROLES = Setting.listSetting("engine.roles", List.of("reader", "writer"), value -> {
        if (value.equals("reader") == false && value.equals("writer") == false) throw new IllegalArgumentException(
            "unknown engine role: " + value
        );
        return value;
    }, Setting.Property.NodeScope);

    private final Settings settings;
    private final Map<String, EngineProvider> providers = new LinkedHashMap<>();
    private EngineRuntime runtime;

    public EnginePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(WORKERS, QUEUE, SHARDS, PENDING_BYTES, ROLES);
    }

    @Override
    public void accept(Plugin plugin) {
        if (runtime != null) throw new IllegalStateException("engine discovery has finished");
        if (plugin instanceof EngineExtension == false) throw new IllegalArgumentException(
            "engine extensions must implement EngineExtension"
        );
        for (EngineProvider provider : ((EngineExtension) plugin).engineProviders()) {
            EngineDescriptor descriptor = Objects.requireNonNull(provider.descriptor());
            if (descriptor.apiVersion() != EngineDescriptor.CURRENT_API_VERSION) {
                throw new IllegalArgumentException("unsupported engine API version for " + descriptor.id());
            }
            if (providers.putIfAbsent(descriptor.id(), provider) != null) throw new IllegalArgumentException(
                "duplicate engine provider: " + descriptor.id()
            );
        }
    }

    @Override
    public Collection<Object> createComponents(PluginResources resources) {
        if (runtime != null) throw new IllegalStateException("engine components already created");
        runtime = new EngineRuntime(providers, resources.environment().dataFiles()[0].resolve("engines"), settings);
        return List.of(runtime);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        EngineService service = Objects.requireNonNull(runtime, "createComponents must run before engine bindings");
        return List.of(binder -> binder.bind(EngineService.class).toInstance(service));
    }
}
