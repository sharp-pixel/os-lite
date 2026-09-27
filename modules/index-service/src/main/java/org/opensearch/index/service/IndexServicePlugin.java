/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.service;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.Scopes;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.engine.api.EngineExtension;
import org.opensearch.index.api.IndexActions;
import org.opensearch.index.api.IndexExtension;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

/** Owns index actions and catalog lifecycle; REST consumers are accepted without registering their routes here. @opensearch.internal */
public final class IndexServicePlugin extends Plugin implements ActionPlugin, EngineExtension, ExtensiblePlugin {
    public static final Setting<String> REPOSITORY = Setting.simpleString("index_service.repository", Setting.Property.NodeScope);
    public static final Setting<String> REPOSITORY_PATH = Setting.simpleString("index_service.repository_path", Setting.Property.NodeScope);
    private final java.util.Map<String, org.opensearch.index.api.SnapshotRepositoryProvider> repositories = new java.util.LinkedHashMap<>();
    public static final Setting<Integer> MAX_INDICES = Setting.intSetting(
        "index_service.max_indices",
        16,
        1,
        1024,
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MAX_INDICES, REPOSITORY, REPOSITORY_PATH);
    }

    @Override
    public void accept(Plugin plugin) {
        if (plugin instanceof IndexExtension == false) throw new IllegalArgumentException(
            "index-service consumers must implement IndexExtension"
        );
        for (var provider : ((IndexExtension) plugin).repositoryProviders()) {
            if (provider.id().matches("[a-z][a-z0-9-]{0,63}") == false) throw new IllegalArgumentException(
                "invalid repository provider ID"
            );
            if (repositories.putIfAbsent(provider.id(), provider) != null) throw new IllegalArgumentException(
                "duplicate repository provider: " + provider.id()
            );
        }
    }

    @Override
    public Collection<Module> createGuiceModules() {
        RepositoryRegistry registry = new RepositoryRegistry(repositories);
        return List.of(binder -> {
            binder.bind(RepositoryRegistry.class).toInstance(registry);
            binder.bind(LocalIndexService.class).in(Scopes.SINGLETON);
        });
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return List.of(LocalIndexService.class);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(IndexActions.CREATE, IndexTransportActions.Create.class),
            new ActionHandler<>(IndexActions.CLAIM, IndexTransportActions.Claim.class),
            new ActionHandler<>(IndexActions.DESCRIBE, IndexTransportActions.Describe.class),
            new ActionHandler<>(IndexActions.PUT, IndexTransportActions.Put.class),
            new ActionHandler<>(IndexActions.DELETE, IndexTransportActions.Delete.class),
            new ActionHandler<>(IndexActions.REFRESH, IndexTransportActions.Refresh.class),
            new ActionHandler<>(IndexActions.GET, IndexTransportActions.Get.class),
            new ActionHandler<>(IndexActions.SEARCH, IndexTransportActions.Search.class)
        );
    }
}
