/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.service;

import org.opensearch.index.api.SnapshotRepositoryProvider;

import java.util.Map;

/** Immutable repository provider discovery result. @opensearch.internal */
public record RepositoryRegistry(Map<String, SnapshotRepositoryProvider> providers) {
    public RepositoryRegistry {
        providers = Map.copyOf(providers);
    }
}
