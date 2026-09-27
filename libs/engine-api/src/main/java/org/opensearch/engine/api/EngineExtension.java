/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.List;

/** Provider and consumer extension contract exported by the engine plugin. @opensearch.experimental */
public interface EngineExtension {
    default List<EngineProvider> engineProviders() {
        return List.of();
    }
}
