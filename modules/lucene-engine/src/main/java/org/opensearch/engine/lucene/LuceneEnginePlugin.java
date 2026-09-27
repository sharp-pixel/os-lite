/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.lucene;

import org.opensearch.engine.api.EngineExtension;
import org.opensearch.engine.api.EngineProvider;
import org.opensearch.plugins.Plugin;

import java.util.List;

/** Local committed Lucene backend. @opensearch.internal */
public final class LuceneEnginePlugin extends Plugin implements EngineExtension {
    @Override
    public List<EngineProvider> engineProviders() {
        return List.of(new LuceneEngineProvider());
    }
}
