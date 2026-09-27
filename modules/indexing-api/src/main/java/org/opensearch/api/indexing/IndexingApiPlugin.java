/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.api.indexing;

import org.opensearch.rest.spi.RestApiPlugin;
import org.opensearch.rest.spi.RestOperationCategory;

/**
 * Extension hub for indexing REST API handlers.
 *
 * @opensearch.internal
 */
public class IndexingApiPlugin extends RestApiPlugin {
    public IndexingApiPlugin() {
        super(RestOperationCategory.INDEXING);
    }
}
