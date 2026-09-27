/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.api.search;

import org.opensearch.rest.spi.RestApiPlugin;
import org.opensearch.rest.spi.RestOperationCategory;

/**
 * Extension hub for search REST API handlers.
 *
 * @opensearch.internal
 */
public class SearchApiPlugin extends RestApiPlugin {
    public SearchApiPlugin() {
        super(RestOperationCategory.SEARCH);
    }
}
