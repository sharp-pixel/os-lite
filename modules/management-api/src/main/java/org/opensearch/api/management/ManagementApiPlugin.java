/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.api.management;

import org.opensearch.rest.spi.RestApiPlugin;
import org.opensearch.rest.spi.RestOperationCategory;

/**
 * Extension hub for management REST API handlers.
 *
 * @opensearch.internal
 */
public class ManagementApiPlugin extends RestApiPlugin {
    public ManagementApiPlugin() {
        super(RestOperationCategory.MANAGEMENT);
    }
}
