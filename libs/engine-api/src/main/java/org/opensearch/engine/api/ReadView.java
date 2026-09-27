/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.Optional;

/** An owned snapshot lease. Returned documents outlive the view. @opensearch.experimental */
public interface ReadView extends AutoCloseable {
    SearchResult search(SearchQuery query, int limit, OperationContext context);

    Optional<EngineDocument> get(String id, OperationContext context);

    Checkpoint checkpoint();

    @Override
    void close();
}
