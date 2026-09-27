/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

/** A committed reader with no mutation operations. @opensearch.experimental */
public interface ShardReader extends AutoCloseable {
    ReadView acquireView();

    Checkpoint refresh(OperationContext context);

    @Override
    void close();
}
