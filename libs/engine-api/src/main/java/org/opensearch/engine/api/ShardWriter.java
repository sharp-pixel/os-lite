/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.List;

/** Blocking writer owned by the runtime; implementations serialize mutations. @opensearch.experimental */
public interface ShardWriter extends AutoCloseable {
    WriteResult write(List<Mutation> mutations, OperationContext context);

    Checkpoint checkpoint();

    @Override
    void close();
}
