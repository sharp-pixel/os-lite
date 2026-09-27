/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

/** A factory; discovery must not allocate shard resources. @opensearch.experimental */
public interface EngineProvider {
    EngineDescriptor descriptor();

    ShardWriter openWriter(ShardSpec shard);

    ShardReader openReader(ShardSpec shard);
}
