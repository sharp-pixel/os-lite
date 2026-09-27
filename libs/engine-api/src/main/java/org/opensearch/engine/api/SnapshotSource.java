/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.io.IOException;
import java.io.InputStream;

/** Pins an exact immutable manifest and its files. Close streams before closing their source. @opensearch.experimental */
public interface SnapshotSource extends AutoCloseable {
    SnapshotManifest manifest();

    InputStream open(String name) throws IOException;

    @Override
    void close();
}
