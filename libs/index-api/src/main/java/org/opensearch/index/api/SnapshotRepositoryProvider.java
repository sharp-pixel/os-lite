/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.api;

import java.nio.file.Path;

/** Repository factory discovered through the index-service extension mechanism. @opensearch.experimental */
public interface SnapshotRepositoryProvider {
    String id();

    SnapshotRepository open(Path root, boolean writable);
}
