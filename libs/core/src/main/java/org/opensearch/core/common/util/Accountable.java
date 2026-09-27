/*
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.core.common.util;

import java.util.Collection;
import java.util.List;

/** Approximate retained-memory accounting independent of a search engine. @opensearch.internal */
public interface Accountable {
    long ramBytesUsed();

    default Collection<Accountable> getChildResources() {
        return List.of();
    }
}
