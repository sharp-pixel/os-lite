/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.List;
import java.util.Objects;

/** Owned hits from one committed view, with explicit total-hit accuracy. @opensearch.experimental */
public record SearchResult(List<Hit> hits, long totalHits, boolean exactTotal, Checkpoint checkpoint) {
    public SearchResult {
        hits = List.copyOf(hits);
        Objects.requireNonNull(checkpoint);
    }

    public record Hit(EngineDocument document, float score) {
        public Hit {
            Objects.requireNonNull(document);
        }
    }
}
