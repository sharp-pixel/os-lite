/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.spi;

import java.util.Locale;

/**
 * Semantic operation groups, independent of the HTTP method used by an endpoint.
 *
 * @opensearch.api
 */
public enum RestOperationCategory {
    /** Queries, aggregations, and document retrieval. */
    SEARCH,
    /** Document creation, updates, and deletion. */
    INDEXING,
    /** Index definitions, mappings, settings, and service metadata. */
    MANAGEMENT;

    /**
     * Parses a category name used in node settings.
     * @param value a lowercase category name
     * @return the operation category
     * @throws IllegalArgumentException if the name is unknown
     */
    public static RestOperationCategory fromString(String value) {
        for (RestOperationCategory category : values()) {
            if (category.settingValue().equals(value)) {
                return category;
            }
        }
        throw new IllegalArgumentException("unknown REST operation category [" + value + "]; expected search, indexing, or management");
    }

    /** Returns the category name used in node settings. */
    public String settingValue() {
        return name().toLowerCase(Locale.ROOT);
    }
}
