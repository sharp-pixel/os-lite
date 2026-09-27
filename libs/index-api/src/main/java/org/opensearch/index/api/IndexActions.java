/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.api;

import org.opensearch.action.ActionType;

/** Backend-independent transport action identities. @opensearch.experimental */
public final class IndexActions {
    private IndexActions() {}

    public static final ActionType<IndexResponse.Metadata> CREATE = new ActionType<>(
        "indices:admin/os_lite/create",
        IndexResponse.Metadata::new
    );
    public static final ActionType<IndexResponse.Metadata> DESCRIBE = new ActionType<>(
        "indices:admin/os_lite/get",
        IndexResponse.Metadata::new
    );
    public static final ActionType<IndexResponse.Mutation> PUT = new ActionType<>(
        "indices:data/write/os_lite/index",
        IndexResponse.Mutation::new
    );
    public static final ActionType<IndexResponse.Mutation> DELETE = new ActionType<>(
        "indices:data/write/os_lite/delete",
        IndexResponse.Mutation::new
    );
    public static final ActionType<IndexResponse.Refreshed> REFRESH = new ActionType<>(
        "indices:admin/os_lite/refresh",
        IndexResponse.Refreshed::new
    );
    public static final ActionType<IndexResponse.Document> GET = new ActionType<>(
        "indices:data/read/os_lite/get",
        IndexResponse.Document::new
    );
    public static final ActionType<IndexResponse.Search> SEARCH = new ActionType<>(
        "indices:data/read/os_lite/search",
        IndexResponse.Search::new
    );
}
