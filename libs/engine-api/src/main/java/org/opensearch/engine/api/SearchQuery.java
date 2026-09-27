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

/** Typed query tree. Providers validate fields and bound depth and expanded clauses. @opensearch.experimental */
public sealed interface SearchQuery permits SearchQuery.All, SearchQuery.Term, SearchQuery.Match, SearchQuery.Bool {
    static long estimatedBytes(SearchQuery query) {
        Objects.requireNonNull(query);
        return estimate(query, 0, new int[1]);
    }

    private static long estimate(SearchQuery query, int depth, int[] nodes) {
        if (depth > 16 || ++nodes[0] > 128) throw new EngineException(EngineException.Code.RESOURCE_LIMIT, "query tree exceeds limits");
        if (query instanceof Term term) return 2L * (term.field().length() + term.value().length()) + 128;
        if (query instanceof Match match) return 2L * (match.field().length() + match.text().length()) + 128;
        long size = 128;
        if (query instanceof Bool bool) {
            for (SearchQuery child : bool.must())
                size += estimate(child, depth + 1, nodes);
            for (SearchQuery child : bool.should())
                size += estimate(child, depth + 1, nodes);
            for (SearchQuery child : bool.mustNot())
                size += estimate(child, depth + 1, nodes);
        }
        return size;
    }

    private static void checkText(String field, String text) {
        if (Objects.requireNonNull(field).length() > 128 || Objects.requireNonNull(text).length() > 16384)
            throw new IllegalArgumentException("query text exceeds limits");
        EngineDocument.validateUnicode(field, "query field");
        EngineDocument.validateUnicode(text, "query text");
    }

    record All() implements SearchQuery {
    }

    record Term(String field, String value) implements SearchQuery {
        public Term {
            checkText(field, value);
        }
    }

    record Match(String field, String text) implements SearchQuery {
        public Match {
            checkText(field, text);
        }
    }

    record Bool(List<SearchQuery> must, List<SearchQuery> should, List<SearchQuery> mustNot, int minimumShouldMatch)
        implements
            SearchQuery {
        public Bool {
            if ((long) must.size() + should.size() + mustNot.size() > 128 || minimumShouldMatch < 0 || minimumShouldMatch > should.size()) {
                throw new IllegalArgumentException("invalid Boolean query bounds");
            }
            must = List.copyOf(must);
            should = List.copyOf(should);
            mustNot = List.copyOf(mustNot);
        }
    }
}
