/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;

import java.io.IOException;

/** Typed plans are compiled here; callers never construct Lucene queries. @opensearch.internal */
final class LuceneQueries {
    private LuceneQueries() {}

    static Query compile(SearchQuery query, Schema schema, Analyzer analyzer) throws IOException {
        return compile(query, schema, analyzer, new int[1]);
    }

    private static Query compile(SearchQuery query, Schema schema, Analyzer analyzer, int[] clauses) throws IOException {
        if (++clauses[0] > 512) throw new EngineException(EngineException.Code.RESOURCE_LIMIT, "expanded query exceeds clause limit");
        if (query instanceof SearchQuery.All) return MatchAllDocsQuery.INSTANCE;
        if (query instanceof SearchQuery.Term term) {
            field(schema, term.field());
            if (new BytesRef(term.value()).length > 32766) throw new EngineException(
                EngineException.Code.INVALID_ARGUMENT,
                "term exceeds byte limit"
            );
            return new TermQuery(new Term(term.field(), term.value()));
        }
        if (query instanceof SearchQuery.Match match) {
            if (field(schema, match.field()) != Schema.FieldType.TEXT) throw new EngineException(
                EngineException.Code.UNSUPPORTED,
                "match requires a text field"
            );
            BooleanQuery.Builder terms = new BooleanQuery.Builder();
            int count = 0;
            try (TokenStream tokens = analyzer.tokenStream(match.field(), match.text())) {
                CharTermAttribute term = tokens.addAttribute(CharTermAttribute.class);
                tokens.reset();
                while (tokens.incrementToken()) {
                    if (++clauses[0] > 512) throw new EngineException(
                        EngineException.Code.RESOURCE_LIMIT,
                        "analyzed query exceeds clause limit"
                    );
                    terms.add(new TermQuery(new Term(match.field(), term.toString())), BooleanClause.Occur.SHOULD);
                    count++;
                }
                tokens.end();
            }
            return count == 0 ? MatchNoDocsQuery.INSTANCE : terms.build();
        }
        SearchQuery.Bool bool = (SearchQuery.Bool) query;
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (bool.must().isEmpty() && bool.should().isEmpty()) builder.add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.FILTER);
        for (SearchQuery child : bool.must())
            builder.add(compile(child, schema, analyzer, clauses), BooleanClause.Occur.MUST);
        for (SearchQuery child : bool.should())
            builder.add(compile(child, schema, analyzer, clauses), BooleanClause.Occur.SHOULD);
        for (SearchQuery child : bool.mustNot())
            builder.add(compile(child, schema, analyzer, clauses), BooleanClause.Occur.MUST_NOT);
        builder.setMinimumNumberShouldMatch(bool.minimumShouldMatch());
        return builder.build();
    }

    private static Schema.FieldType field(Schema schema, String name) {
        Schema.FieldType type = schema.fields().get(name);
        if (type == null) throw new EngineException(EngineException.Code.INVALID_ARGUMENT, "unknown query field: " + name);
        return type;
    }
}
