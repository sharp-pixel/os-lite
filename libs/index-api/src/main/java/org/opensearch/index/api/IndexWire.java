/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.api;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.engine.api.Checkpoint;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;
import org.opensearch.engine.api.ShardId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Versioned bounded wire primitives shared by actions and the local catalog. @opensearch.internal */
public final class IndexWire {
    private IndexWire() {}

    public static int count(StreamInput in, int maximum) throws IOException {
        int count = in.readVInt();
        if (count < 0 || count > maximum) throw new IOException("wire count exceeds limit");
        return count;
    }

    public static byte[] bytes(StreamInput in, int maximum) throws IOException {
        byte[] bytes = new byte[count(in, maximum)];
        if (bytes.length != 0) in.readBytes(bytes, 0, bytes.length);
        return bytes;
    }

    public static void bytes(StreamOutput out, byte[] bytes) throws IOException {
        out.writeVInt(bytes.length);
        out.writeBytes(bytes, 0, bytes.length);
    }

    public static String string(StreamInput in, int maximum) throws IOException {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes(in, maximum)))
                .toString();
        } catch (CharacterCodingException e) {
            throw new IOException("invalid UTF-8 on wire", e);
        }
    }

    public static void string(StreamOutput out, String value) throws IOException {
        bytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    public static UUID uuid(StreamInput in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    public static void uuid(StreamOutput out, UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    public static Schema schema(StreamInput in) throws IOException {
        long version = in.readVLong();
        Map<String, Schema.FieldType> fields = new LinkedHashMap<>();
        int count = count(in, 128);
        for (int i = 0; i < count; i++) {
            String name = string(in, 128);
            int type = count(in, 1);
            if (fields.put(name, type == 0 ? Schema.FieldType.KEYWORD : Schema.FieldType.TEXT) != null) throw new IOException(
                "duplicate schema field"
            );
        }
        try {
            return new Schema(version, fields);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid schema", e);
        }
    }

    public static void schema(StreamOutput out, Schema schema) throws IOException {
        out.writeVLong(schema.version());
        out.writeVInt(schema.fields().size());
        for (Map.Entry<String, Schema.FieldType> field : new java.util.TreeMap<>(schema.fields()).entrySet()) {
            string(out, field.getKey());
            out.writeVInt(field.getValue() == Schema.FieldType.KEYWORD ? 0 : 1);
        }
    }

    public static IndexMetadata metadata(StreamInput in) throws IOException {
        try {
            return new IndexMetadata(string(in, 128), uuid(in), string(in, 64), schema(in));
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid index metadata", e);
        }
    }

    public static void metadata(StreamOutput out, IndexMetadata metadata) throws IOException {
        string(out, metadata.name());
        uuid(out, metadata.id());
        string(out, metadata.engine());
        schema(out, metadata.schema());
    }

    public static EngineDocument document(StreamInput in) throws IOException {
        String id = string(in, 1024);
        int count = count(in, 128);
        Map<String, String> fields = new LinkedHashMap<>();
        long estimated = 0;
        for (int i = 0; i < count; i++) {
            String name = string(in, 128);
            // The 1 MiB document budget uses two bytes per UTF-16 code unit. UTF-8 can require three.
            String value = string(in, 3 << 19);
            estimated += 2L * ((long) name.length() + value.length());
            if (estimated > 1 << 20) throw new IOException("document fields exceed limit");
            if (fields.put(name, value) != null) throw new IOException("duplicate document field");
        }
        byte[] source = bytes(in, (int) ((1 << 20) - estimated));
        try {
            return new EngineDocument(id, fields, source);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid document", e);
        }
    }

    public static void document(StreamOutput out, EngineDocument document) throws IOException {
        string(out, document.id());
        out.writeVInt(document.fields().size());
        for (Map.Entry<String, String> field : document.fields().entrySet()) {
            string(out, field.getKey());
            string(out, field.getValue());
        }
        bytes(out, document.source());
    }

    public static Checkpoint checkpoint(StreamInput in) throws IOException {
        try {
            return new Checkpoint(new ShardId(uuid(in), count(in, Integer.MAX_VALUE)), uuid(in), in.readVLong());
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid checkpoint", e);
        }
    }

    public static void checkpoint(StreamOutput out, Checkpoint checkpoint) throws IOException {
        uuid(out, checkpoint.shard().indexId());
        out.writeVInt(checkpoint.shard().shard());
        uuid(out, checkpoint.history());
        out.writeVLong(checkpoint.sequence());
    }

    public static SearchQuery query(StreamInput in) throws IOException {
        return query(in, 0, new int[1]);
    }

    private static SearchQuery query(StreamInput in, int depth, int[] nodes) throws IOException {
        if (depth > 16 || ++nodes[0] > 128) throw new IOException("query exceeds wire limits");
        try {
            return switch (in.readByte()) {
                case 0 -> new SearchQuery.All();
                case 1 -> new SearchQuery.Term(string(in, 128), string(in, 65536));
                case 2 -> new SearchQuery.Match(string(in, 128), string(in, 65536));
                case 3 -> new SearchQuery.Bool(
                    queries(in, depth, nodes),
                    queries(in, depth, nodes),
                    queries(in, depth, nodes),
                    count(in, 128)
                );
                default -> throw new IOException("unknown query wire tag");
            };
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid query", e);
        }
    }

    private static List<SearchQuery> queries(StreamInput in, int depth, int[] nodes) throws IOException {
        int count = count(in, 128);
        List<SearchQuery> queries = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            queries.add(query(in, depth + 1, nodes));
        return queries;
    }

    public static void query(StreamOutput out, SearchQuery query) throws IOException {
        SearchQuery.estimatedBytes(query);
        if (query instanceof SearchQuery.All) {
            out.writeByte((byte) 0);
        } else if (query instanceof SearchQuery.Term term) {
            out.writeByte((byte) 1);
            string(out, term.field());
            string(out, term.value());
        } else if (query instanceof SearchQuery.Match match) {
            out.writeByte((byte) 2);
            string(out, match.field());
            string(out, match.text());
        } else {
            SearchQuery.Bool bool = (SearchQuery.Bool) query;
            out.writeByte((byte) 3);
            queries(out, bool.must());
            queries(out, bool.should());
            queries(out, bool.mustNot());
            out.writeVInt(bool.minimumShouldMatch());
        }
    }

    private static void queries(StreamOutput out, List<SearchQuery> queries) throws IOException {
        out.writeVInt(queries.size());
        for (SearchQuery child : queries)
            query(out, child);
    }
}
