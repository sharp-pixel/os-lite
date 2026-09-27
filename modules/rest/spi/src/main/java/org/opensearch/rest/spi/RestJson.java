/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.spi;

import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Bounded strict JSON parsing for API adapters; no concrete REST implementation dependency. @opensearch.api */
public final class RestJson {
    private RestJson() {}

    public static Map<String, Object> object(RestRequest request, int maximumBytes) {
        int length = request.getHttpRequest().content().length();
        if (length == 0 || length > maximumBytes) throw new IllegalArgumentException(
            "JSON body must contain 1 to " + maximumBytes + " bytes"
        );
        if (request.getMediaType() == null || "json".equals(request.getMediaType().format()) == false) throw new IllegalArgumentException(
            "Content-Type must be application/json"
        );
        try (
            XContentParser parser = request.getMediaType()
                .xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    request.getHttpRequest().content().streamInput()
                )
        ) {
            parser.nextToken();
            Object value = value(parser, 0, new int[1]);
            if (parser.nextToken() != null) throw new IllegalArgumentException("trailing JSON content");
            return object(value, "request body");
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid JSON body", e);
        }
    }

    private static Object value(XContentParser parser, int depth, int[] values) throws IOException {
        if (depth > 24 || ++values[0] > 8192) throw new IllegalArgumentException("JSON complexity limit exceeded");
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            Map<String, Object> object = new LinkedHashMap<>();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() != XContentParser.Token.FIELD_NAME) throw new IllegalArgumentException("expected JSON field");
                String name = parser.currentName();
                if (object.containsKey(name)) throw new IllegalArgumentException("duplicate JSON field: " + name);
                parser.nextToken();
                object.put(name, value(parser, depth + 1, values));
            }
            return object;
        }
        if (token == XContentParser.Token.START_ARRAY) {
            List<Object> array = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY)
                array.add(value(parser, depth + 1, values));
            return array;
        }
        if (token == XContentParser.Token.VALUE_STRING) return parser.text();
        if (token == XContentParser.Token.VALUE_NUMBER) return parser.numberValue();
        if (token == XContentParser.Token.VALUE_BOOLEAN) return parser.booleanValue();
        if (token == XContentParser.Token.VALUE_NULL) return null;
        throw new IllegalArgumentException("unexpected end or token in JSON body");
    }

    public static Map<String, Object> object(Object value, String name) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() instanceof String key == false) throw new IllegalArgumentException(name + " must contain string keys");
                result.put((String) entry.getKey(), entry.getValue());
            }
            return result;
        }
        throw new IllegalArgumentException(name + " must be an object");
    }

    public static String string(Object value, String name) {
        if (value instanceof String text) return text;
        throw new IllegalArgumentException(name + " must be a string");
    }

    public static int integer(Object value, String name, int minimum, int maximum) {
        if (value instanceof Integer || value instanceof Long) {
            long number = ((Number) value).longValue();
            if (number >= minimum && number <= maximum) return (int) number;
        }
        throw new IllegalArgumentException(name + " must be an integer from " + minimum + " to " + maximum);
    }

    public static void fields(Map<String, Object> object, Set<String> allowed) {
        for (String name : object.keySet())
            if (allowed.contains(name) == false) throw new IllegalArgumentException("unsupported field: " + name);
    }

    public static void noBody(RestRequest request) {
        if (request.getHttpRequest().content().length() != 0) throw new IllegalArgumentException("this operation does not accept a body");
    }
}
