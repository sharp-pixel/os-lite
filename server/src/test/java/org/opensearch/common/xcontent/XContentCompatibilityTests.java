/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.common.xcontent;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tools.jackson.core.JsonParseException;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class XContentCompatibilityTests extends RandomizedTest {
    public void testFilteredPrettyOutputAcrossLocalCoreAndUpstreamGenerator() throws Exception {
        try (
            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream(), Set.of("kept"), Set.of())
        ) {
            builder.prettyPrint().lfAtEnd().startObject().field("kept", "value").field("hidden", "secret").endObject();
            String json = BytesReference.bytes(builder).utf8ToString();
            assertTrue(json.contains("\n"));
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    json
                )
            ) {
                assertEquals(Map.of("kept", "value"), parser.map());
            }
        }
    }

    public void testMalformedJsonRemainsBadRequestAfterJacksonUpgrade() throws Exception {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                "{\"value\":}"
            )
        ) {
            JsonParseException error = assertThrows(JsonParseException.class, parser::map);
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(error));
        }
    }
}
