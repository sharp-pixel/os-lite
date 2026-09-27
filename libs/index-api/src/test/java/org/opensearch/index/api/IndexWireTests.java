/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.api;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.engine.api.Checkpoint;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;
import org.opensearch.engine.api.SearchResult;
import org.opensearch.engine.api.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class IndexWireTests extends RandomizedTest {
    public void testIndependentDescribeFixtureIncludesParentTaskAndVersion() throws Exception {
        // Empty parent-task node ID, protocol version 2, UTF-8 length 5, then index name.
        byte[] fixture = { 0, 2, 5, 'b', 'o', 'o', 'k', 's' };
        try (StreamInput input = StreamInput.wrap(fixture)) {
            IndexRequest.Describe request = new IndexRequest.Describe(input);
            assertEquals(TaskId.EMPTY_TASK_ID, request.getParentTask());
            assertEquals("books", request.index());
            assertEquals(-1, input.read());
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new IndexRequest.Describe("books").writeTo(out);
            assertArrayEquals(fixture, BytesReference.toBytes(out.bytes()));
        }
    }

    public void testTruncationVersionAndOversizedLengthsAreRejected() throws Exception {
        byte[] fixture = { 0, 2, 5, 'b', 'o', 'o', 'k', 's' };
        for (int i = 0; i < fixture.length; i++) {
            try (StreamInput input = StreamInput.wrap(Arrays.copyOf(fixture, i))) {
                assertThrows(IOException.class, () -> new IndexRequest.Describe(input));
            }
        }
        for (byte[] malformed : List.of(new byte[] { 0, 1, 0 }, new byte[] { 0, 2, (byte) 129, 1 }, new byte[] { 0, 2, 1, (byte) 0xff })) {
            try (StreamInput input = StreamInput.wrap(malformed)) {
                assertThrows(IOException.class, () -> new IndexRequest.Describe(input));
            }
        }
    }

    public void testQueryUsesIndependentTagsAndBounds() throws Exception {
        try (StreamInput input = StreamInput.wrap(new byte[] { 1, 3, 't', 'a', 'g', 1, 'x' })) {
            assertEquals(new SearchQuery.Term("tag", "x"), IndexWire.query(input));
        }
        try (StreamInput input = StreamInput.wrap(new byte[] { 99 })) {
            assertThrows(IOException.class, () -> IndexWire.query(input));
        }
        try (StreamInput input = StreamInput.wrap(new byte[] { 3, (byte) 129, 1 })) {
            assertThrows(IOException.class, () -> IndexWire.query(input));
        }
        SearchQuery query = new SearchQuery.Bool(
            List.of(new SearchQuery.Match("title", "hello")),
            List.of(),
            List.of(new SearchQuery.Term("tag", "private")),
            0
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexWire.query(out, query);
            try (StreamInput input = out.bytes().streamInput()) {
                assertEquals(query, IndexWire.query(input));
            }
        }
    }

    public void testDuplicateSchemaFieldsAreRejected() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVLong(1);
            out.writeVInt(2);
            for (int i = 0; i < 2; i++) {
                IndexWire.string(out, "tag");
                out.writeVInt(0);
            }
            try (StreamInput input = out.bytes().streamInput()) {
                assertThrows(IOException.class, () -> IndexWire.schema(input));
            }
        }
    }

    public void testOwnedDocumentAndSearchResponseSurviveWireRoundtrip() throws Exception {
        EngineDocument document = new EngineDocument("1", Map.of("title", "hello"), new byte[] { 1, 2 });
        IndexRequest.Put request = new IndexRequest.Put("books", document);
        request.setParentTask("node", 42);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput input = out.bytes().streamInput()) {
                IndexRequest.Put copy = new IndexRequest.Put(input);
                assertEquals(request.getParentTask(), copy.getParentTask());
                assertEquals(document.fields(), copy.document().fields());
                assertArrayEquals(document.source(), copy.document().source());
            }
        }
        Checkpoint checkpoint = new Checkpoint(new ShardId(UUID.randomUUID(), 0), UUID.randomUUID(), 7);
        IndexResponse.Search response = new IndexResponse.Search(
            "books",
            new SearchResult(List.of(new SearchResult.Hit(document, 1.5f)), 1, true, checkpoint)
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput input = out.bytes().streamInput()) {
                IndexResponse.Search copy = new IndexResponse.Search(input);
                assertEquals(checkpoint, copy.result().checkpoint());
                assertEquals(1.5f, copy.result().hits().get(0).score(), 0);
                assertArrayEquals(document.source(), copy.result().hits().get(0).document().source());
            }
        }
    }

    public void testMaximumUnicodeDocumentFitsWireEncoding() throws Exception {
        // The document budget counts UTF-16 bytes; a BMP character needs three UTF-8 bytes on the wire.
        EngineDocument document = new EngineDocument("1", Map.of("title", "\u4e2d".repeat((1 << 19) - 5)), new byte[0]);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexWire.document(out, document);
            try (StreamInput input = out.bytes().streamInput()) {
                assertEquals(document.fields(), IndexWire.document(input).fields());
                assertEquals(-1, input.read());
            }
        }
    }

    public void testCreateMetadataUsesDeterministicSchemaEncoding() throws Exception {
        IndexRequest.Create request = new IndexRequest.Create(
            "books",
            "lucene",
            new Schema(1, Map.of("title", Schema.FieldType.TEXT, "tag", Schema.FieldType.KEYWORD))
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput input = out.bytes().streamInput()) {
                IndexRequest.Create copy = new IndexRequest.Create(input);
                assertEquals(request.schema(), copy.schema());
                assertEquals("lucene", copy.engine());
            }
        }
    }

    public void testCancellationReachesOperationContext() {
        IndexRequest.IndexTask task = (IndexRequest.IndexTask) new IndexRequest.Get("books", "1").createTask(
            1,
            "local",
            "get",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        task.cancel("client disconnected");
        assertTrue(task.context().cancelled());
        assertEquals(EngineException.Code.CANCELLED, assertThrows(EngineException.class, task.context()::check).code());
    }

    public void testCheckpointWaitAndWriterEpochSurviveActionWire() throws Exception {
        Checkpoint checkpoint = new Checkpoint(new ShardId(UUID.randomUUID(), 0), UUID.randomUUID(), 7);
        IndexRequest.Search request = new IndexRequest.Search("books", new SearchQuery.All(), 10, checkpoint, 123);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                IndexRequest.Search copy = new IndexRequest.Search(input);
                assertEquals(checkpoint, copy.minimum());
                assertEquals(123, copy.waitMillis());
            }
        }
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            new IndexRequest.Claim("books", 42).writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                assertEquals(42, new IndexRequest.Claim(input).expectedEpoch());
            }
        }
        assertThrows(IllegalArgumentException.class, () -> new IndexRequest.Search("books", new SearchQuery.All(), 10, null, 1));
        assertThrows(IllegalArgumentException.class, () -> new IndexRequest.Claim("books", 0));
    }

    public void testRepositoryWireRejectsIndependentBadMagicVersionAndTruncation() throws Exception {
        for (byte[] bytes : List.of(
            new byte[] { 0, 0, 0, 0, 1 },
            new byte[] { 'O', 'S', 'R', 'P', 2 },
            new byte[] { 'O', 'S', 'R', 'P', 1 }
        )) {
            try (StreamInput input = StreamInput.wrap(bytes)) {
                assertThrows(IOException.class, () -> SnapshotWire.publication(input));
            }
        }
        IndexMetadata metadata = new IndexMetadata(
            "books",
            UUID.randomUUID(),
            "lucene",
            new Schema(1, Map.of("title", Schema.FieldType.TEXT))
        );
        var manifest = new org.opensearch.engine.api.SnapshotManifest(
            "lucene",
            "lucene-1",
            metadata.schema(),
            new Checkpoint(metadata.shard(), UUID.randomUUID(), 0),
            List.of(new org.opensearch.engine.api.SnapshotManifest.File("data", 0, "0".repeat(64)))
        );
        var publication = new SnapshotRepository.Publication(metadata, 1, UUID.randomUUID(), UUID.randomUUID(), manifest);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            SnapshotWire.publication(output, publication);
            byte[] bytes = BytesReference.toBytes(output.bytes());
            for (int i = 0; i < bytes.length; i++) {
                try (StreamInput input = StreamInput.wrap(Arrays.copyOf(bytes, i))) {
                    assertThrows(IOException.class, () -> SnapshotWire.publication(input));
                }
            }
            try (StreamInput input = StreamInput.wrap(bytes)) {
                assertEquals(publication, SnapshotWire.publication(input));
            }
        }
    }

    public void testIndexNamesCannotEscapeCatalog() {
        for (String name : List.of("../index", "a/b", "_all", "*", "UPPER", "", "."))
            assertThrows(IllegalArgumentException.class, () -> new IndexRequest.Describe(name));
    }
}
