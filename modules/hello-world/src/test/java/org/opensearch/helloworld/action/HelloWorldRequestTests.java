/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.helloworld.action;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class HelloWorldRequestTests extends RandomizedTest {
    public void testReadsEmptyParentTaskFromWire() throws IOException {
        try (StreamInput input = StreamInput.wrap(new byte[] { 0 })) {
            HelloWorldRequest request = new HelloWorldRequest(input);
            assertEquals(TaskId.EMPTY_TASK_ID, request.getParentTask());
            assertEquals(-1, input.read());
        }
    }

    public void testReadsParentTaskFromIndependentWireFixture() throws IOException {
        // TaskId consists of a length-prefixed node ID followed by an eight-byte task ID.
        byte[] wire = { 4, 'n', 'o', 'd', 'e', 0, 0, 0, 0, 0, 0, 0, 42 };
        try (StreamInput input = StreamInput.wrap(wire)) {
            HelloWorldRequest request = new HelloWorldRequest(input);
            assertEquals(new TaskId("node", 42), request.getParentTask());
            assertEquals(-1, input.read());
        }
    }

    public void testRejectsTruncatedParentTask() throws IOException {
        try (StreamInput input = StreamInput.wrap(new byte[] { 4, 'n', 'o' })) {
            assertThrows(IOException.class, () -> new HelloWorldRequest(input));
        }
    }
}
