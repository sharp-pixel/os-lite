/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.Version;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.recycler.Recycler;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.transport.nativeprotocol.NativeOutboundMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class InboundTransportTests extends RandomizedTest {
    public void testPartialRequestsReserveBreakerBytesBeforeFrameCompletion() {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        Header header = new Header(TransportProtocol.NATIVE, 100, 1, (byte) 0, Version.CURRENT);
        header.headers = new Tuple<>(Map.of(), Map.of());
        header.actionName = "test";
        try (
            InboundAggregator aggregator = new InboundAggregator(() -> breaker, (String action) -> true);
            ReleasableBytesReference content = reference(new byte[100])
        ) {
            aggregator.headerReceived(header);
            aggregator.aggregate(content);
            verify(breaker).addEstimateBytesAndMaybeBreak(100, "test");
        }
        verify(breaker).addWithoutBreaking(-100);
    }

    public void testBreakerTripReleasesPartialBodyAndReservation() throws Exception {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        doThrow(new CircuitBreakingException("full", CircuitBreaker.Durability.TRANSIENT)).when(breaker)
            .addEstimateBytesAndMaybeBreak(101, "test");
        Header header = new Header(TransportProtocol.NATIVE, 201, 1, (byte) 0, Version.CURRENT);
        header.headers = new Tuple<>(Map.of(), Map.of());
        header.actionName = "test";
        AtomicInteger releases = new AtomicInteger();
        try (InboundAggregator aggregator = new InboundAggregator(() -> breaker, (String action) -> true)) {
            aggregator.headerReceived(header);
            try (ReleasableBytesReference first = new ReleasableBytesReference(new BytesArray(new byte[100]), releases::incrementAndGet)) {
                aggregator.aggregate(first);
            }
            assertEquals(0, releases.get());
            try (ReleasableBytesReference second = reference(new byte[101])) {
                aggregator.aggregate(second);
            }
            assertEquals(1, releases.get());
            verify(breaker).addWithoutBreaking(-100);
            try (InboundMessage message = aggregator.finishAggregation()) {
                assertTrue(message.isShortCircuit());
            }
        }
        verify(breaker, times(1)).addWithoutBreaking(-100);
    }

    public void testSuccessfulAggregationTransfersReservationUntilMessageClose() throws Exception {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        Header header = new Header(TransportProtocol.NATIVE, 200, 1, (byte) 0, Version.CURRENT);
        header.headers = new Tuple<>(Map.of(), Map.of());
        header.actionName = "test";
        InboundMessage message;
        try (
            InboundAggregator aggregator = new InboundAggregator(() -> breaker, (String action) -> true);
            ReleasableBytesReference content = reference(new byte[100])
        ) {
            aggregator.headerReceived(header);
            aggregator.aggregate(content);
            aggregator.aggregate(content);
            message = aggregator.finishAggregation();
        }
        verify(breaker, times(2)).addEstimateBytesAndMaybeBreak(100, "test");
        try (message) {
            assertEquals(200, message.getContentLength());
        }
        verify(breaker).addWithoutBreaking(-200);
    }

    public void testProductionCompressedResponseSurvivesFragmentedTransport() throws Exception {
        byte[] expected = new byte[PageCacheRecycler.BYTE_PAGE_SIZE * 5 + 3];
        new java.util.Random(42).nextBytes(expected);
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        NativeOutboundMessage message = new NativeOutboundMessage.Response(
            context,
            Set.of(),
            output -> output.writeBytes(expected),
            Version.CURRENT,
            7,
            false,
            true
        );
        byte[] encoded;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            encoded = BytesReference.toBytes(message.serialize(output));
        }
        for (int packetSize : new int[] { 1, 37, encoded.length }) {
            TrackingRecycler recycler = new TrackingRecycler();
            TcpChannel channel = mock(TcpChannel.class);
            when(channel.getChannelStats()).thenReturn(new TcpChannel.ChannelStats());
            AtomicInteger messages = new AtomicInteger();
            try (
                InboundPipeline pipeline = new InboundPipeline(
                    new StatsTracker(),
                    () -> 0,
                    new InboundDecoder(Version.CURRENT, recycler),
                    new InboundAggregator(() -> new NoopCircuitBreaker("test"), (String action) -> true),
                    (source, received) -> {
                        try {
                            assertArrayEquals(expected, received.openOrGetStreamInput().readAllBytes());
                            messages.incrementAndGet();
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                )
            ) {
                for (int offset = 0; offset < encoded.length; offset += packetSize) {
                    try (
                        ReleasableBytesReference packet = reference(
                            Arrays.copyOfRange(encoded, offset, Math.min(encoded.length, offset + packetSize))
                        )
                    ) {
                        pipeline.handleBytes(channel, packet);
                    }
                }
            }
            assertEquals(1, messages.get());
            assertEquals(0, recycler.livePages.get());
        }
    }

    public void testFailedContentInflationDoesNotPolluteNextOperation() throws Exception {
        DeflateCompressor compressor = new DeflateCompressor();
        // A non-final stored block containing X, followed by an invalid block type.
        byte[] bad = { 'D', 'F', 'L', 0, 0, 1, 0, (byte) 0xfe, (byte) 0xff, 'X', 7 };
        assertThrows(IOException.class, () -> compressor.uncompress(new BytesArray(bad)));
        byte[] expected = { 42 };
        assertArrayEquals(expected, BytesReference.toBytes(compressor.uncompress(new BytesArray(compressed(expected)))));
    }

    public void testDecompressedSizeBoundAndExactLimit() throws Exception {
        byte[] input = new byte[1000];
        TrackingRecycler recycler = new TrackingRecycler();
        try (TransportDecompressor decompressor = new TransportDecompressor(recycler, 999)) {
            assertThrows(IOException.class, () -> decompressor.decompress(new BytesArray(compressed(input))));
        }
        assertEquals(0, recycler.livePages.get());
        try (TransportDecompressor decompressor = new TransportDecompressor(recycler, 1000)) {
            byte[] compressed = compressed(input);
            assertEquals(compressed.length, decompressor.decompress(new BytesArray(compressed)));
            try (ReleasableBytesReference page = decompressor.pollDecompressedPage()) {
                assertArrayEquals(input, BytesReference.toBytes(page));
            }
        }
        assertEquals(0, recycler.livePages.get());
    }

    public void testRejectsHeaderLengthsOutsideDeclaredFrame() {
        for (int variable : new int[] { -1, Integer.MAX_VALUE, 100, 1 }) {
            try (
                InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
                ReleasableBytesReference bytes = reference(frame(variable, new byte[] { 42 }, false))
            ) {
                assertThrows(IOException.class, () -> decoder.decode(bytes, ignored -> {}));
            }
        }
    }

    public void testRejectsFrameShorterThanFixedHeader() {
        byte[] frame = frame(2, new byte[0], false);
        ByteBuffer.wrap(frame).putInt(2, 1);
        try (
            InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
            ReleasableBytesReference bytes = reference(frame)
        ) {
            assertThrows(IOException.class, () -> decoder.decode(bytes, ignored -> {}));
        }
    }

    public void testCorruptDeflateReturnsBorrowedPage() {
        TrackingRecycler recycler = new TrackingRecycler();
        try (TransportDecompressor decompressor = new TransportDecompressor(recycler)) {
            // DFL marker followed by a raw DEFLATE block with the reserved BTYPE=3.
            assertThrows(IOException.class, () -> decompressor.decompress(new BytesArray(new byte[] { 'D', 'F', 'L', 0, 7 })));
        }
        assertEquals(0, recycler.livePages.get());
    }

    public void testRejectsTruncatedCompressedBody() throws Exception {
        byte[] compressed = compressed(new byte[100]);
        assertCompressedRejected(Arrays.copyOf(compressed, compressed.length - 1));
    }

    public void testRejectsTrailingCompressedBytes() throws Exception {
        byte[] compressed = compressed(new byte[100]);
        assertCompressedRejected(Arrays.copyOf(compressed, compressed.length + 1));
    }

    public void testRejectsTruncatedCompressionMarker() throws Exception {
        assertCompressedRejected(new byte[] { 'D', 'F' });
    }

    public void testRejectsMissingCompressedBody() {
        try (
            InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
            ReleasableBytesReference bytes = reference(frame(2, new byte[0], true))
        ) {
            assertThrows(IOException.class, () -> decoder.decode(bytes, ignored -> {}));
        }
    }

    private void assertCompressedRejected(byte[] compressed) throws Exception {
        TrackingRecycler recycler = new TrackingRecycler();
        List<Object> fragments = new ArrayList<>();
        try (
            InboundDecoder decoder = new InboundDecoder(Version.CURRENT, recycler);
            ReleasableBytesReference bytes = reference(frame(2, compressed, true))
        ) {
            int header = decoder.decode(bytes, fragments::add);
            try (ReleasableBytesReference body = bytes.retainedSlice(header, bytes.length() - header)) {
                assertThrows(IOException.class, () -> decoder.decode(body, fragments::add));
            }
        } finally {
            closeFragments(fragments);
        }
        assertEquals(0, recycler.livePages.get());
    }

    public void testDecodeFailureDoesNotContaminateNextConnectionOnSameThread() throws Exception {
        TcpChannel channel = mock(TcpChannel.class);
        when(channel.getChannelStats()).thenReturn(new TcpChannel.ChannelStats());
        AtomicInteger messages = new AtomicInteger();
        try (
            InboundPipeline pipeline = pipeline(messages);
            ReleasableBytesReference bytes = reference(frame(2, new byte[] { 'D', 'F', 'L', 0, 7 }, true))
        ) {
            assertThrows(IOException.class, () -> pipeline.handleBytes(channel, bytes));
        }
        try (
            InboundPipeline pipeline = pipeline(messages);
            ReleasableBytesReference bytes = reference(frame(2, new byte[] { 42 }, false))
        ) {
            pipeline.handleBytes(channel, bytes);
        }
        assertEquals(1, messages.get());
    }

    private static InboundPipeline pipeline(AtomicInteger messages) {
        return new InboundPipeline(
            new StatsTracker(),
            () -> 0,
            new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE),
            new InboundAggregator(() -> new NoopCircuitBreaker("test"), (String action) -> true),
            (channel, message) -> messages.incrementAndGet()
        );
    }

    public void testValidIndependentCompressedFrame() throws Exception {
        byte[] expected = new byte[PageCacheRecycler.BYTE_PAGE_SIZE * 3 + 7];
        Arrays.fill(expected, (byte) 42);
        TrackingRecycler recycler = new TrackingRecycler();
        List<Object> fragments = new ArrayList<>();
        try (
            InboundDecoder decoder = new InboundDecoder(Version.CURRENT, recycler);
            ReleasableBytesReference bytes = reference(frame(2, compressed(expected), true))
        ) {
            int header = decoder.decode(bytes, fragments::add);
            try (ReleasableBytesReference body = bytes.retainedSlice(header, bytes.length() - header)) {
                assertEquals(body.length(), decoder.decode(body, fragments::add));
            }
            assertTrue(fragments.get(0) instanceof Header);
            assertEquals(InboundDecoder.END_CONTENT, fragments.getLast());
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            for (Object fragment : fragments) {
                if (fragment instanceof ReleasableBytesReference content) output.write(BytesReference.toBytes(content));
            }
            assertArrayEquals(expected, output.toByteArray());
        } finally {
            closeFragments(fragments);
        }
        assertEquals(0, recycler.livePages.get());
    }

    private static byte[] frame(int declaredVariableHeader, byte[] body, boolean compressed) {
        // Independent native response fixture: ES, frame length, request ID, status, version, header length,
        // then the two empty thread-context maps and the body. No production frame encoder is used.
        return ByteBuffer.allocate(25 + body.length)
            .put((byte) 'E')
            .put((byte) 'S')
            .putInt(19 + body.length)
            .putLong(1)
            .put((byte) (compressed ? 5 : 1))
            .putInt(Version.CURRENT.id)
            .putInt(declaredVariableHeader)
            .put((byte) 0)
            .put((byte) 0)
            .put(body)
            .array();
    }

    private static byte[] compressed(byte[] input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(new byte[] { 'D', 'F', 'L', 0 });
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        try (DeflaterOutputStream compressed = new DeflaterOutputStream(output, deflater)) {
            compressed.write(input);
        } finally {
            deflater.end();
        }
        return output.toByteArray();
    }

    private static ReleasableBytesReference reference(byte[] bytes) {
        return ReleasableBytesReference.wrap(new BytesArray(bytes));
    }

    private static void closeFragments(List<Object> fragments) {
        for (Object fragment : fragments) {
            if (fragment instanceof ReleasableBytesReference content) content.close();
        }
    }

    private static final class TrackingRecycler extends PageCacheRecycler {
        final AtomicInteger livePages = new AtomicInteger();

        TrackingRecycler() {
            super(Settings.builder().put("cache.recycler.page.limit.heap", "0%").build());
        }

        @Override
        public Recycler.V<byte[]> bytePage(boolean clear) {
            livePages.incrementAndGet();
            return new Recycler.V<>() {
                private final byte[] bytes = new byte[BYTE_PAGE_SIZE];
                private boolean closed;

                @Override
                public byte[] v() {
                    return bytes;
                }

                @Override
                public boolean isRecycled() {
                    return false;
                }

                @Override
                public void close() {
                    assertFalse(closed);
                    closed = true;
                    livePages.decrementAndGet();
                }
            };
        }
    }
}
