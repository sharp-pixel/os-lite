/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.opensearch.common.inject.Guice;
import org.opensearch.common.settings.Settings;
import org.opensearch.engine.api.EngineDescriptor;
import org.opensearch.engine.api.EngineDocument;
import org.opensearch.engine.api.EngineException;
import org.opensearch.engine.api.EngineExtension;
import org.opensearch.engine.api.EngineProvider;
import org.opensearch.engine.api.EngineService;
import org.opensearch.engine.api.Mutation;
import org.opensearch.engine.api.OperationContext;
import org.opensearch.engine.api.Schema;
import org.opensearch.engine.api.SearchQuery;
import org.opensearch.engine.api.ShardId;
import org.opensearch.engine.api.ShardReader;
import org.opensearch.engine.api.ShardWriter;
import org.opensearch.env.Environment;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginResources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class EngineRuntimeTests extends RandomizedTest {
    private static final Schema SCHEMA = new Schema(1, Map.of("title", Schema.FieldType.TEXT));

    public void testRegistryValidationAndSharedLifecycleBinding() throws Exception {
        EngineProvider provider = provider("test");
        EnginePlugin plugin = new EnginePlugin(Settings.EMPTY);
        plugin.accept(new Extension(provider));
        assertThrows(IllegalArgumentException.class, () -> plugin.accept(new Extension(provider)));
        EngineProvider incompatible = provider("other");
        when(incompatible.descriptor()).thenReturn(new EngineDescriptor("other", 2, Set.of(Schema.FieldType.TEXT)));
        assertThrows(IllegalArgumentException.class, () -> plugin.accept(new Extension(incompatible)));
        assertThrows(IllegalArgumentException.class, () -> plugin.accept(new Plugin() {
        }));
        plugin.accept(new Extension());
        Settings settings = Settings.builder().put("path.home", newTempDir().toString()).build();
        Environment environment = new Environment(settings, null);
        EngineRuntime runtime = (EngineRuntime) plugin.createComponents(new PluginResources(null, null, environment, null, null))
            .iterator()
            .next();
        try (runtime) {
            assertSame(runtime, Guice.createInjector(plugin.createGuiceModules()).getInstance(EngineService.class));
            assertEquals(Set.of("test"), runtime.providers());
            verify(provider, never()).openWriter(any());
            verify(provider, never()).openReader(any());
            assertThrows(IllegalStateException.class, () -> plugin.accept(new Extension()));
        }
    }

    public void testLifecycleAdmissionAndExactlyOnceCleanup() throws Exception {
        EngineProvider provider = provider("test");
        ShardWriter writer = mock(ShardWriter.class);
        ShardReader reader = mock(ShardReader.class);
        when(provider.openWriter(any())).thenReturn(writer);
        when(provider.openReader(any())).thenReturn(reader);
        EngineRuntime runtime = runtime(provider, Settings.EMPTY);
        ShardId id = id();
        failure(EngineException.Code.CLOSED, runtime.openShard("test", id, SCHEMA, EngineService.Mode.READ_WRITE, context()));
        runtime.start();
        await(runtime.openShard("test", id, SCHEMA, EngineService.Mode.READ_WRITE, context()));
        failure(EngineException.Code.CONFLICT, runtime.openShard("test", id, SCHEMA, EngineService.Mode.READ_WRITE, context()));
        runtime.close();
        runtime.close();
        verify(writer).close();
        verify(reader).close();
        failure(EngineException.Code.CLOSED, runtime.refresh(id, context()));
    }

    public void testFailedReaderOpenClosesWriterAndAllowsRecovery() throws Exception {
        EngineProvider provider = provider("test");
        ShardWriter writer = mock(ShardWriter.class);
        when(provider.openWriter(any())).thenReturn(writer);
        when(provider.openReader(any())).thenThrow(new EngineException(EngineException.Code.IO_ERROR, "open failed"));
        try (EngineRuntime runtime = runtime(provider, Settings.EMPTY)) {
            runtime.start();
            failure(EngineException.Code.IO_ERROR, runtime.openShard("test", id(), SCHEMA, EngineService.Mode.READ_WRITE, context()));
        }
        verify(writer).close();
    }

    public void testCloseFailureDoesNotLeakOtherResources() throws Exception {
        EngineProvider provider = provider("test");
        ShardWriter writer = mock(ShardWriter.class);
        ShardReader reader = mock(ShardReader.class);
        when(provider.openWriter(any())).thenReturn(writer);
        when(provider.openReader(any())).thenReturn(reader);
        doThrow(new EngineException(EngineException.Code.IO_ERROR, "close failed")).when(reader).close();
        EngineRuntime runtime = runtime(provider, Settings.EMPTY);
        runtime.start();
        await(runtime.openShard("test", id(), SCHEMA, EngineService.Mode.READ_WRITE, context()));
        assertThrows(UncheckedIOException.class, runtime::close);
        verify(writer).close();
        verify(reader).close();
    }

    public void testRolesAndProviderSelectionAreEnforcedBelowRest() throws Exception {
        EngineProvider provider = provider("test");
        Settings settings = Settings.builder().putList("engine.roles", "reader").build();
        try (EngineRuntime runtime = runtime(provider, settings)) {
            assertTrue(runtime.supportsMode(EngineService.Mode.READ_ONLY));
            assertFalse(runtime.supportsMode(EngineService.Mode.WRITE_ONLY));
            assertFalse(runtime.supportsMode(EngineService.Mode.READ_WRITE));
            runtime.start();
            failure(EngineException.Code.UNSUPPORTED, runtime.openShard("test", id(), SCHEMA, EngineService.Mode.READ_WRITE, context()));
            failure(EngineException.Code.UNSUPPORTED, runtime.openShard("missing", id(), SCHEMA, EngineService.Mode.READ_ONLY, context()));
        }
        verify(provider, never()).openWriter(any());
        verify(provider, never()).openReader(any());
    }

    public void testReadOnlyShardNeverOpensWriter() throws Exception {
        EngineProvider provider = provider("test");
        when(provider.openReader(any())).thenReturn(mock(ShardReader.class));
        Path root = newTempDir();
        ShardId id = id();
        Path directory = Files.createDirectories(root.resolve(id.indexId().toString()).resolve("0"));
        Files.writeString(directory.resolve("engine.id"), "test");
        try (EngineRuntime runtime = new EngineRuntime(Map.of("test", provider), root, Settings.EMPTY)) {
            runtime.start();
            await(runtime.openShard("test", id, SCHEMA, EngineService.Mode.READ_ONLY, context()));
            failure(EngineException.Code.UNSUPPORTED, runtime.write(id, List.of(new Mutation.Delete("1")), context()));
        }
        verify(provider, never()).openWriter(any());
    }

    public void testPersistedProviderCannotBeReinterpreted() throws Exception {
        EngineProvider provider = provider("test");
        Path root = newTempDir();
        ShardId id = id();
        Path directory = Files.createDirectories(root.resolve(id.indexId().toString()).resolve("0"));
        Files.writeString(directory.resolve("engine.id"), "different");
        try (EngineRuntime runtime = new EngineRuntime(Map.of("test", provider), root, Settings.EMPTY)) {
            runtime.start();
            failure(EngineException.Code.INCOMPATIBLE, runtime.openShard("test", id, SCHEMA, EngineService.Mode.READ_WRITE, context()));
        }
        assertEquals("different", Files.readString(directory.resolve("engine.id")));
        verify(provider, never()).openWriter(any());
    }

    public void testQueueRejectionCancellationAndDrain() throws Exception {
        EngineProvider provider = provider("test");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ShardWriter writer = mock(ShardWriter.class);
        when(provider.openWriter(any())).thenAnswer(invocation -> {
            entered.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            return writer;
        });
        Settings settings = Settings.builder().put("engine.workers", 1).put("engine.queue_capacity", 1).build();
        try (EngineRuntime runtime = runtime(provider, settings)) {
            runtime.start();
            CompletionStage<Void> first = runtime.openShard("test", id(), SCHEMA, EngineService.Mode.WRITE_ONLY, context());
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                OperationContext cancelled = context();
                CompletionStage<Void> queued = runtime.openShard("test", id(), SCHEMA, EngineService.Mode.WRITE_ONLY, cancelled);
                failure(
                    EngineException.Code.RESOURCE_LIMIT,
                    runtime.openShard("test", id(), SCHEMA, EngineService.Mode.WRITE_ONLY, context())
                );
                cancelled.cancel();
                runtime.stop();
                release.countDown();
                await(first);
                failure(EngineException.Code.CANCELLED, queued);
            } finally {
                release.countDown();
            }
        }
        verify(provider).openWriter(any());
        verify(writer).close();
    }

    public void testPendingByteBudgetIsReleasedAfterRejectedWork() throws Exception {
        EngineProvider provider = provider("test");
        ShardWriter writer = mock(ShardWriter.class);
        when(provider.openWriter(any())).thenReturn(writer);
        Settings settings = Settings.builder().put("engine.max_pending_bytes", 1 << 20).build();
        try (EngineRuntime runtime = runtime(provider, settings)) {
            runtime.start();
            ShardId id = id();
            await(runtime.openShard("test", id, SCHEMA, EngineService.Mode.WRITE_ONLY, context()));
            EngineDocument document = new EngineDocument("1", Map.of(), new byte[600000]);
            failure(
                EngineException.Code.RESOURCE_LIMIT,
                runtime.write(id, List.of(new Mutation.Put(document), new Mutation.Put(document)), context())
            );
            await(runtime.write(id, List.of(new Mutation.Delete("1")), context()));
            verify(writer).write(any(), any());
        }
    }

    public void testFilesystemFailureUsesEngineErrorCategory() throws Exception {
        EngineProvider provider = provider("test");
        Path file = Files.createFile(newTempDir().resolve("not-a-directory"));
        try (EngineRuntime runtime = new EngineRuntime(Map.of("test", provider), file, Settings.EMPTY)) {
            runtime.start();
            failure(EngineException.Code.IO_ERROR, runtime.openShard("test", id(), SCHEMA, EngineService.Mode.WRITE_ONLY, context()));
        }
        verify(provider, never()).openWriter(any());
    }

    public void testMalformedDocumentIdsCannotAliasInUtf8() {
        assertThrows(IllegalArgumentException.class, () -> new Mutation.Delete("\uD800"));
        assertThrows(IllegalArgumentException.class, () -> new EngineDocument("\uD801", Map.of(), new byte[0]));
        assertEquals("\uD83D\uDE00", new Mutation.Delete("\uD83D\uDE00").id());
    }

    public void testOversizedBooleanQueryIsRejectedBeforeCopying() {
        List<SearchQuery> oversized = new java.util.AbstractList<>() {
            @Override
            public SearchQuery get(int index) {
                throw new AssertionError("oversized query must not be copied");
            }

            @Override
            public Object[] toArray() {
                throw new AssertionError("oversized query must not be copied");
            }

            @Override
            public int size() {
                return Integer.MAX_VALUE;
            }
        };
        assertThrows(IllegalArgumentException.class, () -> new SearchQuery.Bool(oversized, oversized, List.of(), 0));
    }

    public void testOversizedBatchIsRejectedBeforeCopying() throws Exception {
        try (EngineRuntime runtime = runtime(provider("test"), Settings.EMPTY)) {
            runtime.start();
            List<Mutation> oversized = new java.util.AbstractList<>() {
                @Override
                public Mutation get(int index) {
                    throw new AssertionError("oversized input must not be copied");
                }

                @Override
                public int size() {
                    return 1025;
                }
            };
            failure(EngineException.Code.RESOURCE_LIMIT, runtime.write(id(), oversized, context()));
        }
    }

    public void testInterruptedWorkerCanStillCloseShardResources() throws Exception {
        EngineProvider provider = provider("test");
        ShardWriter writer = mock(ShardWriter.class);
        when(provider.openWriter(any())).thenReturn(writer);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(writer.write(any(), any())).thenAnswer(invocation -> {
            entered.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            Thread.currentThread().interrupt();
            return null;
        });
        doAnswer(invocation -> {
            assertFalse("cleanup must clear the worker interrupt before closing I/O resources", Thread.currentThread().isInterrupted());
            return null;
        }).when(writer).close();
        Settings settings = Settings.builder().put("engine.workers", 1).build();
        try (EngineRuntime runtime = runtime(provider, settings)) {
            runtime.start();
            ShardId id = id();
            await(runtime.openShard("test", id, SCHEMA, EngineService.Mode.WRITE_ONLY, context()));
            CompletionStage<?> write = runtime.write(id, List.of(new Mutation.Delete("1")), context());
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                runtime.stop();
            } finally {
                release.countDown();
            }
            await(write);
        }
        verify(writer).close();
    }

    private EngineRuntime runtime(EngineProvider provider, Settings settings) throws IOException {
        return new EngineRuntime(Map.of(provider.descriptor().id(), provider), newTempDir(), settings);
    }

    private static EngineProvider provider(String name) {
        EngineProvider provider = mock(EngineProvider.class);
        when(provider.descriptor()).thenReturn(new EngineDescriptor(name, 1, Set.of(Schema.FieldType.TEXT)));
        return provider;
    }

    private static ShardId id() {
        return new ShardId(UUID.randomUUID(), 0);
    }

    private static OperationContext context() {
        return OperationContext.standard();
    }

    private static <T> T await(CompletionStage<T> result) throws Exception {
        return result.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static void failure(EngineException.Code code, CompletionStage<?> result) {
        ExecutionException failure = assertThrows(ExecutionException.class, () -> await(result));
        assertTrue(failure.getCause().toString(), failure.getCause() instanceof EngineException);
        assertEquals(code, ((EngineException) failure.getCause()).code());
    }

    private static final class Extension extends Plugin implements EngineExtension {
        private final List<EngineProvider> providers;

        Extension(EngineProvider... providers) {
            this.providers = List.of(providers);
        }

        @Override
        public List<EngineProvider> engineProviders() {
            return providers;
        }
    }
}
