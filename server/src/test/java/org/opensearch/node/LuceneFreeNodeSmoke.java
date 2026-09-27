/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.node;

import org.opensearch.Build;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.http.HttpServerTransport;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;

/** Forked distribution smoke test with only production host dependencies. @opensearch.internal */
public final class LuceneFreeNodeSmoke {
    private static final List<String> MODULES = List.of(
        "rest",
        "search-api",
        "indexing-api",
        "management-api",
        "engine",
        "index-service",
        "index-search-api",
        "index-indexing-api",
        "index-management-api",
        "transport-netty4"
    );

    private LuceneFreeNodeSmoke() {}

    private static final class SmokeNode extends Node {
        private HttpServerTransport http;

        SmokeNode(Path home) {
            super(
                new Environment(
                    Settings.builder()
                        .put("path.home", home)
                        .put("node.name", "lucene-free-smoke")
                        .put("http.port", 0)
                        .put("transport.port", 0)
                        .build(),
                    null
                )
            );
        }

        @Override
        protected HttpServerTransport newHttpTransport(NetworkModule network) {
            http = super.newHttpTransport(network);
            return http;
        }

        URI uri(String path) {
            return URI.create("http://127.0.0.1:" + http.boundAddress().publishAddress().getPort() + path);
        }
    }

    private static String request(HttpClient client, SmokeNode node, String method, String path, String body, int status) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(node.uri(path)).timeout(Duration.ofSeconds(15));
        if (body != null) request.header("Content-Type", "application/json");
        HttpResponse<String> response = client.send(
            request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(),
            HttpResponse.BodyHandlers.ofString()
        );
        if (response.statusCode() != status) throw new AssertionError(
            method + " " + path + ": " + response.statusCode() + " " + response.body()
        );
        return response.body();
    }

    private static void copyTree(Path source, Path target) throws IOException {
        try (var files = Files.walk(source)) {
            for (Path path : files.toList()) {
                Path copy = target.resolve(source.relativize(path));
                if (Files.isDirectory(path)) Files.createDirectories(copy);
                else Files.copy(path, copy);
            }
        }
    }

    private static void removeTree(Path root) throws IOException {
        try (var files = Files.walk(root)) {
            for (Path path : files.sorted(Comparator.reverseOrder()).toList())
                Files.delete(path);
        }
    }

    private static void assertLuceneHidden() {
        try {
            Class.forName("org.apache.lucene.util.Version", false, LuceneFreeNodeSmoke.class.getClassLoader());
            throw new AssertionError("Lucene is visible to the host classloader");
        } catch (ClassNotFoundException expected) {
            // Only the provider's child classloader may see its dependencies.
        }
    }

    public static void main(String[] args) throws Exception {
        assertLuceneHidden();
        if (Build.CURRENT.hash().equals("unknown")) throw new AssertionError("packaged core build metadata is missing");
        Path distributions = Path.of(args[0]);
        Path home = Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "os-lite-lucene-free-");
        try (HttpClient client = HttpClient.newHttpClient()) {
            for (String module : MODULES)
                copyTree(distributions.resolve(module), home.resolve("modules").resolve(module));
            try (SmokeNode node = new SmokeNode(home)) {
                node.start();
                request(client, node, "GET", "/missing", null, 404);
                String error = request(
                    client,
                    node,
                    "PUT",
                    "/books",
                    "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}",
                    400
                );
                if (error.contains("engine provider not installed") == false) throw new AssertionError(error);
            }
            copyTree(distributions.resolve("lucene-engine"), home.resolve("modules/lucene-engine"));
            try (SmokeNode node = new SmokeNode(home)) {
                node.start();
                request(client, node, "PUT", "/books", "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}", 200);
                request(client, node, "PUT", "/books/_doc/1", "{\"title\":\"isolated engine\"}", 200);
                request(client, node, "POST", "/books/_refresh", null, 200);
                String result = request(client, node, "GET", "/books/_doc/1", null, 200);
                if (result.contains("isolated engine") == false) throw new AssertionError(result);
                assertLuceneHidden();
            }
            removeTree(home.resolve("modules/lucene-engine"));
            try (SmokeNode node = new SmokeNode(home)) {
                node.start();
                String metadata = request(client, node, "GET", "/books", null, 200);
                if (metadata.contains("lucene") == false) throw new AssertionError(metadata);
                String error = request(client, node, "POST", "/books/_search", null, 400);
                if (error.contains("engine provider not installed") == false) throw new AssertionError(error);
                assertLuceneHidden();
            }
        } finally {
            removeTree(home);
        }
    }
}
