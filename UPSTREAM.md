# Upstream compatibility baseline

os-lite targets **OpenSearch 3.10.0-SNAPSHOT**, using the local OpenSearch checkout at commit `dc9bf3bc09585c57673526bbe9eed6ce70a56980` (2026-09-25) as the source reference.

The dependency catalog follows the versions in that checkout for dependencies already used here: Lucene 10.5.1, Jackson 3.2.2 for content APIs, Log4j 2.25.5, Netty 4.2.18.Final, Reactor Netty 1.3.7, and Adoptium JDK 25.0.4.1+1. Gradle is 9.8.0. Jackson 2 remains available to build tooling. OpenSearch auxiliary libraries resolve as 3.10.0-SNAPSHOT artifacts through the existing repositories; these artifacts are mutable snapshots, not binaries pinned to the source commit.

## Source changes carried forward

- Local core version identifiers through 3.10.0, with no Lucene version field.
- Jackson 3 exception translation, filters, content generation, pretty printing, plugin descriptor parsing, and JSON logging compatibility.
- Upstream nesting-depth limits in generic binary deserialization and content parsing.
- Netty HTTP pipeline-depth configuration and HTTP/2 advertised header-size limits.
- HTTP 504 status for timeout exceptions and optional plugin dependency logging.

## Fork boundaries

This is a compatibility upgrade of the existing lightweight implementation, not a wholesale import of upstream features. REST stays in modules, the host remains Lucene-free, and the local core supplies engine-neutral byte/wire contracts. Existing REST categories, index and engine APIs, snapshot publication, circuit breakers, remote-bind restrictions, and lifecycle fixes are retained.

Upstream APIs for features absent here (including DataFusion/native-memory management, new cluster/search actions, virtual-thread pools, and Arrow streaming consumers) are not imported. Upstream plugins using the original Lucene-bearing signatures must be adapted and rebuilt. The original core provenance and independent wire fixtures are recorded in `libs/core/UPSTREAM.md`.

## Validation

Run `./gradlew build` for compilation, tests, formatting checks, dependency/license checks, distribution archives, and the Lucene-free node smoke test. Use `./gradlew spotlessApply` before committing changes. The regression suite covers binary wire fixtures, nesting limits, filtered/pretty JSON output, malformed-JSON HTTP status mapping, module behavior, and engine isolation.
