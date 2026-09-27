# AGENTS.md — Context for Coding Agents

## Project Overview

`os-lite` is a lightweight, modular reimplementation of [OpenSearch](https://opensearch.org/) (upstream compatibility target 3.10.0-SNAPSHOT; see `UPSTREAM.md`). The goal is a clean, extensible search/analytics server where features are delivered as pluggable modules rather than baked into the core. It is licensed under Apache 2.0.

## Repository Layout

```
os-lite/
├── server/                  # Core server: bootstrap, cluster, HTTP, node, plugins, transport
├── modules/
│   ├── hello-world/         # Demo plugin: shows how to add an action + REST handler
│   ├── transport-netty4/    # Netty 4 HTTP/transport implementation
│   ├── rest/                # REST infrastructure and SPI
│   └── http-api/            # HTTP API definitions (placeholder)
├── libs/
│   ├── core/                # Engine-neutral core and wire contracts
│   ├── engine-api/          # Pluggable engine contracts
│   └── index-api/           # Engine-neutral index contracts
├── distribution/            # Packaging (Linux x64 tar, macOS arm64 tar)
│   └── tools/               # java-version-checker, launchers
├── buildSrc/                # Custom Gradle plugins and build tasks
├── test/
│   └── logger-usage/        # Logger usage validator (OpenSearchLoggerUsageChecker)
└── gradle/                  # Gradle helper scripts (run, formatting, coverage, IDE, etc.)
```

## Technology Stack

| Layer | Technology |
|---|---|
| Language | Java 25 runtime; Java 21 build-tool target |
| Build | Gradle 9.8.0 (wrapper: `./gradlew`) |
| Dependency versions | `gradle/libs.versions.toml` (version catalog) |
| Search engine | Lucene 10.5.1 (lucene-engine module only) |
| Networking | Netty 4.2.18, Reactor Netty 1.3.7 |
| Serialization | Jackson 3.2.2 (runtime), 2.22.2 (build tooling), Protocol Buffers 3.25.8 |
| Logging | Log4j 2.25.5, SLF4j 2.0.17 |
| Testing | JUnit 4/5, Hamcrest, Mockito 5, RandomizedRunner |
| Bundled JDK | Adoptium JDK 25.0.4.1+1 |

## Build, Run, and Test Commands

```bash
# Build everything
./gradlew build

# Assemble distribution archives only
./gradlew assemble

# Run the server (single node)
./gradlew run

# Run with multiple nodes / zones
./gradlew run -PnumNodes=3
./gradlew run -PnumZones=2

# Run all tests
./gradlew test

# Run tests for a specific subproject
./gradlew :server:test
./gradlew :modules:hello-world:test

# Check code formatting (Spotless)
./gradlew spotlessCheck

# Apply code formatting
./gradlew spotlessApply
```

## Plugin / Module Architecture

The project uses an action-plugin system. A module contributes functionality by:

1. Extending `Plugin` (and optionally `ActionPlugin`, `RestPlugin`, etc.) in its main plugin class.
2. Declaring its plugin class in the `opensearch.plugin` Gradle extension.
3. Registering actions, REST handlers, and transport handlers via overridden methods.

The `modules/hello-world` module is the canonical reference example. Study these files to understand the pattern:

| File | Purpose |
|---|---|
| `HelloWorldPlugin.java` | Plugin entry point; registers actions and REST handlers |
| `HelloWorldAction.java` | Defines the action (name, request/response types) |
| `RestHelloWorldAction.java` | Maps an HTTP route to the action |
| `HelloWorldTransportAction.java` | Server-side handler that executes the action |
| `HelloWorldRequest.java` / `HelloWorldResponse.java` | Request and response value objects |

When adding a new module, follow the same structure and register the subproject in `settings.gradle`.

Note that plugins/modules depend on `server/`, not the other way around. We do not explicitly declare the dependency on `server/` in the plugin's `build.gradle` file. Instead,
the dependency is implicitly injected by the `opensearchplugin` Gradle plugin, which is implemented in `buildSrc/src/main/groovy/org/opensearch/gradle/plugin/PluginBuildPlugin.groovy`.
Specifically, its `configureDependencies` method adds `compileOnly project.project(':server')` the plugin's dependencies.

## Architecture constraints

REST extraction is complete: `modules/rest` owns `RestController` and HTTP-to-REST adaptation. The server dispatcher accepts `HttpRequest` and `HttpChannel`. `NetworkModule` selects the dispatcher contributed by a `NetworkPlugin`, rejects multiple providers, and uses `NO_OP_DISPATCHER` when none is installed.

The host and local `libs/core` must remain free of Lucene dependencies. Lucene belongs to the isolated `modules/lucene-engine` provider. Preserve the local core dependency substitution, engine/index APIs, REST category modules, and lifecycle/security fixes when porting upstream changes. Upstream plugin binary compatibility is not promised.

## Code Quality

- The build enforces strict compiler warnings (`-Xlint:all`) and strict Javadoc linting (`-Xdoclint`).
- Code formatting is enforced via Spotless — run `./gradlew spotlessApply` before committing.
- Logger usage is validated by the `test/logger-usage` tool; use the project's logging conventions.
- Use `@opensearch.internal`, `@opensearch.api`, or `@opensearch.experimental` Javadoc tags to classify APIs.

## Key Gradle Properties (`gradle.properties`)

- Build caching and parallel builds are enabled.
- JVM max heap is set to 3 GB (`org.gradle.jvmargs=-Xmx3g`).
- Dependency version conflict detection is disabled (versions are managed centrally in `gradle/libs.versions.toml`).

## Notes for Agents

- Prefer editing existing files over creating new ones.
- When adding a new plugin feature, model it after `modules/hello-world`.
- All dependency versions must go through `gradle/libs.versions.toml`; do not hard-code versions in `build.gradle` files.
- Do not skip Spotless or compiler-warning fixes — the build will fail.
- The distribution archives bundle a JDK; do not assume a system JDK at runtime.
