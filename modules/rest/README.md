# REST API categories

The `rest` module owns HTTP adaptation, routing, error responses, tracing, and request cleanup.
It supplies the only HTTP dispatcher. API modules contribute handlers to that dispatcher:

| Module | Handler category | Intended operations |
| --- | --- | --- |
| `search-api` | `SEARCH` | Queries, aggregations, document retrieval |
| `indexing-api` | `INDEXING` | Document creation, updates, deletion |
| `management-api` | `MANAGEMENT` | Index definitions, mappings, settings, service metadata |

Categories describe the operation, independent of the HTTP method: `POST /_search` is search,
while `POST /_bulk` is indexing. A handler serves one category across its regular, deprecated,
and replacement routes. Split a handler that mixes categories into separate handlers.

## Node configuration

All categories are enabled by default. To expose search and management APIs on a node:

```yaml
rest.api.categories: [search, management]
```

For an indexing endpoint:

```yaml
rest.api.categories: [indexing]
```

The setting is static and takes effect at node startup. Names must be lowercase. An empty
list disables all REST operations. Disabled routes are omitted from registration and route
metadata, including `OPTIONS` responses and `Allow` headers. Paths without an enabled route
return HTTP 404 for all supported HTTP methods, just like unknown paths. A path with enabled
routes still returns HTTP 405 for unsupported methods and advertises only enabled methods.
The built-in favicon is classified as management.

This is an HTTP API capability setting, not authorization or a transport action permission.
Internal `NodeClient` and transport actions are unaffected. It does not establish separate
indexing/search compute pools, shared index storage, or cross-node request forwarding.

## Contributing an API

A feature plugin implements `RestHandlerPlugin`, returns its handlers from `getRestHandlers()`,
and declares its API module as an extension:

```groovy
opensearchplugin {
    classname = 'example.SearchFeaturePlugin'
    extendedPlugins = ['search-api']
}

dependencies {
    compileOnly project(':modules:rest:spi')
}
```

Declare the category on each handler:

```java
@Override
public RestOperationCategory operationCategory() {
    return RestOperationCategory.SEARCH;
}
```

The API module aggregates matching handlers and rejects a category mismatch at startup.
The dispatcher still accepts direct `RestHandlerPlugin` extensions for compatibility.
Legacy handlers without category metadata default to management.

The hello-world feature extends `management-api` and retains its existing `GET /` endpoint
and transport action. Search and indexing modules are extension hubs; this repository does
not yet implement search or indexing engine endpoints. All three modules are bundled by the
existing distribution module discovery.
