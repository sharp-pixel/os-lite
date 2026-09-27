# Local core provenance

The core sources originate from the Apache-2.0 OpenSearch core artifact:

- Coordinate: `org.opensearch:opensearch-core:3.6.0-20260410.141306-164:sources`.
- Source: https://ci.opensearch.org/ci/dbc/snapshots/maven/org/opensearch/opensearch-core/3.6.0-SNAPSHOT/opensearch-core-3.6.0-20260410.141306-164-sources.jar
- Source archive SHA-256: `fb31daf8c41048927dab1c0134482ae5a267242627d6b5bb7d64488af421ce4c`.
- Original license headers, LICENSE.txt and NOTICE.txt are retained.

Local changes replace Lucene byte slices, allocation/memory utilities, platform constants and checksums with core-owned/JDK types. Product Version no longer carries an engine version. Legacy storage exception wire tags decode to engine-neutral exception representations. The unused Lucene-only BytesRefUtils API is omitted. The build substitutes this module for all upstream opensearch-core dependencies; the two implementations must never coexist.

The byte-level transport encodings remain unchanged. Java plugins that use the old Lucene-bearing core signatures must be rebuilt against this module. This is not binary compatibility with upstream OpenSearch plugins.

`src/test/resources/wire` contains independent fixtures generated before the migration with the previously resolved upstream core binary (SHA-1 `5bd40ebf5c0ae449e5b23e30b6a8f0b9215a3820`) and Lucene 10.4.0. They cover fixed/variable signed numbers, UTF-16 string wire encoding (including isolated surrogates), UTF-8 Text values and replacement semantics, composite bytes, generic byte slices, product versions and all five legacy storage exception tags (including both old-format variants). Tests decode known values and compare newly encoded bytes to these fixtures.

The compatibility target is now OpenSearch `3.10.0-SNAPSHOT`, reconciled against upstream commit `dc9bf3bc09585c57673526bbe9eed6ce70a56980`. The original provenance and wire fixtures above remain unchanged. This update ports current version constants, Jackson 3 exception adapters and filtering, content generator/builder contracts, and binary/content nesting-depth limits. Lucene-bearing upstream core signatures remain replaced by the local engine-neutral types. See the repository `UPSTREAM.md` for the upgrade scope.
