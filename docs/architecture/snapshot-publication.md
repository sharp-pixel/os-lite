# Snapshot publication and independent readers

Status: implemented experimental filesystem reference profile, following the local profile in [lucene-engine.md](lucene-engine.md).

## Repository boundary

The engine API exports a committed snapshot through an owned lease: provider/format, schema, checkpoint, and a bounded list of file names, lengths, and SHA-256 digests. A lease supplies file streams and keeps its commit alive until closed. Engine runtime executes snapshot callbacks on its bounded workers and closes leases before releasing shard ownership.

Index-service owns repository selection, index names, writer epochs, publication, and read freshness. Repository providers extend index-service through its exported SPI. REST adapters continue to call typed actions. The filesystem provider stores opaque files and metadata without interpreting Lucene segments.

The initial repository profile uses separate writer and reader nodes, each with its own local data directory. Repository-backed nodes explicitly choose one execution role. The existing local profile remains the default.

## Publication and fencing

A repository publication contains immutable index metadata, a writer owner UUID and epoch, a snapshot UUID, and its complete manifest. A writer keeps a stable owner UUID in its local data directory. Initial creation publishes metadata and the empty committed shard together.

Each mutation is committed locally, exported through a pinned lease, copied and verified in the repository, then published atomically. Success is acknowledged only after the repository publication is durable. A failure after local commit has an unknown outcome and stops that writer until recovery. Recovery restores the repository head before opening a writer, discarding unacknowledged local progress.

Publication checks both the writer token and the expected previous snapshot. Manual takeover requires the expected epoch and advances it durably. An old token cannot publish after takeover. Startup does not automatically steal another node's assignment.

## Filesystem reference provider

One repository lock serializes metadata changes, publication, and active snapshot transfers across processes. A process-wide gate in the server's parent classloader ensures only one local channel accesses a given lock file at a time, including across plugin classloaders. This avoids the JDK's documented behavior where closing a second channel can release another channel's native lock. [JDK FileLock](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/nio/channels/FileLock.html)

State files use atomic replacement followed by directory sync. File uploads are written and synced before the state file can reference them. Incomplete uploads are invisible. Before reclaiming snapshots, the provider syncs the state directory again to make any previously uncertain rename durable.

The lock must work across every process accessing the repository. The filesystem must provide coherent reads, atomic rename, and reliable file/directory sync. The first verification target is separate processes on the same host. Deployment on a network filesystem requires validating those guarantees; object stores need their own conditional-update implementation.

A transfer holds the repository lock until its source lease closes. Before a subsequent publication, snapshots no longer referenced by any index are reclaimed under that same lock. A restarted process validates all durable state references before cleanup. Reader nodes retain their own verified copies, so repository cleanup cannot invalidate active local reads. The newest head and its predecessor can remain until the next publication; there is no historical snapshot API.

## Reader installation and freshness

Readers copy the exact manifest into a fresh local directory, enforce file and byte limits, verify every checksum, and open the backend reader before changing the visible handle. Failed transfers and corrupt snapshots leave the previous reader available. The runtime swaps readers under shard ownership and reclaims the previous local copy after its operations finish.

Ordinary reads use the current visible snapshot. Explicit refresh installs a newer published snapshot. A search can request a minimum checkpoint and a bounded wait; a different index/history fails explicitly, and timeout never returns a success from an older snapshot.

Lucene exports use `SnapshotDeletionPolicy` to retain commit files during transfer. The lease closes all opened streams and releases the pin. Closing a writer with outstanding direct leases defers resource release until those leases close; runtime transfer callbacks always close leases before releasing shard ownership. [Lucene SnapshotDeletionPolicy](https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/index/SnapshotDeletionPolicy.html)

## Configuration and API

Install the `snapshot-filesystem` module along with engine, Lucene, index-service, REST, and the desired API modules. Every process needs access to the same repository path, including write access to its lock file. Each node must have its own data directory. An existing local-profile catalog requires explicit migration; enabling this profile on that data directory fails startup.

Writer configuration:

```yaml
engine.roles: [writer]
index_service.repository: filesystem
index_service.repository_path: /srv/os-lite/snapshots
rest.api.categories: [management, indexing]
```

Reader configuration:

```yaml
engine.roles: [reader]
index_service.repository: filesystem
index_service.repository_path: /srv/os-lite/snapshots
rest.api.categories: [management, search]
```

Create and write on the writer node using the existing `PUT /books` and `PUT /books/_doc/1` routes. The first reader request discovers an index and installs its published snapshot. Subsequent ordinary reads use the visible snapshot until refresh or a minimum-checkpoint search advances it.

To refresh a reader:

```sh
curl -sS -X POST http://reader:9200/books/_refresh
```

To wait for a checkpoint returned by a write, copy its exact values into the search body:

```json
{
  "query": {"match_all": {}},
  "minimum_checkpoint": {
    "index_uuid": "UUID from the write response",
    "shard": 0,
    "history": "history UUID from the write response",
    "sequence": 42
  },
  "wait_timeout_millis": 1000
}
```

`wait_timeout_millis` is 0–30000 and requires `minimum_checkpoint`; its default is 1000 when a minimum is present. Timeout returns 408, while incompatible shard/history identity returns 409. The operation's cooperative deadline also includes admission time. Local-profile searches support the same option and wait for local commits.

`GET /books` includes `writer_epoch` and the published `checkpoint`. A replacement writer with a fresh local directory first reads that metadata, then explicitly claims the current epoch:

```sh
curl -sS -X POST http://replacement-writer:9200/books/_writer \
  -H 'Content-Type: application/json' -d '{"expected_epoch":1}'
```

The returned epoch is 2. The replacement restores the published head before its next mutation; the old writer cannot publish using epoch 1. A stale takeover request returns 409. The action uses the management category and the existing action-filter boundary. It requires the caller's normal management authorization when an authorization plugin is installed.

## Limits and failure behavior

- The filesystem backend is a correctness reference. It copies and hashes complete snapshots for every mutation; large workloads need incremental transfer and a more concurrent repository implementation.
- A snapshot contains at most 1024 files and 1 GiB total. Transfers use 64 KiB buffers. A repository holds at most 1024 index records, each at most 512 KiB. Per-node index/shard and worker admission limits still apply.
- Repository operations are serialized through the bounded index-service queue (64 waiting operations) and the repository lock. Engine imports run on the bounded engine pool. Deadlines and cancellation are cooperative and cannot interrupt every filesystem sync.
- A successful repository-profile write means its checkpoint was durably published. Failure after local commit returns `WRITE_OUTCOME_UNKNOWN` and stops further writes through that instance until recovery. Unacknowledged data can become visible if publication completed before an error.
- Recovery uses the repository head. Unpublished local changes are discarded. The stable local owner UUID allows the same writer assignment to resume after restart; a different owner requires explicit epoch takeover.
- Reader installation verifies length, SHA-256, provider format, schema, shard identity, and checkpoint before swapping. Runtime-owned private copies live under `engines/.snapshots/` and are reclaimed on replacement/close; abandoned copies for a shard are removed on its next installation.
- State records use repository format 1. Index action request/response wire format is now 2 and rejects version 1. All nodes participating in these experimental actions must run compatible builds.
- There is no automatic failover, allocation coordinator, cloud repository, object-store consistency claim, or complete OpenSearch compatibility. Lucene is isolated in its provider plugin; the host can run without that provider.

## Verification

- Independent writer and reader nodes with distinct data paths.
- Unpublished and incomplete data stays invisible.
- Acknowledged writes survive writer process and local-data loss.
- Stale epochs and competing publications fail.
- Checksum failure preserves the old reader.
- Active transfers prevent premature reclamation; restart cleans only unreferenced snapshots.
- Minimum-checkpoint waits succeed or time out correctly.
- Existing local API behavior and category filtering remain covered.

The verification includes real disk-loaded plugin bundles and Netty HTTP across separate JVMs. It kills a writer after acknowledgement, claims the next epoch on a node with fresh local storage, and reads all published documents. An independent OS process also reproduces the cross-channel lock-release defect and confirms it cannot acquire the corrected lock while its owner is active.
