# S3 Local Cache Feature

## Goal

Allow a LanceDB connection opened on object storage, especially `s3://...`, to
use a local directory as a write-back cache. The cache should make normal
LanceDB reads and writes operate against fast local storage while preserving the
original object-store URI and storage options.

```python
db = lancedb.connect(
    "s3://bucket/path",
    storage_options={
        "timeout": "60s",
        "localcachedir": "/mnt/nvme/lancedb-cache",
    },
)
```

## Public API

`storage_options["localcachedir"]`

- Optional string.
- Default is unset.
- Empty string is treated as unset.
- Applies only to local/native LanceDB connections backed by non-local object
  stores.
- Ignored for local filesystem URIs and remote/cloud database connections.
- Must not be forwarded to the underlying object-store credential/config
  options.

Connection methods:

- `db.s3_cache_pull()`: prefetch remote objects for this database prefix into
  the local cache.
- `db.s3_cache_commit()`: upload locally changed or deleted objects back to the
  backing object store.
- `db.s3_cache_close()`: close the cache for this connection and reject further
  cache operations through this connection.

The method names are S3-oriented for the user API, but the implementation
should work through Lance object-store abstractions so the design can extend to
other object stores.

## Architecture

Implement the feature as a Lance object-store wrapper/cache, not as in-process
FUSE, bind mount, or OS mount lifecycle management.

Expected integration points:

- Parse `localcachedir` from `ListingDatabaseOptions`.
- Keep `localcachedir` out of normal storage options passed to Lance/object_store.
- When the database URI resolves to a non-local object store, wrap that store
  with a local cache object store.
- Use the existing `WrappingObjectStore` / read-write parameter patterns so
  table opens, reads, writes, and metadata operations see the cached store.
- Keep all object paths in Lance/object_store path form. Do not rewrite the
  database URI to a local filesystem URI.

## Cache Layout

The local cache root is:

```text
<localcachedir>/<bucket-or-store-host>/<object_storage_path>
```

For:

```text
s3://my-bucket/a/b/db
localcachedir=/mnt/nvme/lancedb-cache
```

cached objects for the database prefix live under:

```text
/mnt/nvme/lancedb-cache/my-bucket/a/b/db
```

The cache must preserve relative object paths so a remote object and its local
cached copy have a deterministic one-to-one mapping.

## Runtime Semantics

Without `localcachedir`:

- LanceDB follows the existing behavior with no cache effect.

With `localcachedir` enabled on a supported object-store URI:

- Reads first try the local cache.
- On a cache miss, reads fall back to the backing object store.
- Successful cache-miss reads should materialize the object into the local cache
  when possible.
- Writes go to the local cache and are marked dirty.
- Deletes remove or hide the local cached object and are marked for remote
  deletion on commit.
- Listings should reflect the merged view of remote objects plus local cached
  changes, excluding locally deleted objects.
- `s3_cache_pull()` copies the current remote objects under the database prefix
  into the local cache for clean entries. Local dirty writes and delete
  tombstones remain authoritative until `s3_cache_commit()` or cache close.
- `s3_cache_commit()` uploads dirty local objects and applies pending deletes to
  the backing object store.
- `s3_cache_close()` closes this connection cache handle. It does not need to
  unmount anything because the design does not use OS mounts.

## Error Semantics

- Calling cache methods when cache mode is not enabled should return a clear
  not-supported error.
- Calling cache methods on unsupported connection types should return a clear
  not-supported error.
- Reads of objects that do not exist in either cache or remote should preserve
  normal object-store not-found behavior.
- If an object is locally deleted but not yet committed, reads should treat it
  as deleted even if it still exists remotely.
- Commit should be explicit; closing the cache should not silently upload dirty
  data unless a future API explicitly promises that behavior.

## Non-Goals

- Do not manage FUSE mounts, bind mounts, `/bucket` paths, or process-global
  mount state inside LanceDB.
- Do not require a full prefetch before opening or reading a database.
- Do not make local filesystem databases use this cache path.
- Do not change existing storage option names or object-store authentication
  behavior.

## Verification Checklist

- `localcachedir` is parsed and removed from forwarded storage options.
- Unsupported URIs ignore `localcachedir` for normal operations.
- Cache methods return not-supported errors when cache mode is disabled.
- Cache-hit reads return local cached objects.
- Cache-miss reads fetch from remote and populate the local cache.
- Writes remain local until `s3_cache_commit()`.
- `s3_cache_commit()` uploads dirty objects and deletes remotely deleted objects.
- `s3_cache_pull()` preloads the database prefix.
- Listings reflect remote objects, local writes, and pending deletes correctly.
- Python bindings expose the storage option and cache methods.
