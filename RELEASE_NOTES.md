# Release Notes

## Unreleased

- Added object-store local cache support for native LanceDB connections through
  `storage_options["localcachedir"]`, including read-through cache misses,
  write-back local changes, explicit `s3_cache_pull()`, `s3_cache_commit()`, and
  `s3_cache_close()` APIs, plus Python binding/type-stub exposure.
