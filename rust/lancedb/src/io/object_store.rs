// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Object store wrappers used by LanceDB

use std::{
    collections::{HashMap, HashSet},
    fmt::Formatter,
    sync::{Arc, Mutex},
};

use futures::{
    StreamExt, TryFutureExt, TryStreamExt,
    stream::{self, BoxStream},
};
use lance::io::WrappingObjectStore;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    RenameOptions, UploadPart,
    path::Path,
};

use async_trait::async_trait;

#[cfg(test)]
pub mod io_tracking;

#[derive(Debug)]
struct MirroringObjectStore {
    primary: Arc<dyn ObjectStore>,
    secondary: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for MirroringObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MirrowingObjectStore")?;
        writeln!(f, "primary:")?;
        self.primary.fmt(f)?;
        writeln!(f, "secondary:")?;
        self.secondary.fmt(f)?;
        Ok(())
    }
}

trait PrimaryOnly {
    fn primary_only(&self) -> bool;
}

impl PrimaryOnly for Path {
    fn primary_only(&self) -> bool {
        self.filename().unwrap_or("") == "_latest.manifest"
    }
}

/// An object store that mirrors write to secondsry object store first
/// and than commit to primary object store.
///
/// This is meant to mirrow writes to a less-durable but lower-latency
/// store. We have primary store that is durable but slow, and a secondary
/// store that is fast but not asdurable
///
/// Note: this object store does not mirror writes to *.manifest files
#[async_trait]
impl ObjectStore for MirroringObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        if location.primary_only() {
            self.primary.put_opts(location, bytes, options).await
        } else {
            self.secondary
                .put_opts(location, bytes.clone(), options.clone())
                .await?;
            self.primary.put_opts(location, bytes, options).await
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        if location.primary_only() {
            return self.primary.put_multipart_opts(location, opts).await;
        }

        let secondary = self
            .secondary
            .put_multipart_opts(location, opts.clone())
            .await?;
        let primary = self.primary.put_multipart_opts(location, opts).await?;

        Ok(Box::new(MirroringUpload { primary, secondary }))
    }

    // Reads are routed to primary only
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.primary.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.primary.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.primary.list_with_delimiter(prefix).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let primary = self.primary.clone();
        let secondary = self.secondary.clone();
        locations
            .map(move |location| {
                let primary = primary.clone();
                let secondary = secondary.clone();
                async move {
                    let location = location?;
                    if !location.primary_only() {
                        match secondary.delete(&location).await {
                            Err(Error::NotFound { .. }) | Ok(_) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    primary.delete(&location).await?;
                    Ok(location)
                }
            })
            .buffered(10)
            .boxed()
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        if to.primary_only() {
            self.primary.copy_opts(from, to, options).await
        } else {
            self.secondary.copy_opts(from, to, options.clone()).await?;
            self.primary.copy_opts(from, to, options).await?;
            Ok(())
        }
    }
}

#[derive(Debug)]
struct MirroringUpload {
    primary: Box<dyn MultipartUpload>,
    secondary: Box<dyn MultipartUpload>,
}

#[async_trait]
impl MultipartUpload for MirroringUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let put_primary = self.primary.put_part(data.clone());
        let put_secondary = self.secondary.put_part(data);
        Box::pin(put_secondary.and_then(|_| put_primary))
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.secondary.complete().await?;
        self.primary.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.secondary.abort().await?;
        self.primary.abort().await
    }
}

#[derive(Debug)]
pub struct MirroringObjectStoreWrapper {
    secondary: Arc<dyn ObjectStore>,
}

impl MirroringObjectStoreWrapper {
    pub fn new(secondary: Arc<dyn ObjectStore>) -> Self {
        Self { secondary }
    }
}

impl WrappingObjectStore for MirroringObjectStoreWrapper {
    fn wrap(&self, _store_prefix: &str, primary: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(MirroringObjectStore {
            primary,
            secondary: self.secondary.clone(),
        })
    }
}

/// Storage option enabling object-store local cache mode.
pub const LOCAL_CACHE_DIR_OPTION: &str = "localcachedir";

#[derive(Debug, Clone)]
pub struct LocalCacheObjectStore {
    remote: Arc<dyn ObjectStore>,
    cache: Arc<dyn ObjectStore>,
    sync_prefix: Option<Path>,
    dirty: Arc<Mutex<HashMap<Path, u64>>>,
    deleted: Arc<Mutex<HashMap<Path, u64>>>,
    change_generation: Arc<Mutex<u64>>,
    state_lock: Arc<Mutex<()>>,
    closed: Arc<Mutex<bool>>,
}

impl LocalCacheObjectStore {
    pub fn new(
        remote: Arc<dyn ObjectStore>,
        cache: Arc<dyn ObjectStore>,
        sync_prefix: Option<Path>,
    ) -> Self {
        Self {
            remote,
            cache,
            sync_prefix,
            dirty: Arc::new(Mutex::new(HashMap::new())),
            deleted: Arc::new(Mutex::new(HashMap::new())),
            change_generation: Arc::new(Mutex::new(0)),
            state_lock: Arc::new(Mutex::new(())),
            closed: Arc::new(Mutex::new(false)),
        }
    }

    fn closed_error() -> Error {
        Error::Generic {
            store: "LocalCacheObjectStore",
            source: "local cache is closed".into(),
        }
    }

    fn check_open(&self) -> Result<()> {
        if *self.closed.lock().expect("poisoned local cache close mutex") {
            Err(Self::closed_error())
        } else {
            Ok(())
        }
    }

    fn is_deleted(&self, location: &Path) -> bool {
        let _state = self
            .state_lock
            .lock()
            .expect("poisoned local cache state mutex");
        self.deleted
            .lock()
            .expect("poisoned local cache deleted mutex")
            .contains_key(location)
    }

    fn next_change_generation(&self) -> u64 {
        let mut generation = self
            .change_generation
            .lock()
            .expect("poisoned local cache change generation mutex");
        *generation = generation.saturating_add(1);
        *generation
    }

    fn mark_dirty(&self, location: Path) {
        let _state = self
            .state_lock
            .lock()
            .expect("poisoned local cache state mutex");
        let generation = self.next_change_generation();
        self.deleted
            .lock()
            .expect("poisoned local cache deleted mutex")
            .remove(&location);
        self.dirty
            .lock()
            .expect("poisoned local cache dirty mutex")
            .insert(location, generation);
    }

    fn mark_deleted(&self, location: Path) {
        let _state = self
            .state_lock
            .lock()
            .expect("poisoned local cache state mutex");
        let generation = self.next_change_generation();
        self.dirty
            .lock()
            .expect("poisoned local cache dirty mutex")
            .remove(&location);
        self.deleted
            .lock()
            .expect("poisoned local cache deleted mutex")
            .insert(location, generation);
    }

    async fn populate_from_remote(&self, location: &Path) -> Result<()> {
        if self.is_deleted(location) {
            return Err(Error::NotFound {
                path: location.to_string(),
                source: "object was deleted in local cache".into(),
            });
        }
        let bytes = self.remote.get(location).await?.bytes().await?;
        self.cache.put(location, bytes.into()).await?;
        Ok(())
    }

    pub async fn pull(&self) -> Result<()> {
        self.check_open()?;
        let prefix = self.sync_prefix.as_ref();
        let objects = self
            .remote
            .list(prefix)
            .try_collect::<Vec<ObjectMeta>>()
            .await?;
        let remote_locations = objects
            .iter()
            .map(|meta| meta.location.clone())
            .collect::<HashSet<_>>();
        let (dirty, deleted) = {
            let _state = self
                .state_lock
                .lock()
                .expect("poisoned local cache state mutex");
            let dirty = self
                .dirty
                .lock()
                .expect("poisoned local cache dirty mutex")
                .clone();
            let deleted = self
                .deleted
                .lock()
                .expect("poisoned local cache deleted mutex")
                .clone();
            (dirty, deleted)
        };

        // Pull reconciles clean cached state to remote. Dirty writes and local
        // delete tombstones stay authoritative until commit/close.
        for meta in objects {
            if dirty.contains_key(&meta.location) || deleted.contains_key(&meta.location) {
                continue;
            }
            let bytes = self.remote.get(&meta.location).await?.bytes().await?;
            self.cache.put(&meta.location, bytes.into()).await?;
        }

        let cache_objects = self
            .cache
            .list(prefix)
            .try_collect::<Vec<ObjectMeta>>()
            .await?;
        for meta in cache_objects {
            if remote_locations.contains(&meta.location)
                || dirty.contains_key(&meta.location)
                || deleted.contains_key(&meta.location)
            {
                continue;
            }
            match self.cache.delete(&meta.location).await {
                Ok(_) | Err(Error::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub async fn commit(&self) -> Result<()> {
        self.check_open()?;
        let (dirty_set, deleted_set) = {
            let _state = self
                .state_lock
                .lock()
                .expect("poisoned local cache state mutex");
            let dirty_set = self
                .dirty
                .lock()
                .expect("poisoned local cache dirty mutex")
                .clone();
            let deleted_set = self
                .deleted
                .lock()
                .expect("poisoned local cache deleted mutex")
                .clone();
            (dirty_set, deleted_set)
        };
        let mut dirty = dirty_set.keys().cloned().collect::<Vec<_>>();
        dirty.sort_by(|left, right| {
            Self::commit_order(left)
                .cmp(&Self::commit_order(right))
                .then_with(|| left.to_string().cmp(&right.to_string()))
        });
        let mut deleted = deleted_set.keys().cloned().collect::<Vec<_>>();
        deleted.sort_by(|left, right| left.to_string().cmp(&right.to_string()));

        for location in &dirty {
            let bytes = self.cache.get(location).await?.bytes().await?;
            self.remote.put(location, bytes.into()).await?;
        }
        for location in &deleted {
            match self.remote.delete(location).await {
                Ok(_) | Err(Error::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
        }

        // Do not hold state locks across remote uploads. Instead, clear only
        // the exact generation that was committed so concurrent same-path
        // writes/deletes remain dirty for a later commit.
        let _state = self
            .state_lock
            .lock()
            .expect("poisoned local cache state mutex");
        self.dirty
            .lock()
            .expect("poisoned local cache dirty mutex")
            .retain(|location, generation| dirty_set.get(location) != Some(generation));
        self.deleted
            .lock()
            .expect("poisoned local cache deleted mutex")
            .retain(|location, generation| deleted_set.get(location) != Some(generation));
        Ok(())
    }

    fn commit_order(location: &Path) -> u8 {
        let path: &str = location.as_ref();
        let filename = location.filename().unwrap_or("");
        if path.contains("_latest")
            || path.contains("_versions/")
            || path.contains("/_versions")
            || filename.ends_with(".manifest")
            || filename.contains("manifest")
            || filename.contains("version")
        {
            2
        } else if path.contains("_metadata")
            || filename.contains("metadata")
            || path.contains("_transactions")
            || path.contains("_indices")
        {
            1
        } else {
            0
        }
    }

    async fn has_visible_descendants(
        store: &Arc<dyn ObjectStore>,
        prefix: &Path,
        deleted: &HashMap<Path, u64>,
    ) -> Result<bool> {
        let objects = store
            .list(Some(prefix))
            .try_collect::<Vec<ObjectMeta>>()
            .await?;
        Ok(objects
            .into_iter()
            .any(|meta| !deleted.contains_key(&meta.location)))
    }

    pub fn close(&self) -> Result<()> {
        let mut closed = self.closed.lock().expect("poisoned local cache close mutex");
        *closed = true;
        Ok(())
    }
}

impl std::fmt::Display for LocalCacheObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "LocalCacheObjectStore")?;
        writeln!(f, "remote:")?;
        self.remote.fmt(f)?;
        writeln!(f, "cache:")?;
        self.cache.fmt(f)?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for LocalCacheObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.check_open()?;
        let result = self.cache.put_opts(location, bytes, options).await?;
        self.mark_dirty(location.clone());
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.check_open()?;
        let upload = self.cache.put_multipart_opts(location, opts).await?;
        Ok(Box::new(LocalCacheUpload {
            upload,
            store: Arc::new(self.clone()),
            location: location.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.check_open()?;
        match self.cache.get_opts(location, options.clone()).await {
            Ok(result) => Ok(result),
            Err(Error::NotFound { .. }) => {
                if self.is_deleted(location) {
                    Err(Error::NotFound {
                        path: location.to_string(),
                        source: "object was deleted in local cache".into(),
                    })
                } else if options.head {
                    self.remote.get_opts(location, options).await
                } else {
                    self.populate_from_remote(location).await?;
                    self.cache.get_opts(location, options).await
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        self.check_open()?;
        match self.cache.get_ranges(location, ranges).await {
            Ok(result) => Ok(result),
            Err(Error::NotFound { .. }) => {
                self.populate_from_remote(location).await?;
                self.cache.get_ranges(location, ranges).await
            }
            Err(err) => Err(err),
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        if let Err(err) = self.check_open() {
            return stream::once(async move { Err(err) }).boxed();
        }
        let cache = self.cache.clone();
        let remote = self.remote.clone();
        let prefix = prefix.cloned();
        let deleted = {
            let _state = self
                .state_lock
                .lock()
                .expect("poisoned local cache state mutex");
            self.deleted
                .lock()
                .expect("poisoned local cache deleted mutex")
                .clone()
        };
        async move {
            let mut objects = HashMap::new();
            let remote_objects = remote
                .list(prefix.as_ref())
                .try_collect::<Vec<ObjectMeta>>()
                .await?;
            for meta in remote_objects {
                if !deleted.contains_key(&meta.location) {
                    objects.insert(meta.location.clone(), meta);
                }
            }
            let cache_objects = cache
                .list(prefix.as_ref())
                .try_collect::<Vec<ObjectMeta>>()
                .await?;
            for meta in cache_objects {
                if !deleted.contains_key(&meta.location) {
                    objects.insert(meta.location.clone(), meta);
                }
            }
            Ok::<_, Error>(stream::iter(objects.into_values().map(Ok)).boxed())
        }
        .try_flatten_stream()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.check_open()?;
        let deleted = {
            let _state = self
                .state_lock
                .lock()
                .expect("poisoned local cache state mutex");
            self.deleted
                .lock()
                .expect("poisoned local cache deleted mutex")
                .clone()
        };
        let remote = self.remote.list_with_delimiter(prefix).await?;
        let cache = self.cache.list_with_delimiter(prefix).await?;

        let mut objects = HashMap::new();
        for meta in remote.objects.into_iter().chain(cache.objects) {
            if !deleted.contains_key(&meta.location) {
                objects.insert(meta.location.clone(), meta);
            }
        }
        let mut prefix_candidates = remote.common_prefixes;
        for prefix in cache.common_prefixes {
            if !prefix_candidates.contains(&prefix) {
                prefix_candidates.push(prefix);
            }
        }
        let mut common_prefixes = Vec::new();
        for prefix in prefix_candidates {
            if Self::has_visible_descendants(&self.cache, &prefix, &deleted).await?
                || Self::has_visible_descendants(&self.remote, &prefix, &deleted).await?
            {
                common_prefixes.push(prefix);
            }
        }
        common_prefixes.sort();
        Ok(ListResult {
            common_prefixes,
            objects: objects.into_values().collect(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        if let Err(err) = self.check_open() {
            return stream::once(async move { Err(err) }).boxed();
        }
        let cache = self.cache.clone();
        let store = Arc::new(self.clone());
        locations
            .map(move |location| {
                let cache = cache.clone();
                let store = store.clone();
                async move {
                    let location = location?;
                    match cache.delete(&location).await {
                        Ok(_) | Err(Error::NotFound { .. }) => {}
                        Err(err) => return Err(err),
                    }
                    store.mark_deleted(location.clone());
                    Ok(location)
                }
            })
            .buffered(10)
            .boxed()
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.check_open()?;
        match self
            .cache
            .get_opts(from, GetOptions::new().with_head(true))
            .await
        {
            Ok(_) => {}
            Err(Error::NotFound { .. }) => self.populate_from_remote(from).await?,
            Err(err) => return Err(err),
        }
        self.cache.copy_opts(from, to, options).await?;
        self.mark_dirty(to.clone());
        Ok(())
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.check_open()?;
        match self
            .cache
            .get_opts(from, GetOptions::new().with_head(true))
            .await
        {
            Ok(_) => {}
            Err(Error::NotFound { .. }) => self.populate_from_remote(from).await?,
            Err(err) => return Err(err),
        }
        self.cache.rename_opts(from, to, options).await?;
        self.mark_deleted(from.clone());
        self.mark_dirty(to.clone());
        Ok(())
    }
}

#[derive(Debug)]
struct LocalCacheUpload {
    upload: Box<dyn MultipartUpload>,
    store: Arc<LocalCacheObjectStore>,
    location: Path,
}

#[async_trait]
impl MultipartUpload for LocalCacheUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.upload.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let result = self.upload.complete().await?;
        self.store.mark_dirty(self.location.clone());
        Ok(result)
    }

    async fn abort(&mut self) -> Result<()> {
        self.upload.abort().await
    }
}

#[derive(Debug)]
pub struct LocalCacheObjectStoreWrapper {
    store: Arc<LocalCacheObjectStore>,
}

impl LocalCacheObjectStoreWrapper {
    pub fn new(store: Arc<LocalCacheObjectStore>) -> Self {
        Self { store }
    }
}

impl WrappingObjectStore for LocalCacheObjectStoreWrapper {
    fn wrap(&self, _store_prefix: &str, _primary: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }
}

// windows pathing can't be simply concatenated
#[cfg(all(test, not(windows)))]
mod test {
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::TryStreamExt;
    use lance::{dataset::WriteParams, io::ObjectStoreParams};
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32, RandomVector};
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tempfile;
    use tokio::sync::Barrier;

    use crate::{
        connect,
        query::{ExecutableQuery, QueryBase},
        table::WriteOptions,
    };

    #[derive(Debug)]
    struct RecordingObjectStore {
        target: Arc<dyn ObjectStore>,
        puts: Arc<Mutex<Vec<Path>>>,
    }

    impl RecordingObjectStore {
        fn new(target: Arc<dyn ObjectStore>) -> Self {
            Self {
                target,
                puts: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn puts(&self) -> Arc<Mutex<Vec<Path>>> {
            self.puts.clone()
        }
    }

    impl std::fmt::Display for RecordingObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            writeln!(f, "RecordingObjectStore")?;
            self.target.fmt(f)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RecordingObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            options: PutOptions,
        ) -> Result<PutResult> {
            self.puts
                .lock()
                .expect("poisoned recording puts mutex")
                .push(location.clone());
            self.target.put_opts(location, bytes, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.target.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.target.get_opts(location, options).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.target.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.target.list_with_delimiter(prefix).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.target.delete_stream(locations)
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.target.copy_opts(from, to, options).await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
            self.target.rename_opts(from, to, options).await
        }
    }

    #[derive(Debug)]
    struct DelayedPutObjectStore {
        target: Arc<dyn ObjectStore>,
        after_first_put: Arc<Barrier>,
        release_first_put: Arc<Barrier>,
        delay_next_put: AtomicBool,
    }

    impl DelayedPutObjectStore {
        fn new(
            target: Arc<dyn ObjectStore>,
            after_first_put: Arc<Barrier>,
            release_first_put: Arc<Barrier>,
        ) -> Self {
            Self {
                target,
                after_first_put,
                release_first_put,
                delay_next_put: AtomicBool::new(true),
            }
        }
    }

    impl std::fmt::Display for DelayedPutObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            writeln!(f, "DelayedPutObjectStore")?;
            self.target.fmt(f)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for DelayedPutObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            options: PutOptions,
        ) -> Result<PutResult> {
            let result = self.target.put_opts(location, bytes, options).await?;
            if self.delay_next_put.swap(false, Ordering::SeqCst) {
                self.after_first_put.wait().await;
                self.release_first_put.wait().await;
            }
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.target.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.target.get_opts(location, options).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.target.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.target.list_with_delimiter(prefix).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.target.delete_stream(locations)
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.target.copy_opts(from, to, options).await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
            self.target.rename_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn test_local_cache_reads_populate_and_commit_writes() {
        let remote = Arc::new(InMemory::new());
        let cache = Arc::new(InMemory::new());
        let store = LocalCacheObjectStore::new(remote.clone(), cache.clone(), None);
        let location = Path::from("bucket/prefix/table.lance/data.bin");

        remote
            .put(&location, PutPayload::from_static(b"remote"))
            .await
            .unwrap();

        let bytes = store.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"remote");
        assert_eq!(
            cache
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"remote"
        );

        store
            .put(&location, PutPayload::from_static(b"local"))
            .await
            .unwrap();
        assert_eq!(
            remote
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"remote"
        );

        store.commit().await.unwrap();
        assert_eq!(
            remote
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"local"
        );
    }

    #[tokio::test]
    async fn test_local_cache_commit_deletes_and_close_fails() {
        let remote = Arc::new(InMemory::new());
        let cache = Arc::new(InMemory::new());
        let store = LocalCacheObjectStore::new(remote.clone(), cache, None);
        let location = Path::from("bucket/prefix/table.lance/delete.bin");

        remote
            .put(&location, PutPayload::from_static(b"remote"))
            .await
            .unwrap();
        store.delete(&location).await.unwrap();
        assert!(remote.get(&location).await.is_ok());

        store.commit().await.unwrap();
        assert!(matches!(
            remote.get(&location).await,
            Err(Error::NotFound { .. })
        ));

        store.close().unwrap();
        assert!(matches!(
            store.commit().await,
            Err(Error::Generic { store, .. }) if store == "LocalCacheObjectStore"
        ));
    }

    #[tokio::test]
    async fn test_local_cache_commit_uploads_payloads_before_publication_files() {
        let remote = RecordingObjectStore::new(Arc::new(InMemory::new()));
        let recorded_puts = remote.puts();
        let cache = Arc::new(InMemory::new());
        let store = LocalCacheObjectStore::new(Arc::new(remote), cache, None);
        let payload = Path::from("bucket/table.lance/data/part-0.lance");
        let metadata = Path::from("bucket/table.lance/_metadata/schema.pb");
        let latest = Path::from("bucket/table.lance/_latest.manifest");
        let version = Path::from("bucket/table.lance/_versions/2.manifest");

        store
            .put(&version, PutPayload::from_static(b"version"))
            .await
            .unwrap();
        store
            .put(&latest, PutPayload::from_static(b"latest"))
            .await
            .unwrap();
        store
            .put(&metadata, PutPayload::from_static(b"metadata"))
            .await
            .unwrap();
        store
            .put(&payload, PutPayload::from_static(b"payload"))
            .await
            .unwrap();

        store.commit().await.unwrap();

        let puts = recorded_puts
            .lock()
            .expect("poisoned recording puts mutex")
            .clone();
        assert_eq!(
            puts.iter()
                .map(LocalCacheObjectStore::commit_order)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 2]
        );
        assert_eq!(puts[0], payload);
        assert_eq!(puts[1], metadata);
    }

    #[tokio::test]
    async fn test_local_cache_pull_prunes_clean_stale_entries_only() {
        let remote = Arc::new(InMemory::new());
        let cache = Arc::new(InMemory::new());
        let sync_prefix = Path::from("bucket/prefix");
        let store =
            LocalCacheObjectStore::new(remote.clone(), cache.clone(), Some(sync_prefix.clone()));
        let remote_path = Path::from("bucket/prefix/remote.bin");
        let stale_path = Path::from("bucket/prefix/stale.bin");
        let dirty_path = Path::from("bucket/prefix/dirty.bin");
        let deleted_path = Path::from("bucket/prefix/deleted.bin");
        let outside_prefix = Path::from("bucket/other/stale.bin");

        remote
            .put(&remote_path, PutPayload::from_static(b"remote"))
            .await
            .unwrap();
        remote
            .put(&deleted_path, PutPayload::from_static(b"deleted"))
            .await
            .unwrap();
        cache
            .put(&stale_path, PutPayload::from_static(b"stale"))
            .await
            .unwrap();
        cache
            .put(&outside_prefix, PutPayload::from_static(b"outside"))
            .await
            .unwrap();
        store
            .put(&dirty_path, PutPayload::from_static(b"dirty"))
            .await
            .unwrap();
        store.delete(&deleted_path).await.unwrap();

        store.pull().await.unwrap();

        assert_eq!(
            cache
                .get(&remote_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"remote"
        );
        assert!(matches!(
            cache.get(&stale_path).await,
            Err(Error::NotFound { .. })
        ));
        assert_eq!(
            cache
                .get(&dirty_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"dirty"
        );
        assert!(matches!(
            store.get(&deleted_path).await,
            Err(Error::NotFound { .. })
        ));
        assert!(cache.get(&outside_prefix).await.is_ok());
    }

    #[tokio::test]
    async fn test_local_cache_commit_preserves_same_path_dirty_race() {
        let remote_target = Arc::new(InMemory::new());
        let after_first_put = Arc::new(Barrier::new(2));
        let release_first_put = Arc::new(Barrier::new(2));
        let remote = Arc::new(DelayedPutObjectStore::new(
            remote_target.clone(),
            after_first_put.clone(),
            release_first_put.clone(),
        ));
        let cache = Arc::new(InMemory::new());
        let store = LocalCacheObjectStore::new(remote, cache, None);
        let location = Path::from("bucket/prefix/race.bin");

        store
            .put(&location, PutPayload::from_static(b"first"))
            .await
            .unwrap();

        let commit_store = store.clone();
        let commit = tokio::spawn(async move {
            commit_store.commit().await.unwrap();
        });

        after_first_put.wait().await;
        store
            .put(&location, PutPayload::from_static(b"second"))
            .await
            .unwrap();
        release_first_put.wait().await;
        commit.await.unwrap();

        assert_eq!(
            remote_target
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"first"
        );

        store.commit().await.unwrap();
        assert_eq!(
            remote_target
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"second"
        );
    }

    #[tokio::test]
    async fn test_local_cache_list_with_delimiter_hides_deleted_remote_prefixes() {
        let remote = Arc::new(InMemory::new());
        let cache = Arc::new(InMemory::new());
        let store = LocalCacheObjectStore::new(remote.clone(), cache, None);
        let table_file = Path::from("dropped.lance/data/part-0.lance");
        remote
            .put(&table_file, PutPayload::from_static(b"remote"))
            .await
            .unwrap();

        let before_delete = store.list_with_delimiter(None).await.unwrap();
        assert!(before_delete
            .common_prefixes
            .contains(&Path::from("dropped.lance")));

        store.delete(&table_file).await.unwrap();

        let after_delete = store.list_with_delimiter(None).await.unwrap();
        assert!(!after_delete
            .common_prefixes
            .contains(&Path::from("dropped.lance")));
    }

    // This test is ignored because lance 3.0 introduced LocalWriter optimization
    // that bypasses the object store wrapper for local writes. The mirroring feature
    // still works for remote/cloud storage, but can't be tested with local storage.
    // See lance commit c878af433 "perf: create local writer for efficient local writes"
    #[ignore]
    #[tokio::test]
    async fn test_e2e() {
        let dir1 = tempfile::tempdir().unwrap().keep().canonicalize().unwrap();
        let dir2 = tempfile::tempdir().unwrap().keep().canonicalize().unwrap();

        let secondary_store = LocalFileSystem::new_with_prefix(dir2.to_str().unwrap()).unwrap();
        let object_store_wrapper = Arc::new(MirroringObjectStoreWrapper {
            secondary: Arc::new(secondary_store),
        });

        let db = connect(dir1.to_str().unwrap()).execute().await.unwrap();

        let mut param = WriteParams::default();
        let store_params = ObjectStoreParams {
            object_store_wrapper: Some(object_store_wrapper),
            ..Default::default()
        };
        param.store_params = Some(store_params);

        let mut datagen = BatchGenerator::new();
        datagen = datagen.col(Box::<IncrementingInt32>::default());
        datagen = datagen.col(Box::new(RandomVector::default().named("vector".into())));

        let data: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(datagen.batch(100));
        let res = db
            .create_table("test", data)
            .write_options(WriteOptions {
                lance_write_params: Some(param),
            })
            .execute()
            .await;

        // leave this here for easy debugging
        let t = res.unwrap();

        assert_eq!(t.count_rows(None).await.unwrap(), 100);

        let q = t
            .query()
            .limit(10)
            .nearest_to(&[0.1, 0.1, 0.1, 0.1])
            .unwrap()
            .execute()
            .await
            .unwrap();

        let bateches = q.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(bateches.len(), 1);
        assert_eq!(bateches[0].num_rows(), 10);

        use walkdir::WalkDir;

        let primary_location = dir1.join("test.lance").canonicalize().unwrap();
        let secondary_location = dir2.join(primary_location.strip_prefix("/").unwrap());

        // Skip lance internal directories (_versions, _transactions) and manifest files
        let should_skip = |path: &std::path::Path| -> bool {
            let path_str = path.to_str().unwrap();
            path_str.contains("_latest.manifest")
                || path_str.contains("_versions")
                || path_str.contains("_transactions")
        };

        let primary_files: Vec<_> = WalkDir::new(&primary_location)
            .into_iter()
            .filter_entry(|e| !should_skip(e.path()))
            .filter_map(|e| e.ok())
            .map(|e| {
                e.path()
                    .strip_prefix(&primary_location)
                    .unwrap()
                    .to_path_buf()
            })
            .collect();

        let secondary_files: Vec<_> = WalkDir::new(&secondary_location)
            .into_iter()
            .filter_entry(|e| !should_skip(e.path()))
            .filter_map(|e| e.ok())
            .map(|e| {
                e.path()
                    .strip_prefix(&secondary_location)
                    .unwrap()
                    .to_path_buf()
            })
            .collect();

        assert_eq!(primary_files, secondary_files, "File lists should match");
    }
}
