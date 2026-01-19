//! Implements caching of WIT resources.
//!
//! # Background
//! When executing WASM UDFs, the host needs to pass data to the guest WASM module. Creating these resources involves:
//!
//! 1. Serializing host data into a format the WASM guest understands
//! 2. Making a cross-boundary call into the WASM module to create the resource
//! 3. The guest allocating memory and constructing the object
//!
//! This is expensive, and the same data is often reused across multiple UDF invocations. The cache stores the WASM
//! resource handle so subsequent calls with the same data skip the creation overhead.
//!
//! # Resource Lifetime
//! The resource model is mostly designed to work with [`wasmtime::component::ResourceAny`]. This resource is a
//! cloneable handle. Cloning the handle does NOT clone the resource itself. The handle is also NOT some kind of smart
//! pointer and has NO reference counting.
//!
//! This means to keep the resource alive (i.e. prevent it from being [cleaned](ResourceCache::clean)), you MUST hold
//! the key [`Arc`]. Note that [`ResourceCache::cache`] takes a reference to the [`Arc`]-ed key, so the caller can hold
//! onto the [`Arc`] after the call.
//!
//! It is technically possible to onto a value/resource after it was cleaned. This is safe in the Rust sense, but may
//! error or panic if the resource is used, e.g. when you call into the WASM guest and pass this resource.
//!
//! # Mutability
//! Cached values MUST be immutable. Especially they MUST NOT implement [interior mutability], e.g. via [`Mutex`].
//!
//!
//! [interior mutability]: https://doc.rust-lang.org/reference/interior-mutability.html
//! [`Mutex`]: std::sync::Mutex
use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZeroUsize,
    sync::{Arc, Weak},
};

use datafusion_common::{DataFusionError, error::Result as DataFusionResult};

/// Resource cache value.
pub(crate) trait ResourceCacheValue<K>: Clone + Debug + Send {
    /// Additional context required for [`new`](Self::new) / [`clean`](Self::clean).
    type Context: Send + Sync;

    /// Create resource from key.
    async fn new(k: &Arc<K>, ctx: &Self::Context) -> DataFusionResult<Self>;

    /// Clean up resource from guest.
    async fn clean(self, ctx: &Self::Context) -> DataFusionResult<()>;
}

/// Cache entry.
#[derive(Debug)]
struct CacheEntry<K, V>
where
    K: Debug + Send + Sync,
    V: ResourceCacheValue<K>,
{
    /// Corresponding key.
    key: Weak<K>,

    /// Cached value.
    value: V,

    /// Last used timestamp, logical clock.
    last_used: u64,
}

/// WIT resource cache.
///
/// Avoids creating expensive resource multiple times.
#[derive(Debug)]
pub(crate) struct ResourceCache<K, V>
where
    K: Debug + Send + Sync,
    V: ResourceCacheValue<K>,
{
    /// Cached entries, keyed by address.
    ///
    /// We are using the address gathered via [`Arc::as_ptr`] and then mapped to an integer using [`pointer::addr`]
    /// instead of a (const) pointer, because pointers in Rust are usually not [`Send`].
    ///
    ///
    /// [`pointer::addr`]: https://doc.rust-lang.org/std/primitive.pointer.html#method.add
    cache: HashMap<usize, CacheEntry<K, V>>,

    /// Maximum number of entries in [`cache`](Self::cache).
    max_entries: NonZeroUsize,

    /// Logical clock for last-used entries.
    ///
    /// We use a simple, mutable integer instead of a processor or wall clock since the latter usually involves a
    /// rather expensive syscall. And we don't need the actual time here, but rather a logical ordering of events.
    logical_clock: u64,
}

impl<K, V> ResourceCache<K, V>
where
    K: Debug + Send + Sync,
    V: ResourceCacheValue<K>,
{
    /// Create new, empty cache.
    pub(crate) fn new(max_entries: NonZeroUsize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_entries.get()),
            max_entries,
            logical_clock: 0,
        }
    }

    /// Cache new entry.
    pub(crate) async fn cache(&mut self, k: &Arc<K>, ctx: &V::Context) -> DataFusionResult<V> {
        let addr = Arc::as_ptr(k).addr();

        // fast-path
        if let Some(entry) = self.cache.get_mut(&addr) {
            // cache HIT
            self.logical_clock += 1;
            entry.last_used = self.logical_clock;
            return Ok(entry.value.clone());
        }

        // cache MISS

        // only clean if we might need to evict
        if self.cache.len() >= self.max_entries.get() {
            // hope that we can make enough space by cleaning old entries
            self.clean(ctx).await?;
        }

        // ... but if not, we have to evict something else
        if self.cache.len() >= self.max_entries.get() {
            let to_delete_addr = *self
                .cache
                .iter()
                .min_by_key(|(_addr, entry)| entry.last_used)
                .expect("max_entries is NonZeroUsize")
                .0;
            let entry = self
                .cache
                .remove(&to_delete_addr)
                .expect("just found this entry");
            entry.value.clean(ctx).await?;
        }

        // now create new entry
        let value = V::new(k, ctx).await?;
        self.logical_clock += 1;
        self.cache.insert(
            addr,
            CacheEntry {
                key: Arc::downgrade(k),
                value: value.clone(),
                last_used: self.logical_clock,
            },
        );
        Ok(value)
    }

    /// Evict potentially unused entries.
    pub(crate) async fn clean(&mut self, ctx: &V::Context) -> DataFusionResult<()> {
        let mut to_clean = vec![];
        self.cache.retain(|_addr, entry| {
            if entry.key.strong_count() == 0 {
                to_clean.push(entry.value.clone());
                false
            } else {
                // keep
                true
            }
        });

        let mut errors = vec![];
        for v in to_clean {
            if let Err(e) = v.clean(ctx).await {
                errors.push(e);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else if errors.len() == 1 {
            Err(errors.pop().unwrap())
        } else {
            Err(DataFusionError::Collection(errors))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::DataFusionError;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// Unique ID generator for MockValue instances.
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    /// Mock context that tracks all `new` and `clean` calls.
    #[derive(Debug, Default)]
    struct MockContext {
        /// Key addresses that triggered creation.
        new_calls: Mutex<Vec<usize>>,
        /// Value IDs that were cleaned.
        clean_calls: Mutex<Vec<u64>>,
        /// Error injection for `new()`.
        fail_new: AtomicBool,
        /// Error injection for `clean()`.
        fail_clean: AtomicBool,
    }

    impl MockContext {
        fn new() -> Self {
            Self::default()
        }

        fn set_fail_new(&self, fail: bool) {
            self.fail_new.store(fail, Ordering::SeqCst);
        }

        fn set_fail_clean(&self, fail: bool) {
            self.fail_clean.store(fail, Ordering::SeqCst);
        }

        fn get_new_calls(&self) -> Vec<usize> {
            self.new_calls.lock().unwrap().clone()
        }

        fn get_clean_calls(&self) -> Vec<u64> {
            self.clean_calls.lock().unwrap().clone()
        }
    }

    /// Mock value implementing ResourceCacheValue<String>.
    #[derive(Debug, Clone)]
    struct MockValue {
        /// Unique identifier.
        id: u64,
        /// Address of the key it was created from.
        key_addr: usize,
    }

    impl ResourceCacheValue<String> for MockValue {
        type Context = MockContext;

        async fn new(k: &Arc<String>, ctx: &Self::Context) -> DataFusionResult<Self> {
            if ctx.fail_new.load(Ordering::SeqCst) {
                return Err(DataFusionError::Internal("mock new error".to_string()));
            }

            let key_addr = Arc::as_ptr(k).addr();
            ctx.new_calls.lock().unwrap().push(key_addr);

            Ok(Self {
                id: NEXT_ID.fetch_add(1, Ordering::SeqCst),
                key_addr,
            })
        }

        async fn clean(self, ctx: &Self::Context) -> DataFusionResult<()> {
            if ctx.fail_clean.load(Ordering::SeqCst) {
                return Err(DataFusionError::Internal("mock clean error".to_string()));
            }

            ctx.clean_calls.lock().unwrap().push(self.id);
            Ok(())
        }
    }

    // ==================== Basic Functionality ====================

    #[tokio::test]
    async fn test_cache_miss_creates_new_value() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(10).unwrap());

        let key = Arc::new("test".to_string());
        let value = cache.cache(&key, &ctx).await.unwrap();

        // Verify new was called
        let new_calls = ctx.get_new_calls();
        assert_eq!(new_calls.len(), 1);
        assert_eq!(new_calls[0], Arc::as_ptr(&key).addr());
        assert_eq!(value.key_addr, Arc::as_ptr(&key).addr());
    }

    #[tokio::test]
    async fn test_cache_hit_returns_same_value() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(10).unwrap());

        let key = Arc::new("test".to_string());
        let value1 = cache.cache(&key, &ctx).await.unwrap();
        let value2 = cache.cache(&key, &ctx).await.unwrap();

        // Same value returned (same id)
        assert_eq!(value1.id, value2.id);

        // new() called only once
        let new_calls = ctx.get_new_calls();
        assert_eq!(new_calls.len(), 1);
    }

    #[tokio::test]
    async fn test_different_keys_create_different_values() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(10).unwrap());

        let key1 = Arc::new("key1".to_string());
        let key2 = Arc::new("key2".to_string());

        let value1 = cache.cache(&key1, &ctx).await.unwrap();
        let value2 = cache.cache(&key2, &ctx).await.unwrap();

        // Different values created
        assert_ne!(value1.id, value2.id);

        // new() called twice
        let new_calls = ctx.get_new_calls();
        assert_eq!(new_calls.len(), 2);
    }

    // ==================== LRU Eviction ====================

    #[tokio::test]
    async fn test_lru_eviction_removes_oldest() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(2).unwrap());

        let key1 = Arc::new("key1".to_string());
        let key2 = Arc::new("key2".to_string());
        let key3 = Arc::new("key3".to_string());

        let value1 = cache.cache(&key1, &ctx).await.unwrap();
        let _value2 = cache.cache(&key2, &ctx).await.unwrap();

        // Adding key3 should evict key1 (oldest)
        let _value3 = cache.cache(&key3, &ctx).await.unwrap();

        // key1's value should have been cleaned
        let clean_calls = ctx.get_clean_calls();
        assert_eq!(clean_calls.len(), 1);
        assert_eq!(clean_calls[0], value1.id);
    }

    #[tokio::test]
    async fn test_access_updates_lru_timestamp() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(2).unwrap());

        let key1 = Arc::new("key1".to_string());
        let key2 = Arc::new("key2".to_string());
        let key3 = Arc::new("key3".to_string());

        let _value1 = cache.cache(&key1, &ctx).await.unwrap();
        let value2 = cache.cache(&key2, &ctx).await.unwrap();

        // Access key1 again to make it "newer"
        let _value1_again = cache.cache(&key1, &ctx).await.unwrap();

        // Adding key3 should now evict key2 (oldest by access time)
        let _value3 = cache.cache(&key3, &ctx).await.unwrap();

        let clean_calls = ctx.get_clean_calls();
        assert_eq!(clean_calls.len(), 1);
        assert_eq!(clean_calls[0], value2.id);
    }

    #[tokio::test]
    async fn test_single_entry_cache() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(1).unwrap());

        let key1 = Arc::new("key1".to_string());
        let key2 = Arc::new("key2".to_string());

        let value1 = cache.cache(&key1, &ctx).await.unwrap();
        let _value2 = cache.cache(&key2, &ctx).await.unwrap();

        // key1 should have been evicted
        let clean_calls = ctx.get_clean_calls();
        assert_eq!(clean_calls.len(), 1);
        assert_eq!(clean_calls[0], value1.id);

        // Cache should work with single entry
        let new_calls = ctx.get_new_calls();
        assert_eq!(new_calls.len(), 2);
    }

    // ==================== Weak Reference Cleanup ====================

    #[tokio::test]
    async fn test_clean_removes_entries_with_dropped_keys() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(10).unwrap());

        let value_id;
        {
            let key = Arc::new("temporary".to_string());
            let value = cache.cache(&key, &ctx).await.unwrap();
            value_id = value.id;
            // key dropped here
        }

        // Explicit clean should remove the entry
        cache.clean(&ctx).await.unwrap();

        let clean_calls = ctx.get_clean_calls();
        assert_eq!(clean_calls.len(), 1);
        assert_eq!(clean_calls[0], value_id);
    }

    #[tokio::test]
    async fn test_cache_auto_cleans_before_eviction() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(2).unwrap());

        let expired_value_id;
        {
            let temp_key = Arc::new("temp".to_string());
            let value = cache.cache(&temp_key, &ctx).await.unwrap();
            expired_value_id = value.id;
            // temp_key dropped here
        }

        let key1 = Arc::new("key1".to_string());
        let value1 = cache.cache(&key1, &ctx).await.unwrap();

        // Now cache has: 1 expired entry (temp) + 1 valid entry (key1)
        // Adding key2 should first clean expired entry, then add without evicting key1
        let key2 = Arc::new("key2".to_string());
        let _value2 = cache.cache(&key2, &ctx).await.unwrap();

        let clean_calls = ctx.get_clean_calls();
        // Only the expired entry should be cleaned, not value1
        assert_eq!(clean_calls.len(), 1);
        assert_eq!(clean_calls[0], expired_value_id);

        // value1 should still be accessible (not evicted)
        let value1_again = cache.cache(&key1, &ctx).await.unwrap();
        assert_eq!(value1.id, value1_again.id);
    }

    #[tokio::test]
    async fn test_expired_entry_makes_room_without_lru_eviction() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(1).unwrap());

        let expired_value_id;
        {
            let temp_key = Arc::new("temp".to_string());
            let value = cache.cache(&temp_key, &ctx).await.unwrap();
            expired_value_id = value.id;
            // temp_key dropped here
        }

        // Cache is "full" (1 entry) but that entry is expired
        // Adding new key should clean expired entry, not trigger LRU eviction
        let key = Arc::new("new_key".to_string());
        let _value = cache.cache(&key, &ctx).await.unwrap();

        let clean_calls = ctx.get_clean_calls();
        assert_eq!(clean_calls.len(), 1);
        assert_eq!(clean_calls[0], expired_value_id);
    }

    // ==================== Error Handling ====================

    #[tokio::test]
    async fn test_new_error_propagates() {
        let ctx = MockContext::new();
        ctx.set_fail_new(true);

        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(10).unwrap());

        let key = Arc::new("test".to_string());
        let result = cache.cache(&key, &ctx).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mock new error"));
    }

    #[tokio::test]
    async fn test_clean_error_propagates() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(1).unwrap());

        let key1 = Arc::new("key1".to_string());
        let value1 = cache.cache(&key1, &ctx).await.unwrap();

        // Enable failure for clean (which happens during eviction)
        ctx.set_fail_clean(true);

        let key2 = Arc::new("key2".to_string());
        let result = cache.cache(&key2, &ctx).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mock clean error"));

        // Verify the entry was still removed from cache despite the error
        ctx.set_fail_clean(false);
        let new_calls_before = ctx.get_new_calls().len();
        let value1_again = cache.cache(&key1, &ctx).await.unwrap();
        let new_calls_after = ctx.get_new_calls().len();

        // Should have created a new value (cache miss)
        assert_eq!(new_calls_after, new_calls_before + 1);
        assert_ne!(value1.id, value1_again.id);
    }

    #[tokio::test]
    async fn test_explicit_clean_error_propagates() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(10).unwrap());

        {
            let key = Arc::new("temp".to_string());
            let _value = cache.cache(&key, &ctx).await.unwrap();
            // key dropped here
        }

        ctx.set_fail_clean(true);

        let result = cache.clean(&ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mock clean error"));
    }

    // ==================== Edge Cases ====================

    #[tokio::test]
    async fn test_same_string_different_arc_is_cache_miss() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(10).unwrap());

        // Two Arcs with same string content but different addresses
        let key1 = Arc::new("same_content".to_string());
        let key2 = Arc::new("same_content".to_string());

        assert_ne!(Arc::as_ptr(&key1), Arc::as_ptr(&key2));

        let value1 = cache.cache(&key1, &ctx).await.unwrap();
        let value2 = cache.cache(&key2, &ctx).await.unwrap();

        // Different values because address-based keying
        assert_ne!(value1.id, value2.id);

        let new_calls = ctx.get_new_calls();
        assert_eq!(new_calls.len(), 2);
    }

    #[tokio::test]
    async fn test_many_sequential_accesses_same_key() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(10).unwrap());

        let key = Arc::new("test".to_string());
        let first_value = cache.cache(&key, &ctx).await.unwrap();

        // Access the same key many times
        for _ in 0..100 {
            let value = cache.cache(&key, &ctx).await.unwrap();
            assert_eq!(value.id, first_value.id);
        }

        // new() should only have been called once
        let new_calls = ctx.get_new_calls();
        assert_eq!(new_calls.len(), 1);
    }

    #[tokio::test]
    async fn test_lazy_cleaning_skips_when_below_capacity() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(3).unwrap());

        // Create an entry with a key that will be dropped
        let expired_value_id;
        {
            let temp_key = Arc::new("temp".to_string());
            let value = cache.cache(&temp_key, &ctx).await.unwrap();
            expired_value_id = value.id;
            // temp_key dropped here, entry is now expired
        }

        // Add a new entry while below capacity (1 entry, capacity 3)
        // With lazy cleaning, clean() should NOT be called
        let key1 = Arc::new("key1".to_string());
        let _value1 = cache.cache(&key1, &ctx).await.unwrap();

        // Verify the expired entry was NOT cleaned (lazy behavior)
        let clean_calls = ctx.get_clean_calls();
        assert_eq!(
            clean_calls.len(),
            0,
            "clean() should not be called when below capacity"
        );

        // Now explicitly clean and verify the expired entry is cleaned
        cache.clean(&ctx).await.unwrap();
        let clean_calls = ctx.get_clean_calls();
        assert_eq!(clean_calls.len(), 1);
        assert_eq!(clean_calls[0], expired_value_id);
    }

    #[tokio::test]
    async fn test_multiple_clean_errors_aggregated() {
        let ctx = MockContext::new();
        let mut cache: ResourceCache<String, MockValue> =
            ResourceCache::new(NonZeroUsize::new(10).unwrap());

        // Create multiple entries with keys that will be dropped
        {
            let key1 = Arc::new("temp1".to_string());
            let key2 = Arc::new("temp2".to_string());
            let key3 = Arc::new("temp3".to_string());
            let _v1 = cache.cache(&key1, &ctx).await.unwrap();
            let _v2 = cache.cache(&key2, &ctx).await.unwrap();
            let _v3 = cache.cache(&key3, &ctx).await.unwrap();
            // All keys dropped here
        }

        // Enable failure for clean
        ctx.set_fail_clean(true);

        // Clean should fail with multiple errors aggregated
        let result = cache.clean(&ctx).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            DataFusionError::Collection(errors) => {
                assert_eq!(errors.len(), 3);
                for e in &errors {
                    assert!(e.to_string().contains("mock clean error"));
                }
            }
            _ => panic!("Expected Collection error, got: {:?}", err),
        }
    }
}
