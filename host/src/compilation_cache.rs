use std::{borrow::Cow, collections::HashMap, sync::RwLock};

use wasmtime::CacheStore;

#[derive(Debug, Default)]
pub(crate) struct CompilationCache {
    data: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl CacheStore for CompilationCache {
    fn get(&self, key: &[u8]) -> Option<Cow<'_, [u8]>> {
        let guard = self.data.read().expect("not poisoned");
        guard.get(key).map(|data| Cow::Owned(data.clone()))
    }

    fn insert(&self, key: &[u8], value: Vec<u8>) -> bool {
        let mut guard = self.data.write().expect("not poisoned");
        guard.insert(key.to_owned(), value.to_owned());

        // always succeeds
        true
    }
}
