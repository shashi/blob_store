use super::{IfMatch, ObjectStore, ObjectStoreError, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct InMemoryStore {
    // Key: object key, Value: (data, etag)
    map: Arc<Mutex<HashMap<String, (Vec<u8>, String)>>>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        InMemoryStore {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl InMemoryStore {
    fn compute_etag(data: &[u8]) -> String {
        format!("{:x}", md5::compute(data))
    }
}

impl ObjectStore for InMemoryStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let map = self.map.lock().unwrap();
        Ok(map.get(key).map(|(data, _)| data.clone()))
    }

    fn put(&self, key: &str, body: &[u8], cond: IfMatch) -> Result<String> {
        let mut map = self.map.lock().unwrap();
        let new_etag = Self::compute_etag(body);

        match cond {
            IfMatch::Any => {
                map.insert(key.to_string(), (body.to_vec(), new_etag.clone()));
                Ok(new_etag)
            }
            IfMatch::Tag(expected_etag) => {
                if let Some((_, etag)) = map.get(key) {
                    if etag == expected_etag {
                        map.insert(key.to_string(), (body.to_vec(), new_etag.clone()));
                        Ok(new_etag)
                    } else {
                        Err(ObjectStoreError::PreconditionFailed)
                    }
                } else {
                    Err(ObjectStoreError::PreconditionFailed)
                }
            }
            IfMatch::NoneMatch => {
                if map.contains_key(key) {
                    Err(ObjectStoreError::PreconditionFailed)
                } else {
                    map.insert(key.to_string(), (body.to_vec(), new_etag.clone()));
                    Ok(new_etag)
                }
            }
        }
    }

    fn list(&self, prefix: &str, continuation: Option<String>) -> Result<(Vec<String>, Option<String>)> {
        let map = self.map.lock().unwrap();
        let mut keys: Vec<String> = map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        keys.sort();

        // Simple pagination: 1000 per page
        let page_size = 1000;
        let start = continuation
            .and_then(|token| keys.iter().position(|k| k > &token))
            .unwrap_or(0);
        let end = (start + page_size).min(keys.len());

        let next_token = if end < keys.len() {
            Some(keys[end - 1].clone())
        } else {
            None
        };

        Ok((keys[start..end].to_vec(), next_token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::{IfMatch, ObjectStore};
    use crate::object_store::test_helpers::tests::run_object_store_tests;
    use uuid::Uuid;

    #[test]
    fn test_put_and_get() {
        let store = InMemoryStore::default();
        let etag = store.put("foo.txt", b"hello", IfMatch::Any).unwrap();
        assert!(!etag.is_empty());

        let data = store.get("foo.txt").unwrap();
        assert_eq!(data, Some(b"hello".to_vec()));
    }

    #[test]
    fn test_get_nonexistent() {
        let store = InMemoryStore::default();
        assert_eq!(store.get("nope.txt").unwrap(), None);
    }

    #[test]
    fn test_conditional_put_tag_match() {
        let store = InMemoryStore::default();
        let etag1 = store.put("bar.txt", b"one", IfMatch::Any).unwrap();

        // Should succeed: ETag matches
        let etag2 = store.put("bar.txt", b"two", IfMatch::Tag(&etag1)).unwrap();
        assert_ne!(etag1, etag2);

        // Data updated
        let data = store.get("bar.txt").unwrap();
        assert_eq!(data, Some(b"two".to_vec()));
    }

    #[test]
    fn test_conditional_put_tag_mismatch() {
        let store = InMemoryStore::default();
        store.put("baz.txt", b"one", IfMatch::Any).unwrap();

        // Should fail: ETag does not match
        let result = store.put("baz.txt", b"two", IfMatch::Tag("wrong-etag"));
        assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));
    }

    #[test]
    fn test_conditional_put_none_match() {
        let store = InMemoryStore::default();

        // Should succeed: does not exist
        let etag = store.put("new.txt", b"data", IfMatch::NoneMatch).unwrap();
        assert!(!etag.is_empty());

        // Should fail: already exists
        let result = store.put("new.txt", b"data", IfMatch::NoneMatch);
        assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));
    }

    #[test]
    fn test_list_and_pagination() {
        let store = InMemoryStore::default();

        // Add 3 objects with a common prefix
        store.put("folder/a.txt", b"a", IfMatch::Any).unwrap();
        store.put("folder/b.txt", b"b", IfMatch::Any).unwrap();
        store.put("folder/c.txt", b"c", IfMatch::Any).unwrap();

        let (keys, next) = store.list("folder/", None).unwrap();
        assert_eq!(keys.len(), 3);
        assert!(next.is_none());
        assert!(keys.contains(&"folder/a.txt".to_string()));
        assert!(keys.contains(&"folder/b.txt".to_string()));
        assert!(keys.contains(&"folder/c.txt".to_string()));
    }

    #[test]
    fn test_in_memory_object_store() {
        let store = InMemoryStore::default();
        let prefix = format!("test/{}/", Uuid::new_v4());
        run_object_store_tests(&store, &prefix);
    }
}
