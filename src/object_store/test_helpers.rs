pub mod tests {
    use super::*;
    use crate::object_store::{ObjectStore, IfMatch, ObjectStoreError};
    use uuid::Uuid;

    // Helper to generate a unique prefix for each test run
    fn unique_prefix() -> String {
        format!("test/{}/", Uuid::new_v4())
    }

    // Generic tests for any ObjectStore implementation
    pub fn run_object_store_tests(store: &dyn ObjectStore, prefix: &str) {
        // Put and get
        let key = format!("{}foo.txt", prefix);
        let etag = store.put(&key, b"hello", IfMatch::Any).unwrap();
        assert!(!etag.is_empty());
        let data = store.get(&key).unwrap();
        assert_eq!(data, Some(b"hello".to_vec()));

        // Conditional put: Tag match
        let etag2 = store.put(&key, b"world", IfMatch::Tag(&etag)).unwrap();
        assert_ne!(etag, etag2);

        // Conditional put: Tag mismatch
        let result = store.put(&key, b"fail", IfMatch::Tag("wrong-etag"));
        assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));

        // Conditional put: NoneMatch
        let key2 = format!("{}bar.txt", prefix);
        let etag3 = store.put(&key2, b"new", IfMatch::NoneMatch).unwrap();
        assert!(!etag3.is_empty());
        let result = store.put(&key2, b"fail", IfMatch::NoneMatch);
        assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));

        // List
        let (keys, _next) = store.list(prefix, None).unwrap();
        assert!(keys.contains(&key));
        assert!(keys.contains(&key2));
    }
}
