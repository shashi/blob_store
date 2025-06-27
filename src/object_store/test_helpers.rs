pub mod tests {
    use super::*;
    use crate::object_store::{ObjectStore, IfMatch, ObjectStoreError};
    use uuid::Uuid;

    // Generic tests for any ObjectStore implementation
    pub fn run_object_store_tests(store: &dyn ObjectStore, prefix: &str) {
        use crate::object_store::{IfMatch, ObjectStoreError};

        // 1. Put and get normal value
        let key = format!("{}foo.txt", prefix);
        let etag = store.put(&key, b"hello", IfMatch::Any).unwrap();
        assert!(!etag.is_empty());
        let data = store.get(&key).unwrap();
        assert_eq!(data, Some(b"hello".to_vec()));

        // 2. Overwrite with same value (ETag should be same)
        let etag2 = store.put(&key, b"hello", IfMatch::Any).unwrap();
        assert_eq!(etag, etag2);

        // 3. Overwrite with different value (ETag should change)
        let etag3 = store.put(&key, b"world", IfMatch::Any).unwrap();
        assert_ne!(etag2, etag3);

        // 4. Conditional put: Tag match (should succeed)
        let etag4 = store.put(&key, b"world2", IfMatch::Tag(&etag3)).unwrap();
        assert_ne!(etag3, etag4);

        // 5. Conditional put: Tag mismatch (should fail)
        let result = store.put(&key, b"fail", IfMatch::Tag("wrong-etag"));
        assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));

        // 6. Conditional put: NoneMatch (should fail if exists)
        let result = store.put(&key, b"fail", IfMatch::NoneMatch);
        assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));

        // 7. Conditional put: NoneMatch (should succeed if not exists)
        let key2 = format!("{}bar.txt", prefix);
        let etag5 = store.put(&key2, b"new", IfMatch::NoneMatch).unwrap();
        assert!(!etag5.is_empty());

        // 8. Get non-existent key
        let missing = store.get(&format!("{}doesnotexist", prefix)).unwrap();
        assert!(missing.is_none());

        // 9. List with no matching keys
        let (empty_list, _) = store.list(&format!("{}no_such_prefix/", prefix), None).unwrap();
        assert!(empty_list.is_empty());

        // 10. List with prefix matching multiple keys
        let (keys, _next) = store.list(prefix, None).unwrap();
        assert!(keys.contains(&key));
        assert!(keys.contains(&key2));

        // 11. Empty key (if supported)
        let empty_key = format!("{}emptykey", prefix);
        let etag_empty = store.put(&empty_key, b"", IfMatch::Any).unwrap();
        let data_empty = store.get(&empty_key).unwrap();
        assert_eq!(data_empty, Some(Vec::new()));

        // 12. Unicode/special character key
        let special_key = format!("{}spécial-字符-!@#.bin", prefix);
        let etag_special = store.put(&special_key, b"special", IfMatch::Any).unwrap();
        let data_special = store.get(&special_key).unwrap();
        assert_eq!(data_special, Some(b"special".to_vec()));

        // 13. Very long key (adjust length as per backend limits)
        let long_key = format!("{}{}", prefix, "a".repeat(512));
        let etag_long = store.put(&long_key, b"long", IfMatch::Any).unwrap();
        let data_long = store.get(&long_key).unwrap();
        assert_eq!(data_long, Some(b"long".to_vec()));

        // 14. Non-UTF8 binary data
        let bin_key = format!("{}bin", prefix);
        let bin_data = vec![0, 159, 146, 150, 255, 0, 1, 2, 3];
        let etag_bin = store.put(&bin_key, &bin_data, IfMatch::Any).unwrap();
        let data_bin = store.get(&bin_key).unwrap();
        assert_eq!(data_bin, Some(bin_data));

        // 15. Large blob (adjust size as appropriate for backend, e.g. 1MB)
        let large_key = format!("{}large", prefix);
        let large_blob = vec![42u8; 1024 * 1024]; // 1MB
        let etag_large = store.put(&large_key, &large_blob, IfMatch::Any).unwrap();
        let data_large = store.get(&large_key).unwrap();
        assert_eq!(data_large, Some(large_blob));

        // 16. List after many inserts
        let (all_keys, _) = store.list(prefix, None).unwrap();
        assert!(all_keys.contains(&key));
        assert!(all_keys.contains(&key2));
        assert!(all_keys.contains(&empty_key));
        assert!(all_keys.contains(&special_key));
        assert!(all_keys.contains(&long_key));
        assert!(all_keys.contains(&bin_key));
        assert!(all_keys.contains(&large_key));
    }
}
