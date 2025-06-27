use super::{IfMatch, ObjectStore, ObjectStoreError, Result};
use std::fs::{self, File};
use std::io::{Write};
use std::path::{Path, PathBuf};

use md5;

pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new<P: AsRef<Path>>(root: P) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn object_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    fn compute_etag(data: &[u8]) -> String {
        format!("{:x}", md5::compute(data))
    }
}

impl ObjectStore for LocalStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.object_path(key);
        match fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ObjectStoreError::Io(e)),
        }
    }

    fn put(&self, key: &str, body: &[u8], cond: IfMatch) -> Result<String> {
        let path = self.object_path(key);

        // Check preconditions
        match cond {
            IfMatch::Any => { /* always write */ }
            IfMatch::Tag(expected_etag) => {
                let existing = self.get(key)?;
                match existing {
                    Some(data) => {
                        let etag = Self::compute_etag(&data);
                        if etag != expected_etag {
                            return Err(ObjectStoreError::PreconditionFailed);
                        }
                    }
                    None => return Err(ObjectStoreError::PreconditionFailed),
                }
            }
            IfMatch::NoneMatch => {
                if self.get(key)?.is_some() {
                    return Err(ObjectStoreError::PreconditionFailed);
                }
            }
        }

        // Ensure parent directories exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(ObjectStoreError::Io)?;
        }

        // Write the file
        let mut file = File::create(&path).map_err(ObjectStoreError::Io)?;
        file.write_all(body).map_err(ObjectStoreError::Io)?;

        let etag = Self::compute_etag(body);
        Ok(etag)
    }

    fn list(&self, prefix: &str, continuation: Option<String>) -> Result<(Vec<String>, Option<String>)> {
        let mut keys = Vec::new();

        // Recursively walk the directory tree
        if self.root.exists() {
            for entry in walkdir::WalkDir::new(&self.root)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
            {
                let rel_path = entry.path().strip_prefix(&self.root).unwrap().to_string_lossy().to_string();
                if rel_path.starts_with(prefix) {
                    keys.push(rel_path);
                }
            }
        }

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
    use tempfile::TempDir;
    use std::fs;
    use std::panic;
    use uuid::Uuid;

    fn setup_store() -> (LocalStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::new(tmp.path());
        (store, tmp)
    }

    #[test]
    fn test_put_and_get() {
        let (store, _tmp) = setup_store();
        let etag = store.put("foo.txt", b"hello", IfMatch::Any).unwrap();
        assert!(!etag.is_empty());

        let data = store.get("foo.txt").unwrap();
        assert_eq!(data, Some(b"hello".to_vec()));
    }

    #[test]
    fn test_get_nonexistent() {
        let (store, _tmp) = setup_store();
        assert_eq!(store.get("nope.txt").unwrap(), None);
    }

    #[test]
    fn test_conditional_put_tag_match() {
        let (store, _tmp) = setup_store();
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
        let (store, _tmp) = setup_store();
        store.put("baz.txt", b"one", IfMatch::Any).unwrap();

        // Should fail: ETag does not match
        let result = store.put("baz.txt", b"two", IfMatch::Tag("wrong-etag"));
        assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));
    }

    #[test]
    fn test_conditional_put_none_match() {
        let (store, _tmp) = setup_store();

        // Should succeed: does not exist
        let etag = store.put("new.txt", b"data", IfMatch::NoneMatch).unwrap();
        assert!(!etag.is_empty());

        // Should fail: already exists
        let result = store.put("new.txt", b"data", IfMatch::NoneMatch);
        assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));
    }

    #[test]
    fn test_list_and_pagination() {
        let (store, _tmp) = setup_store();

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
    fn test_parent_dirs_created() {
        let (store, tmp) = setup_store();
        let nested_path = "nested/dir/structure/file.txt";
        let etag = store.put(nested_path, b"deep", IfMatch::Any).unwrap();
        assert!(!etag.is_empty());
        let data = store.get(nested_path).unwrap();
        assert_eq!(data, Some(b"deep".to_vec()));

        // Check that the file actually exists on disk
        let on_disk = fs::read(tmp.path().join(nested_path)).unwrap();
        assert_eq!(on_disk, b"deep");

        let long_key_result = panic::catch_unwind(|| {
            // The code that is expected to panic
            let long_key = format!("{}{}", "local_test", "a".repeat(512));
            let _ = store.put(&long_key, b"long", IfMatch::Any).unwrap();
        });

        assert!(long_key_result.is_err(), "Expected a panic for long file name");
    }

    // this is more of a test of genericness of
    // the trait implementation
    #[test]
    fn test_local_object_store() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::new(tmp.path());
        let prefix = format!("test/{}/", Uuid::new_v4());
        run_object_store_tests(&store, &prefix);
    }

}
