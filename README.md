# `blob_store`

A crate to abstract blob storage across RAM, local file system, and AWS S3.

```
blob_store/
├── src/
│   ├── lib.rs
│   └── object_store/
│       ├── local.rs         # Local filesystem backend
│       ├── memory.rs        # In-memory backend
│       ├── mod.rs           # ObjectStore trait and shared types
│       ├── s3.rs            # AWS S3 backend
│       └── test_helpers.rs  # Shared test logic for all backends
└── tests/
    └── s3_store.rs          # Integration tests
```

## Examples:

### In-memory store

```rust
use blob_store::object_store::{memory::InMemoryStore, ObjectStore, IfMatch};

let store = InMemoryStore::default();
let etag = store.put("hello.txt", b"Hello, world!", IfMatch::Any).unwrap();
let data = store.get("hello.txt").unwrap();
assert_eq!(data, Some(b"Hello, world!".to_vec()));
```
### Local file system


```rust

use blob_store::object_store::{local::LocalStore, ObjectStore, IfMatch};

let store = LocalStore::new("./data");
let etag = store.put("foo.txt", b"File contents", IfMatch::Any).unwrap();
```

### AWS S3

```rust
use blob_store::object_store::{s3::S3Store, ObjectStore, IfMatch};
use aws_config;
use aws_sdk_s3::Client;

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults().await;
    let client = Client::new(&config);
    let store = S3Store::new("your-bucket".to_string(), client);

    let etag = store.put("bar.txt", b"From S3!", IfMatch::Any).unwrap();
}
```

Before running the tests, configure the usual AWS environment (`aws config`), and set the environment variable `TEST_S3_BUCKET`.
