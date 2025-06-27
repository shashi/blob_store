use super::{IfMatch, ObjectStore, ObjectStoreError, Result};
use aws_sdk_s3::{Client};
use aws_sdk_s3::primitives::ByteStream;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct S3Store {
    client: Arc<Client>,
    bucket: String,
    rt: Arc<Runtime>,
}

impl S3Store {
    pub fn new(bucket: String, client: Client) -> Self {
        let rt = Arc::new(
            Runtime::new().expect("Failed to create Tokio runtime")
        );
        Self {
            client: Arc::new(client),
            bucket,
            rt,
        }
    }

    fn compute_etag(data: &[u8]) -> String {
        format!("{:x}", md5::compute(data))
    }
}

impl ObjectStore for S3Store {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();

        let result = self.rt.block_on(async move {
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await;

            match resp {
                Ok(obj) => {
                    let data = obj.body.collect().await
                        .map_err(|e| ObjectStoreError::Other(format!("S3 body error: {e}")))?;
                    Ok(Some(data.into_bytes().to_vec()))
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("NoSuchKey") {
                        Ok(None)
                    } else {
                        Err(ObjectStoreError::Other(format!("S3 error: {e}")))
                    }
                }
            }
        });

        result
    }

    fn put(&self, key: &str, body: &[u8], cond: IfMatch) -> Result<String> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();
        let body_vec = body.to_vec();
        let etag = Self::compute_etag(body);

        let result = self.rt.block_on(async move {
            // Handle preconditions
            match cond {
                IfMatch::Any => {
                    // Always write
                }
                IfMatch::Tag(expected_etag) => {
                    // Fetch current ETag
                    let head = client.head_object().bucket(&bucket).key(&key).send().await;
                    let current_etag = match head {
                        Ok(meta) => meta.e_tag().map(|s| s.trim_matches('"').to_string()),
                        Err(_) => None,
                    };
                    if current_etag.as_deref() != Some(expected_etag) {
                        return Err(ObjectStoreError::PreconditionFailed);
                    }
                }
                IfMatch::NoneMatch => {
                    let head = client.head_object().bucket(&bucket).key(&key).send().await;
                    if head.is_ok() {
                        return Err(ObjectStoreError::PreconditionFailed);
                    }
                }
            }

            // Upload
            let resp = client
                .put_object()
                .bucket(&bucket)
                .key(&key)
                .body(ByteStream::from(body_vec))
                .send()
                .await
                .map_err(|e| ObjectStoreError::Other(format!("S3 put error: {e}")))?;

            // S3 returns ETag as a quoted string
            let s3_etag = resp.e_tag().map(|s| s.trim_matches('"').to_string()).unwrap_or(etag);

            Ok(s3_etag)
        });

        result
    }

    fn list(&self, prefix: &str, continuation: Option<String>) -> Result<(Vec<String>, Option<String>)> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let prefix = prefix.to_string();
        let continuation_token = continuation.clone();

        let result = self.rt.block_on(async move {
            let mut req = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix);

            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send()
                .await
                .map_err(|e| ObjectStoreError::Other(format!("S3 list error: {e}")))?;

            let keys = resp
                .contents()
                .iter()
                .filter_map(|obj| obj.key().map(|s| s.to_string()))
                .collect::<Vec<_>>();

            let next_token = resp.next_continuation_token().map(|s| s.to_string());

            Ok((keys, next_token))
        });

        result
    }
}


