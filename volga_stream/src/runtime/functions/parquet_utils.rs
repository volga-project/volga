use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

pub fn build_object_store(
    path: &str,
    storage_options: &HashMap<String, String>,
    allow_file_path: bool,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    if path.starts_with("s3://") {
        let (bucket, prefix) = split_s3_path(path)?;
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket);
        if let Some(region) = storage_options.get("region") {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = storage_options.get("endpoint_url") {
            builder = builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }
        if let (Some(key), Some(secret)) = (
            storage_options.get("access_key_id"),
            storage_options.get("secret_access_key"),
        ) {
            builder = builder.with_access_key_id(key).with_secret_access_key(secret);
        }
        let store = builder.build()?;
        let store = Arc::new(store) as Arc<dyn ObjectStore>;
        Ok((store, ObjectPath::from(prefix)))
    } else {
        let local_path = if let Some(stripped) = path.strip_prefix("file://") {
            stripped
        } else {
            path
        };
        let local_path = PathBuf::from(local_path);
        if allow_file_path && local_path.is_file() {
            let parent = local_path.parent().ok_or_else(|| anyhow!("invalid file path"))?;
            let store = object_store::local::LocalFileSystem::new_with_prefix(parent)?;
            let prefix = local_path
                .file_name()
                .ok_or_else(|| anyhow!("invalid file path"))?
                .to_string_lossy()
                .to_string();
            let store = Arc::new(store) as Arc<dyn ObjectStore>;
            Ok((store, ObjectPath::from(prefix)))
        } else {
            let store = object_store::local::LocalFileSystem::new_with_prefix(&local_path)?;
            let store = Arc::new(store) as Arc<dyn ObjectStore>;
            Ok((store, ObjectPath::from("")))
        }
    }
}

fn split_s3_path(path: &str) -> Result<(String, String)> {
    let stripped = path.trim_start_matches("s3://");
    let mut parts = stripped.splitn(2, '/');
    let bucket = parts.next().ok_or_else(|| anyhow!("missing s3 bucket"))?.to_string();
    let prefix = parts.next().unwrap_or("").to_string();
    Ok((bucket, prefix))
}
