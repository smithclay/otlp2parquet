use url::Url;

/// Join a table location with a relative suffix to produce the catalog-visible path.
pub fn catalog_path(base_location: &str, suffix: &str) -> String {
    let base = base_location.trim_end_matches('/');
    let suffix = suffix.trim_start_matches('/');
    if base.is_empty() {
        suffix.to_string()
    } else if suffix.is_empty() {
        base.to_string()
    } else {
        format!("{}/{}", base, suffix)
    }
}

/// Convert an absolute URI-style path into the storage-relative key expected by OpenDAL.
pub fn storage_key_from_path(path: &str) -> String {
    if let Ok(url) = Url::parse(path) {
        let key = url.path().trim_start_matches('/').to_string();
        if key.is_empty() {
            // For schemes where the path component is empty, fall back to host if present.
            if let Some(host) = url.host_str() {
                return host.to_string();
            }
        }
        key
    } else {
        path.trim_start_matches('/').to_string()
    }
}
