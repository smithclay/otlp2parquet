#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Server,
}

impl Platform {
    /// Auto-detect the current platform.
    pub fn detect() -> Self {
        Platform::Server
    }

    /// Get platform-specific defaults
    pub fn defaults(&self) -> PlatformDefaults {
        match self {
            Platform::Server => PlatformDefaults {
                batch_max_rows: 200_000,
                batch_max_bytes: 128 * 1024 * 1024, // 128 MB
                batch_max_age_secs: 10,
                max_payload_bytes: 8 * 1024 * 1024, // 8 MB
                storage_backend: "fs",
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlatformDefaults {
    pub batch_max_rows: usize,
    pub batch_max_bytes: usize,
    pub batch_max_age_secs: u64,
    pub max_payload_bytes: usize,
    pub storage_backend: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_defaults() {
        let server = Platform::Server.defaults();
        assert_eq!(server.batch_max_rows, 200_000);
        assert_eq!(server.storage_backend, "fs");
    }
}
