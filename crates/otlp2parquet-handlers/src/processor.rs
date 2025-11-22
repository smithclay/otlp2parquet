/// Result of processing a signal request
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessingResult {
    pub paths_written: Vec<String>,
    pub records_processed: usize,
    pub batches_flushed: usize,
}

/// Configuration for signal processing
pub struct ProcessorConfig<'a> {
    pub catalog: Option<&'a dyn otlp2parquet_writer::icepick::catalog::Catalog>,
    pub namespace: &'a str,
    pub snapshot_timestamp_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processing_result_creation() {
        let result = ProcessingResult {
            paths_written: vec!["path1".to_string(), "path2".to_string()],
            records_processed: 100,
            batches_flushed: 2,
        };

        assert_eq!(result.paths_written.len(), 2);
        assert_eq!(result.records_processed, 100);
        assert_eq!(result.batches_flushed, 2);
    }
}
