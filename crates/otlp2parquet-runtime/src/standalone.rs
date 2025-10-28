// Standalone runtime for local development and testing
//
// Uses blocking I/O and local filesystem
//
// Philosophy: Simple, single-threaded, blocking I/O
// No tokio needed - just std::fs and std::net

use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;

pub struct FilesystemStorage {
    base_path: PathBuf,
}

impl FilesystemStorage {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    /// Write data to local filesystem (blocking)
    pub fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        let mut file = BufWriter::new(self.create_file(path)?);
        file.write_all(data)?;
        file.flush()?;
        Ok(())
    }

    /// Create (and truncate) a file for writing, ensuring parent directories exist.
    pub fn create_file(&self, path: &str) -> Result<File> {
        let full_path = self.base_path.join(path);

        // Create parent directories
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }

        Ok(File::create(&full_path)?)
    }

    /// Read data from local filesystem (blocking)
    pub fn read(&self, path: &str) -> Result<Vec<u8>> {
        let full_path = self.base_path.join(path);
        let data = fs::read(&full_path)?;
        Ok(data)
    }
}

/// Handle a single HTTP request
fn handle_request(mut stream: TcpStream, storage: &FilesystemStorage) -> Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);

    // Read request line
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        send_response(&mut stream, 400, "Bad Request", b"Invalid request")?;
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    // Read headers to get Content-Length
    let mut content_length = 0;
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            break;
        }
        if line.to_lowercase().starts_with("content-length:") {
            if let Some(len_str) = line.split(':').nth(1) {
                content_length = len_str.trim().parse().unwrap_or(0);
            }
        }
    }

    // Route requests
    match (method, path) {
        ("POST", "/v1/logs") => {
            // Read request body
            let mut body = vec![0u8; content_length];
            std::io::Read::read_exact(&mut reader, &mut body)?;

            // Process OTLP logs into Arrow to capture metadata before writing
            let (batch, metadata) = otlp2parquet_core::otlp_to_record_batch(&body)
                .context("Failed to process OTLP logs")?;

            // Generate partition path (ACCIDENT - platform-specific storage decision)
            let path = crate::partition::generate_partition_path(
                &metadata.service_name,
                metadata.first_timestamp_nanos,
            );

            // Stream Parquet directly to filesystem without additional buffering
            let file = storage
                .create_file(&path)
                .context("Failed to create destination file")?;
            let mut writer = BufWriter::new(file);
            otlp2parquet_core::parquet::write_parquet_into(&batch, &mut writer)
                .context("Failed to write Parquet data")?;
            writer.flush()?;

            // Return success response
            send_response(&mut stream, 200, "OK", b"{\"status\":\"ok\"}")?;
        }
        ("GET", "/health") => {
            send_response(&mut stream, 200, "OK", b"Healthy")?;
        }
        _ => {
            send_response(&mut stream, 404, "Not Found", b"Not found")?;
        }
    }

    Ok(())
}

/// Send HTTP response
fn send_response(
    stream: &mut TcpStream,
    status: u16,
    status_text: &str,
    body: &[u8],
) -> Result<()> {
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n",
        status,
        status_text,
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

/// Entry point for standalone mode
pub fn run() -> Result<()> {
    println!("Running in standalone mode");
    println!("Using blocking I/O - no tokio, simple and direct");

    // Get configuration from environment
    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let storage_path = std::env::var("STORAGE_PATH").unwrap_or_else(|_| "./data".to_string());

    // Initialize storage
    let storage = FilesystemStorage::new(PathBuf::from(storage_path));
    println!("Storage path: {}", storage.base_path.display());

    // Start HTTP server
    let listener = TcpListener::bind(&addr).context(format!("Failed to bind to {}", addr))?;

    println!("OTLP HTTP endpoint listening on http://{}", addr);
    println!("POST logs to http://{}/v1/logs", addr);
    println!("Press Ctrl+C to stop");

    // Accept connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(e) = handle_request(stream, &storage) {
                    eprintln!("Error handling request: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }

    Ok(())
}
