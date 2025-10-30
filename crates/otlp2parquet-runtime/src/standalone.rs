// Standalone runtime for local development and testing
//
// Uses OpenDAL filesystem with async I/O
//
// Philosophy: Leverage OpenDAL's mature filesystem abstraction
// Uses tokio for async runtime (consistent with Lambda/CF Workers)

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[cfg(feature = "standalone")]
use crate::opendal_storage::OpenDalStorage;

/// Handle a single HTTP request
async fn handle_request(mut stream: TcpStream, storage: &OpenDalStorage) -> Result<()> {
    let mut reader = BufReader::new(&mut stream);

    // Read request line
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        send_response(&mut stream, 400, "Bad Request", b"Invalid request").await?;
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    // Read headers to get Content-Length
    let mut content_length = 0;
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line).await?;
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
            reader.read_exact(&mut body).await?;

            // Process OTLP logs (PURE - no I/O, deterministic)
            let mut parquet_bytes = Vec::new();
            let metadata = otlp2parquet_core::process_otlp_logs_into(&body, &mut parquet_bytes)
                .context("Failed to process OTLP logs")?;

            // Generate partition path (ACCIDENT - platform-specific storage decision)
            let partition_path = crate::partition::generate_partition_path(
                &metadata.service_name,
                metadata.first_timestamp_nanos,
            );

            // Write to OpenDAL filesystem storage (async)
            storage
                .write(&partition_path, parquet_bytes)
                .await
                .context("Failed to write to storage")?;

            // Return success response
            send_response(&mut stream, 200, "OK", b"{\"status\":\"ok\"}").await?;
        }
        ("GET", "/health") => {
            send_response(&mut stream, 200, "OK", b"Healthy").await?;
        }
        _ => {
            send_response(&mut stream, 404, "Not Found", b"Not found").await?;
        }
    }

    Ok(())
}

/// Send HTTP response
async fn send_response(
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
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

/// Entry point for standalone mode
#[cfg(feature = "standalone")]
pub async fn run() -> Result<()> {
    println!("Running in standalone mode with OpenDAL filesystem");
    println!("Using async I/O with tokio runtime");

    // Get configuration from environment
    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let storage_path = std::env::var("STORAGE_PATH").unwrap_or_else(|_| "./data".to_string());

    // Initialize OpenDAL filesystem storage
    let storage = OpenDalStorage::new_fs(&storage_path)
        .context("Failed to initialize OpenDAL filesystem storage")?;
    println!("Storage path: {}", storage_path);

    // Start HTTP server
    let listener = TcpListener::bind(&addr)
        .await
        .context(format!("Failed to bind to {}", addr))?;

    println!("OTLP HTTP endpoint listening on http://{}", addr);
    println!("POST logs to http://{}/v1/logs", addr);
    println!("Press Ctrl+C to stop");

    // Accept connections
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                // Spawn a new task for each connection
                let storage_clone = storage.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_request(stream, &storage_clone).await {
                        eprintln!("Error handling request: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}
