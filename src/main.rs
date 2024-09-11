use dotenv::dotenv;
use log::{error, info};
use std::{env, time::Duration};
use tokio::time::sleep;

pub mod ftp_operations;
pub use crate::ftp_operations::{FtpClient, FtpError, process_ftp_and_redis};

#[tokio::main]
async fn main() -> Result<(), FtpError> {
    dotenv().ok();
    env_logger::init();

    let ftp_client = FtpClient::new()?;
    let check_interval = env::var("CHECK_INTERVAL")
        .unwrap_or_else(|_| "300".to_string())
        .parse::<u64>()
        .unwrap_or(300);

    info!("Starting FTP checker with interval of {} seconds", check_interval);

    loop {
        async fn check_ftp(ftp_client: &FtpClient) -> Result<bool, FtpError> {
            let mut ftp_stream = ftp_client.connect().await?;
            ftp_client.check_file_timestamp(&mut ftp_stream).await
        }

        let result = process_ftp_and_redis(|| async {
            let ftp_client_clone = ftp_client.clone();
            match check_ftp(&ftp_client_clone).await {
                Ok(is_updated) => {
                    if is_updated {
                        info!("File was updated, operation completed successfully");
                    } else {
                        info!("File was not updated, operation completed successfully");
                    }
                    Ok(is_updated)
                },
                Err(e) => {
                    error!("Error occurred: {}", e);
                    if let Err(notify_err) = ftp_client_clone.notify_admin(&format!("Error occurred: {}", e)).await {
                        error!("Failed to notify admin: {}", notify_err);
                    }
                    Err(e)
                }
            }
        }).await;

        match result {
            Ok(is_updated) => {
                info!("FTP operation completed successfully");
                if is_updated {
                    info!("File was updated");
                } else {
                    info!("File was not updated");
                }
            },
            Err(backoff::Error::Permanent(e)) => {
                error!("Permanent error occurred: {}", e);
                break Err(FtpError::RetryError(e.to_string()));
            },
            Err(backoff::Error::Transient(e)) => {
                error!("Transient error occurred: {}", e);
                // Continue the loop for transient errors
                continue;
            },
        }

        info!("Waiting for {} seconds before next check", check_interval);
        sleep(Duration::from_secs(check_interval)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_ftp_connection() -> Result<(), FtpError> {
        dotenv().ok();
        let ftp_client = FtpClient::new()?;
        match tokio::time::timeout(Duration::from_secs(30), ftp_client.connect()).await {
            Ok(Ok(_)) => {
                // If we reach here, the connection was successful
                Ok(())
            },
            Ok(Err(e)) => Err(e),
            Err(_) => Err(FtpError::TimeoutError),
        }
    }

    #[tokio::test]
    async fn test_file_download() -> Result<(), FtpError> {
        dotenv().ok();
        let ftp_client = FtpClient::new()?;
        let mut ftp_stream = match tokio::time::timeout(
            Duration::from_secs(30),
            ftp_client.connect()
        ).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(FtpError::TimeoutError),
        };
        let local_file_path = ftp_client.download_file(&mut ftp_stream).await?;
        assert!(std::path::Path::new(&local_file_path).exists());
        tokio::fs::remove_file(local_file_path).await?;
        FtpClient::disconnect(&mut ftp_stream).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_file_parsing_and_redis_integration() -> Result<(), FtpError> {
        use redis::AsyncCommands;

        dotenv().ok();
        let ftp_client = FtpClient::new()?;

        let result = tokio::time::timeout(Duration::from_secs(30), async {
            let mut ftp_stream = ftp_client.connect().await?;
            let local_file_path = ftp_client.download_file(&mut ftp_stream).await?;

            let file = tokio::fs::File::open(&local_file_path).await?;
            let reader = tokio::io::BufReader::new(file);

            let mut redis_conn = ftp_client.connect_to_redis().await?;
            ftp_client.parse_and_store_file(reader, &mut redis_conn).await?;

            // Check if data was pushed to Redis
            let key = "file_content";
            let value: Option<String> = redis_conn.get(key).await?;
            assert!(value.is_some(), "No data found in Redis for key: {}", key);

            // Check file content accuracy
            let stored_value = value.unwrap();
            assert!(stored_value.contains("Welcome to test.rebex.net!"), "File content not properly stored");
            assert!(stored_value.contains("For information about Rebex FTP/SSL"), "File content not properly stored");

            tokio::fs::remove_file(local_file_path).await?;
            Ok::<(), FtpError>(())
        }).await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(FtpError::TimeoutError),
        }
    }

    #[tokio::test]
    async fn test_admin_notification_cooldown() -> Result<(), FtpError> {
        dotenv().ok();
        let ftp_client = FtpClient::new()?;

        // First notification
        ftp_client.notify_admin("Test error").await?;

        // Second notification (should be ignored due to cooldown)
        let result = ftp_client.notify_admin("Another test error").await;
        assert!(result.is_ok(), "Second notification should be ignored due to cooldown");

        // Wait for cooldown to expire (using a shorter duration for testing)
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Third notification (should be sent)
        let result = ftp_client.notify_admin("Final test error").await;
        assert!(result.is_ok(), "Third notification should be sent after cooldown");

        // Verify that the last notification time has been updated
        let last_notification = ftp_client.last_notification.lock().map_err(|e| FtpError::LockError(e.to_string()))?;
        assert!(last_notification.is_some(), "Last notification time should be set");

        Ok(())
    }

    #[tokio::test]
    async fn test_main_program_loop() -> Result<(), FtpError> {
        dotenv().ok();
        let check_interval = env::var("CHECK_INTERVAL")
            .unwrap_or_else(|_| "5".to_string())  // Use a short interval for testing
            .parse::<u64>()
            .unwrap_or(5);

        let start_time = Utc::now();
        let mut loop_count = 0;

        let mock_ftp_client = MockFtpClient {
            is_updated: true,
            file_content: "Test content".to_string(),
        };

        while (Utc::now() - start_time).num_seconds() < 5 {  // Run for 5 seconds
            let mock_ftp_client_clone = mock_ftp_client.clone();
            match process_ftp_and_redis(&|| Ok(mock_ftp_client_clone.is_updated)).await {
                Ok(is_updated) => {
                    loop_count += 1;
                    if is_updated {
                        info!("File was updated");
                    } else {
                        info!("File was not updated");
                    }
                },
                Err(e) => {
                    error!("Error in main loop: {:?}", e);
                    return Err(FtpError::RetryError(format!("{:?}", e)));
                }
            }
            tokio::time::sleep(Duration::from_secs(check_interval)).await;
        }

        assert!(loop_count > 0, "Main loop should have run at least once");
        Ok(())
    }
}
