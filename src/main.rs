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
        match process_ftp_and_redis(&ftp_client).await {
            Ok(()) => info!("Operation completed successfully"),
            Err(e) => {
                error!("Error occurred: {}", e);
                if let Err(notify_err) = ftp_client.notify_admin(&format!("Error occurred: {}", e)).await {
                    error!("Failed to notify admin: {}", notify_err);
                }
            }
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
    async fn test_csv_parsing_and_redis_integration() -> Result<(), FtpError> {
        use redis::AsyncCommands;

        dotenv().ok();
        let ftp_client = FtpClient::new()?;

        let result = tokio::time::timeout(Duration::from_secs(30), async {
            let mut ftp_stream = ftp_client.connect().await?;
            let local_file_path = ftp_client.download_file(&mut ftp_stream).await?;

            let file = tokio::fs::File::open(&local_file_path).await?;
            let reader = tokio::io::BufReader::new(file);

            let mut redis_conn = ftp_client.connect_to_redis().await?;
            ftp_client.parse_csv(reader, &mut redis_conn).await?;

            // Check if data was pushed to Redis
            let key = "record:1";  // Assuming the first record has id 1
            let value: Option<String> = redis_conn.get(key).await?;
            assert!(value.is_some(), "No data found in Redis for key: {}", key);

            // Check CSV parsing accuracy
            let parsed_value = value.unwrap();
            assert!(parsed_value.contains(","), "CSV data not properly parsed");

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

        // Wait for cooldown to expire
        tokio::time::sleep(Duration::from_secs(1800)).await;

        // Third notification (should be sent)
        ftp_client.notify_admin("Final test error").await?;

        // Verify that the last notification time has been updated
        let last_notification = ftp_client.last_notification.lock().map_err(|e| FtpError::LockError(e.to_string()))?;
        assert!(last_notification.is_some(), "Last notification time should be set");

        Ok(())
    }

    #[tokio::test]
    async fn test_main_program_loop() -> Result<(), FtpError> {
        dotenv().ok();
        let ftp_client = FtpClient::new()?;
        let check_interval = env::var("CHECK_INTERVAL")
            .unwrap_or_else(|_| "5".to_string())  // Use a short interval for testing
            .parse::<u64>()
            .unwrap_or(5);

        let start_time = Utc::now();
        let mut loop_count = 0;
        let mut timeout_count = 0;
        let mut other_error_count = 0;

        while (Utc::now() - start_time).num_seconds() < 15 {  // Run for 15 seconds
            match process_ftp_and_redis(&ftp_client).await {
                Ok(()) => loop_count += 1,
                Err(e) => {
                    match e {
                        FtpError::TimeoutError => {
                            timeout_count += 1;
                            error!("Timeout error in main loop");
                        },
                        _ => {
                            other_error_count += 1;
                            error!("Error in main loop: {}", e);
                            if let Err(notify_err) = ftp_client.notify_admin(&format!("Error in main loop: {}", e)).await {
                                error!("Failed to notify admin: {}", notify_err);
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(check_interval)).await;
        }

        assert!(loop_count > 0, "Main loop should have run at least once");
        assert!(timeout_count + other_error_count < loop_count, "Too many errors occurred");
        assert!(timeout_count > 0, "No timeout errors occurred, which is unexpected");
        Ok(())
    }
}
