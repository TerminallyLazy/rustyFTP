use std::sync::{Arc, Mutex};
use std::time::Duration;
use chrono::{DateTime, Utc};
use async_ftp::FtpStream;
use backoff::ExponentialBackoff;
use log::{error, info};
use redis::aio::Connection as RedisConnection;
use tokio::time::timeout;
use std::env;
use csv_async::StringRecord;

#[derive(Clone)]
pub struct FtpClient {
    pub host: String,
    pub user: String,
    pub pass: String,
    pub file_path: String,
    pub last_checked: Arc<Mutex<Option<DateTime<Utc>>>>,
    pub redis_url: String,
    pub last_notification: Arc<Mutex<Option<DateTime<Utc>>>>,
}

#[cfg(test)]
#[derive(Clone)]
pub struct MockFtpClient {
    pub is_updated: bool,
    pub file_content: String,
}

#[cfg(test)]
#[derive(Clone)]
pub struct MockRedisConnection;

#[derive(thiserror::Error, Debug)]
pub enum FtpError {
    #[error("Environment variable not found: {0}")]
    EnvVarError(#[from] env::VarError),
    #[error("FTP error: {0}")]
    FtpError(#[from] async_ftp::types::FtpError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("CSV error: {0}")]
    CsvError(#[from] csv::Error),
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("Lock error: {0}")]
    LockError(String),
    #[error("DateTime parse error: {0}")]
    DateTimeParseError(#[from] chrono::ParseError),
    #[error("Timeout error")]
    TimeoutError,
    #[error("Retry error: {0}")]
    RetryError(String),
}

impl FtpClient {
    pub fn new() -> Result<Self, FtpError> {
        dotenv::dotenv().ok();
        Ok(FtpClient {
            host: env::var("FTP_HOST")?,
            user: env::var("FTP_USER")?,
            pass: env::var("FTP_PASS")?,
            file_path: env::var("FTP_FILE_PATH")?,
            last_checked: Arc::new(Mutex::new(None)),
            redis_url: env::var("REDIS_URL")?,
            last_notification: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn connect(&self) -> Result<FtpStream, FtpError> {
        let host = self.host.clone();
        let user = self.user.clone();
        let pass = self.pass.clone();

        info!("Attempting to connect to FTP server: {}", host);
        let connect_future = async {
            let (address, port) = match host.split_once(':') {
                Some((addr, p)) => (addr, p.parse().unwrap_or(21)),
                None => (host.as_str(), 21),
            };
            info!("Resolved FTP address: {}:{}", address, port);
            let mut ftp_stream = FtpStream::connect((address, port)).await?;
            info!("FTP connection established, attempting login for user: {}", user);
            ftp_stream.login(&user, &pass).await?;
            info!("FTP login successful");
            Ok(ftp_stream)
        };

        match timeout(Duration::from_secs(30), connect_future).await {
            Ok(result) => result,
            Err(_) => {
                error!("FTP connection timed out after 30 seconds");
                Err(FtpError::TimeoutError)
            }
        }
    }

    pub async fn check_file_timestamp(&self, ftp_stream: &mut FtpStream) -> Result<bool, FtpError> {
        let file_name = std::path::Path::new(&self.file_path)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| FtpError::FtpError(async_ftp::FtpError::InvalidResponse("Invalid file path".to_string())))?;

        let file_list = ftp_stream.nlst(Some(&self.file_path)).await?;
        let file_info = file_list
            .into_iter()
            .find(|entry| entry == file_name)
            .ok_or_else(|| FtpError::FtpError(async_ftp::FtpError::InvalidResponse("File not found".to_string())))?;

        let last_modified = ftp_stream.mdtm(&file_info).await
            .map_err(|e| FtpError::FtpError(e))?;

        let mut last_checked = self.last_checked.lock().map_err(|e| FtpError::LockError(e.to_string()))?;
        let is_updated = match (*last_checked, last_modified) {
            (Some(checked_time), Some(modified_time)) => modified_time > checked_time,
            (None, Some(_)) => true,
            (_, None) => false,
        };

        if let Some(modified_time) = last_modified {
            *last_checked = Some(modified_time);
        }
        Ok(is_updated)
    }

    pub async fn download_file(&self, ftp_stream: &mut FtpStream) -> Result<String, FtpError> {
        let mut reader = ftp_stream.simple_retr(&self.file_path).await?;
        let temp_dir = std::env::temp_dir();
        let local_file_path = temp_dir.join("downloaded_file.csv");
        let mut file = tokio::fs::File::create(&local_file_path).await?;
        tokio::io::copy(&mut reader, &mut file).await?;
        Ok(local_file_path.to_string_lossy().into_owned())
    }

    pub async fn disconnect(ftp_stream: &mut FtpStream) -> Result<(), FtpError> {
        ftp_stream.quit().await?;
        info!("Disconnected from FTP server");
        Ok(())
    }

    pub async fn parse_and_store_file<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
        &self,
        mut reader: R,
        redis_conn: &mut RedisConnection,
    ) -> Result<(), FtpError> {
        use tokio::io::AsyncReadExt;

        let mut content = String::new();
        reader.read_to_string(&mut content).await?;

        // Store the entire file content in Redis
        redis::cmd("SET")
            .arg("file_content")
            .arg(&content)
            .query_async(redis_conn)
            .await?;

        info!("File content stored in Redis successfully");
        Ok(())
    }

    pub async fn connect_to_redis(&self) -> Result<RedisConnection, FtpError> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let conn = client.get_async_connection().await?;
        Ok(conn)
    }

    pub async fn push_to_redis(&self, conn: &mut RedisConnection, record: &StringRecord) -> Result<(), FtpError> {
        let key = format!("record:{}", record.get(0).unwrap_or_default()); // Assuming the first field is a unique identifier
        let value = record.iter().collect::<Vec<&str>>().join(",");
        redis::cmd("SET").arg(&key).arg(&value).query_async(conn).await?;
        Ok(())
    }

    pub async fn notify_admin(&self, error: &str) -> Result<(), FtpError> {
        let mut last_notification = self.last_notification.lock().map_err(|e| FtpError::LockError(e.to_string()))?;
        let now = Utc::now();

        if let Some(last_time) = *last_notification {
            if now.signed_duration_since(last_time).num_minutes() < 30 {
                info!("Skipping admin notification due to cooldown");
                return Ok(());
            }
        }

        // In a real-world scenario, you would implement an actual notification mechanism here
        // For now, we'll just log the error
        error!("Admin notification: {}", error);

        *last_notification = Some(now);
        Ok(())
    }
}

#[cfg(test)]
impl MockFtpClient {
    pub async fn check_file_timestamp(&self, _: &mut FtpStream) -> Result<bool, FtpError> {
        Ok(self.is_updated)
    }

    pub async fn download_file(&self, _: &mut FtpStream) -> Result<String, FtpError> {
        Ok(self.file_content.clone())
    }

    pub async fn parse_and_store_file<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
        &self,
        _reader: R,
        _redis_conn: &mut MockRedisConnection,
    ) -> Result<(), FtpError> {
        Ok(())
    }

    pub async fn notify_admin(&self, _error: &str) -> Result<(), FtpError> {
        Ok(())
    }
}

pub async fn process_ftp_and_redis<F>(ftp_client: &F) -> Result<bool, backoff::Error<FtpError>>
where
    F: Fn() -> Result<bool, FtpError> + Sync + Send,
{
    const INITIAL_INTERVAL: Duration = Duration::from_secs(1);
    const MAX_INTERVAL: Duration = Duration::from_secs(60);
    const MAX_ELAPSED_TIME: Duration = Duration::from_secs(300);

    let backoff = ExponentialBackoff {
        initial_interval: INITIAL_INTERVAL,
        max_interval: MAX_INTERVAL,
        max_elapsed_time: Some(MAX_ELAPSED_TIME),
        ..ExponentialBackoff::default()
    };

    let retry_operation = || async {
        let is_updated = ftp_client().map_err(backoff::Error::Permanent)?;

        if is_updated {
            // Simulating file processing and Redis storage
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(is_updated)
    };

    match backoff::future::retry(backoff, retry_operation).await {
        Ok(is_updated) => {
            info!("Operation completed successfully after retries");
            Ok(is_updated)
        },
        Err(e) => {
            let error_msg = format!("Error occurred during retry: {}", e);
            error!("{}", error_msg);
            Err(backoff::Error::Permanent(e))
        },
    }
}

async fn process_file(ftp_client: &FtpClient, ftp_stream: &mut FtpStream, redis_conn: &mut RedisConnection) -> Result<(), FtpError> {
    let is_updated = ftp_client.check_file_timestamp(ftp_stream).await?;

    if is_updated {
        info!("File has been updated. Downloading...");
        let local_file_path = ftp_client.download_file(ftp_stream).await?;

        let file = tokio::fs::File::open(&local_file_path).await?;
        let reader = tokio::io::BufReader::new(file);

        info!("Storing file content in Redis...");
        ftp_client.parse_and_store_file(reader, redis_conn).await?;

        info!("File content stored in Redis successfully");

        if let Err(e) = tokio::fs::remove_file(&local_file_path).await {
            error!("Failed to remove local file: {}. Error: {}", local_file_path, e);
        }
    } else {
        info!("File has not been updated since last check.");
    }

    Ok(())
}
