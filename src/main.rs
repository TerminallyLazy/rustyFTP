use dotenv::dotenv;
use log::{error, info};
use std::env;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;
use chrono::{DateTime, Utc};
use redis::aio::Connection as RedisConnection;
use backoff::{ExponentialBackoff, retry};
use futures::StreamExt;
use tokio_util::compat::TokioAsyncReadCompatExt;
use std::future::Future;
use std::pin::Pin;

const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

#[derive(Error, Debug)]
enum FtpError {
    #[error("Environment variable not found: {0}")]
    EnvVarError(#[from] env::VarError),
    #[error("FTP error: {0}")]
    FtpError(#[from] async_ftp::types::FtpError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("CSV error: {0}")]
    CsvError(#[from] csv_async::Error),
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("Backoff error: {0}")]
    BackoffError(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("Lock error: {0}")]
    LockError(String),
    #[error("DateTime parse error: {0}")]
    DateTimeParseError(#[from] chrono::ParseError),
}

use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct FtpClient {
    host: String,
    user: String,
    pass: String,
    file_path: String,
    last_checked: Arc<Mutex<Option<DateTime<Utc>>>>,
    redis_url: String,
    last_notification: Arc<Mutex<Option<DateTime<Utc>>>>,
}

impl FtpClient {
    fn new() -> Result<Self, FtpError> {
        dotenv().ok();
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

    async fn connect(&self) -> Result<async_ftp::FtpStream, FtpError> {
        let host = self.host.clone();
        let user = self.user.clone();
        let pass = self.pass.clone();

        let connect_future = async {
            let mut ftp_stream = async_ftp::FtpStream::connect(&host).await?;
            ftp_stream.login(&user, &pass).await?;
            Ok(ftp_stream)
        };

        tokio::time::timeout(Duration::from_secs(30), connect_future)
            .await
            .map_err(|_| FtpError::IoError(std::io::Error::new(std::io::ErrorKind::TimedOut, "FTP connection timed out")))?
    }

    async fn check_file_timestamp(&self, ftp_stream: &mut async_ftp::FtpStream) -> Result<bool, FtpError> {
        let file_path = self.file_path.clone();
        let modified_time: i64 = ftp_stream.mdtm(&file_path).await?;

        let modified_datetime = chrono::DateTime::<Utc>::from_timestamp(modified_time, 0)
            .ok_or_else(|| FtpError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid timestamp")))?;

        let mut last_checked = self.last_checked.lock()
            .map_err(|e| FtpError::LockError(format!("Failed to acquire lock: {}", e)))?;

        let is_updated = last_checked.as_ref().map_or(true, |&last| modified_datetime > last);

        if is_updated {
            *last_checked = Some(modified_datetime);
        }

        Ok(is_updated)
    }

    async fn download_file(&self, ftp_stream: &mut async_ftp::FtpStream) -> Result<String, FtpError> {
        let file_path = self.file_path.clone();
        let mut remote_file = ftp_stream.simple_retr(&file_path).await
            .map_err(FtpError::FtpError)?;

        let local_file_path = Path::new(&self.file_path)
            .file_name()
            .ok_or_else(|| FtpError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid file path")))?
            .to_str()
            .ok_or_else(|| FtpError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid file name")))?
            .to_owned();

        let mut file_content = Vec::new();
        tokio::io::copy(&mut remote_file, &mut file_content).await
            .map_err(FtpError::IoError)?;

        tokio::fs::write(&local_file_path, &file_content).await?;

        info!("File downloaded successfully: {}", local_file_path);
        Ok(local_file_path)
    }

    async fn disconnect(ftp_stream: &mut async_ftp::FtpStream) -> Result<(), FtpError> {
        ftp_stream.quit().await
            .map_err(FtpError::FtpError)
            .and_then(|_| {
                info!("Disconnected from FTP server");
                Ok(())
            })
    }

    async fn parse_csv<R: tokio::io::AsyncRead + Unpin + Send + 'static>(&self, reader: R, redis_conn: &mut RedisConnection) -> Result<(), FtpError> {
        use csv_async::AsyncReaderBuilder;
        use futures::StreamExt;
        use tokio_util::compat::TokioAsyncReadCompatExt;

        let compat_reader = reader.compat();
        let mut csv_reader = AsyncReaderBuilder::new()
            .flexible(true)
            .create_deserializer(compat_reader);

        let mut record_count = 0;

        while let Some(result) = csv_reader.deserialize::<Vec<String>>().next().await {
            let record = result?;
            record_count += 1;
            info!("Processing record {}", record_count);

            self.push_to_redis(redis_conn, record).await?;
        }

        info!("Successfully processed {} CSV records", record_count);
        Ok(())
    }

    async fn connect_to_redis(&self) -> Result<redis::aio::Connection, FtpError> {
        let client = redis::Client::open(self.redis_url.as_str())
            .map_err(FtpError::RedisError)?;
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(60));

        let conn = retry(backoff, || {
            let client = client.clone();
            async move {
                const REDIS_TIMEOUT: Duration = Duration::from_secs(10);
                match tokio::time::timeout(REDIS_TIMEOUT, client.get_async_connection()).await {
                    Ok(Ok(conn)) => Ok(conn),
                    Ok(Err(e)) => {
                        error!("Redis connection error: {:?}", e);
                        Err(backoff::Error::transient(FtpError::RedisError(e)))
                    },
                    Err(_) => {
                        let e = redis::RedisError::from(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("Redis connection timed out after {:?}", REDIS_TIMEOUT),
                        ));
                        error!("Redis connection timeout: {:?}", e);
                        Err(backoff::Error::transient(FtpError::RedisError(e)))
                    }
                }
            }
        }).await
        .map_err(|e| {
            error!("Failed to connect to Redis after multiple attempts: {:?}", e);
            match e {
                backoff::Error::Transient(e) | backoff::Error::Permanent(e) => e,
            }
        })?;

        info!("Successfully connected to Redis");
        Ok(conn)
    }

    async fn push_to_redis(&self, conn: &mut redis::aio::Connection, record: Vec<String>) -> Result<(), FtpError> {
        use redis::AsyncCommands;
        let key = format!("record:{}", record.first().unwrap_or(&"unknown".to_string()));
        let value = record.join(",");
        conn.set(&key, &value).await.map_err(FtpError::RedisError)?;
        Ok(())
    }

    async fn notify_admin(&self, error: &str) -> Result<(), FtpError> {
        let now = Utc::now();
        let mut last_notification = self.last_notification.lock()
            .map_err(|e| FtpError::LockError(format!("Failed to acquire lock: {}", e)))?;

        if last_notification.as_ref().map_or(true, |&last| now.signed_duration_since(last).num_minutes() >= 30) {
            error!("Admin Notification: {}", error);
            *last_notification = Some(now);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), FtpError> {
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

async fn process_ftp_and_redis(ftp_client: &FtpClient) -> Result<(), FtpError> {
    const INITIAL_TIMEOUT: Duration = Duration::from_secs(30);
    const MAX_TIMEOUT: Duration = Duration::from_secs(120);

    let mut backoff = ExponentialBackoff::default();
    backoff.initial_interval = INITIAL_TIMEOUT;
    backoff.max_interval = MAX_TIMEOUT;
    backoff.max_elapsed_time = Some(Duration::from_secs(300));

    retry(backoff, || {
        let ftp_client = ftp_client.clone();
        async move {
            tokio::time::timeout(
                INITIAL_TIMEOUT,
                async {
                    let mut ftp_stream = ftp_client.connect().await?;
                    let mut redis_conn = ftp_client.connect_to_redis().await?;

                    let result = process_file(&ftp_client, &mut ftp_stream, &mut redis_conn).await;

                    FtpClient::disconnect(&mut ftp_stream).await?;

                    result
                },
            )
            .await
            .map_err(|_| {
                let e = FtpError::IoError(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Operation timed out",
                ));
                error!("Operation timed out");
                e
            })?
        }
    })
    .await
    .map_err(|e| match e {
        backoff::Error::Transient(e) | backoff::Error::Permanent(e) => e,
    })
}

async fn process_file(ftp_client: &FtpClient, ftp_stream: &mut async_ftp::FtpStream, redis_conn: &mut RedisConnection) -> Result<(), FtpError> {
    let is_updated = ftp_client.check_file_timestamp(ftp_stream).await?;

    if is_updated {
        info!("File has been updated. Downloading...");
        let local_file_path = ftp_client.download_file(ftp_stream).await?;

        let file = tokio::fs::File::open(&local_file_path).await?;
        let reader = tokio::io::BufReader::new(file);

        info!("Parsing CSV and pushing data to Redis...");
        ftp_client.parse_csv(reader, redis_conn).await?;

        info!("CSV data processed and pushed to Redis successfully");

        if let Err(e) = tokio::fs::remove_file(&local_file_path).await {
            error!("Failed to remove local file: {}. Error: {}", local_file_path, e);
        }
    } else {
        info!("File has not been updated since last check.");
    }

    Ok(())
}
