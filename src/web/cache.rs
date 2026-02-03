use std::time::{Duration, SystemTime, UNIX_EPOCH};
use moka::future::Cache;
use once_cell::sync::Lazy;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use serde::{ Serialize};
use log::{info};
use tokio::sync::OnceCell;
use std::sync::Arc;
use aws_config::BehaviorVersion;

#[derive(Hash, Eq, PartialEq, Clone)]
struct TileCacheKey {
    table_name: String,
    z: u32,
    x: u32,
    y: u32,
}

// Struktur untuk cache value dengan metadata ukuran
#[derive(Clone)]
struct CachedTile {
    data: Vec<u8>,
    size: usize,
}


impl CachedTile {
    fn new(data: Vec<u8>) -> Self {
        let size = data.len();
        Self { data, size }
    }
    
    fn weight(&self) -> u32 {
        // Return size in bytes, moka akan gunakan ini untuk menghitung total memory
        self.size as u32
    }
}


// Konfigurasi cache
pub struct CacheConfig {
    pub max_memory_mb: u64,      // Max memory dalam MB
    pub max_capacity: u64,        // Max jumlah entries (fallback)
    pub ttl_seconds: u64,         // TTL untuk memory cache
}


impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: std::env::var("CACHE_MAX_MEMORY_MB")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(512), // Default 512 MB
            max_capacity: std::env::var("CACHE_MAX_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50_000), // Default 50k entries
            ttl_seconds: std::env::var("CACHE_TTL_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(900), // Default 15 menit
        }
    }
}

// Global cache instance dengan memory limit
static TILE_CACHE: Lazy<Cache<TileCacheKey, CachedTile>> = Lazy::new(|| {
    let config = CacheConfig::default();
    let max_memory_bytes = config.max_memory_mb * 1024 * 1024;
    
    info!(
        "Initializing cache: max_memory={}MB, max_capacity={}, ttl={}s",
        config.max_memory_mb, config.max_capacity, config.ttl_seconds
    );
    
    Cache::builder()
        // Gunakan weigher untuk menghitung memory usage berdasarkan ukuran data
        .weigher(|_key: &TileCacheKey, value: &CachedTile| -> u32 {
            value.weight()
        })
        // Set max capacity berdasarkan total weight (bytes)
        .max_capacity(max_memory_bytes)
        // Set TTL
        .time_to_live(Duration::from_secs(config.ttl_seconds))
        // Optional: set initial capacity untuk performa
        .initial_capacity(1000)
        .build()
});




// Global singleton
static S3_CLIENT: OnceCell<Option<Arc<S3Client>>> = OnceCell::const_new();

async fn get_s3_client() -> Option<Arc<S3Client>> {
    S3_CLIENT
        .get_or_init(|| async {
            if std::env::var("AWS_ACCESS_KEY_ID").is_err()
                || std::env::var("AWS_SECRET_ACCESS_KEY").is_err()
            {
                info!("AWS env not set, skipping S3");
                None
            } else {
                info!("Initializing S3 client...");
                let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
                Some(Arc::new(S3Client::new(&config)))
            }
        })
        .await
        .clone()
}


// Konfigurasi S3
#[derive(Clone)]
pub struct S3Config {
    pub bucket: String,
    pub prefix: String,
    pub ttl_seconds: u64,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: std::env::var("S3_BUCKET").unwrap_or_else(|_| "tiles-cache".to_string()),
            prefix: std::env::var("S3_PREFIX").unwrap_or_else(|_| "vector-tiles".to_string()),
            ttl_seconds: std::env::var("S3_TTL_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(86400), // Default 24 jam
        }
    }
}

// Generate S3 key
fn generate_s3_key(config: &S3Config, table_name: &str, z: u32, x: u32, y: u32) -> String {
    format!("{}/{}/{}/{}/{}.pbf", config.prefix, table_name, z, x, y)
}



#[derive(Debug, Serialize)]
pub struct CacheStats {
    pub entry_count: u64,
    pub memory_bytes: u64,
    pub memory_mb: f64,
}

// Function untuk get data dari memory cache
pub async fn get_from_memory_cache(
    table_name: &str,
    z: u32,
    x: u32,
    y: u32,
) -> Option<Vec<u8>> {
    let key = TileCacheKey {
        table_name: table_name.to_string(),
        z,
        x,
        y,
    };
    
    TILE_CACHE.get(&key).await.map(|cached| {
        info!(
            "Memory cache hit: {}/{}/{}/{} ({} bytes)",
            table_name, z, x, y, cached.size
        );
        cached.data
    })
}

// Function untuk put data ke memory cache
pub async fn put_to_memory_cache(
    table_name: &str,
    z: u32,
    x: u32,
    y: u32,
    data: Vec<u8>,
) {
    let key = TileCacheKey {
        table_name: table_name.to_string(),
        z,
        x,
        y,
    };
    
    let cached = CachedTile::new(data);
    // let size = cached.size;
    TILE_CACHE.insert(key, cached).await;
    
    // Log cache stats setelah insert
    // let stats = get_cache_stats().await;
    // info!(
    //     "Cached tile: {}/{}/{}/{} ({} bytes) | Total: {} entries, {:.2} MB",
    //     table_name, z, x, y, size, stats.entry_count, stats.memory_mb
    // );
}

// Function untuk get data dari S3
pub async fn get_from_s3(
    config: &S3Config,
    table_name: &str,
    z: u32,
    x: u32,
    y: u32,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let key = generate_s3_key(config, table_name, z, x, y);

    if let Some(client) = get_s3_client().await {


        match client
            .get_object()
            .bucket(&config.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(output) => {
                // Cek metadata untuk expires
                if let Some(metadata) = output.metadata() {
                    if let Some(expires_str) = metadata.get("expires_at") {
                        if let Ok(expires_at) = expires_str.parse::<u64>() {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            
                            if now > expires_at {
                                info!("S3 tile expired, deleting: {}", key);
                                let _ = client
                                    .delete_object()
                                    .bucket(&config.bucket)
                                    .key(&key)
                                    .send()
                                    .await;
                                return Ok(None);
                            }
                        }
                    }
                }
                
                let bytes = output.body.collect().await?.into_bytes();
                Ok(Some(bytes.to_vec()))
            }
            Err(e) => {
                if e.to_string().contains("NoSuchKey") {
                    info!("S3 tile not found: {}", key);
                    Ok(None)
                } else {
                    Err(Box::new(e))
                }
            }
        }
           



    } else {
        info!("S3 not initialized, skipping S3 fetch");
        Ok(None)
    }

    
    
}

// Function untuk put data ke S3 dengan expires
pub async fn put_to_s3(
    config: &S3Config,
    table_name: &str,
    z: u32,
    x: u32,
    y: u32,
    data: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = generate_s3_key(config, table_name, z, x, y);
    
    let expires_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + config.ttl_seconds;

     if let Some(client) = get_s3_client().await {
        client
        .put_object()
        .bucket(&config.bucket)
        .key(&key)
        .body(ByteStream::from(data))
        .content_type("application/x-protobuf")
        .metadata("expires_at", expires_at.to_string())
        .metadata("table_name", table_name)
        .send()
        .await?;
    
        info!("✓ Uploaded to S3: {} (expires in {} seconds)", key, config.ttl_seconds);
        Ok(())

     } else {
        info!("S3 not initialized, skipping S3 fetch");
        Ok(())
     }

    
    
}

// Function untuk invalidate cache (memory + S3)
pub async fn invalidate_tile(
    config: &S3Config,
    table_name: &str,
    z: u32,
    x: u32,
    y: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = TileCacheKey {
        table_name: table_name.to_string(),
        z,
        x,
        y,
    };
    TILE_CACHE.invalidate(&key).await;

    if let Some(client) = get_s3_client().await { 
        let s3_key = generate_s3_key(config, table_name, z, x, y);
        client
            .delete_object()
            .bucket(&config.bucket)
            .key(&s3_key)
            .send()
            .await?;
        
        info!("✓ Invalidated tile: {}/{}/{}/{}", table_name, z, x, y);
        Ok(())

    } else {
        info!("S3 not initialized, skipping S3 fetch");
        Ok(())
    }
    
}
