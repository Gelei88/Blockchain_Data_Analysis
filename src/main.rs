use anyhow::{Result, Context};
use bb8_redis::{bb8, redis::AsyncCommands, RedisConnectionManager};
use futures::{StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use log::{info, warn, error};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use web3::{
    transports::Http,
    types::{BlockId, BlockNumber, Transaction, H160, U256, Address},
    Web3,
};
use std::str::FromStr;


const RPC_URLS: [&str; 7] = [
    "https://rpc.scroll.io/",
    "https://1rpc.io/scroll",
    "https://scroll.drpc.org",
    "https://rpc-scroll.icecreamswap.com",
    "https://rpc.ankr.com/scroll",
    "https://scroll-mainnet.chainstacklabs.com",
    "https://534352.rpc.thirdweb.com",
];

const REDIS_URL: &str = "redis://localhost:6379";
const MAX_ADDRESSES: usize = 500_000;
const BLOCK_BATCH_SIZE: u64 = 50;

type RedisPool = bb8::Pool<RedisConnectionManager>;

#[derive(Serialize, Deserialize, Clone)]
struct Progress {
    last_block: u64,
    address_count: usize,
}

struct RpcManager {
    rpc_urls: Vec<String>,
    current_index: usize,
}

impl RpcManager {
    fn new() -> Self {
        RpcManager {
            rpc_urls: RPC_URLS.iter().map(|&s| s.to_string()).collect(),
            current_index: 0,
        }
    }

    fn get_next_rpc(&mut self) -> String {
        let url = self.rpc_urls[self.current_index].clone();
        self.current_index = (self.current_index + 1) % self.rpc_urls.len();
        url
    }
}

async fn create_web3_client(rpc_url: &str) -> Result<Web3<Http>> {
    let transport = Http::new(rpc_url)?;
    Ok(Web3::new(transport))
}

async fn get_latest_block(web3: &Web3<Http>) -> Result<u64> {
    let block_number = web3.eth().block_number().await?;
    Ok(block_number.as_u64())
}

async fn is_contract(web3: &Web3<Http>, address: &H160) -> Result<bool> {
    let code = web3.eth().code(*address, None).await?;
    Ok(!code.0.is_empty())
}
async fn process_transaction(
    web3: &Web3<Http>,
    tx: Transaction,
    redis: &RedisPool,
    address_count: &Arc<Mutex<usize>>,
) -> Result<Vec<String>> {
    let mut new_addresses = Vec::new();

    async fn check_and_add_address(
        web3: &Web3<Http>,
        address: H160,
        redis: &RedisPool,
        address_count: &Arc<Mutex<usize>>,
        nonce: U256,
    ) -> Result<Option<String>> {
        if !is_contract(web3, &address).await? && nonce > U256::from(30) {
            let address_string = format!("{:?}", address);
            
            if Address::from_str(&address_string).is_ok() {
                let mut conn = redis.get().await?;
                let is_new: bool = conn.sadd("scroll_address_set", &address_string).await?;
                if is_new {
                    let mut count = address_count.lock().await;
                    *count += 1;
                    return Ok(Some(address_string));
                }
            } else {
                warn!("Invalid address format: {}", address_string);
            }
        }
        Ok(None)
    }

    if let Some(from) = tx.from {
        let nonce = web3.eth().transaction_count(from, None).await?;
        if let Some(addr) = check_and_add_address(web3, from, redis, address_count, nonce).await? {
            new_addresses.push(addr);
        }
    }

    if let Some(to) = tx.to {
        let nonce = web3.eth().transaction_count(to, None).await?;
        if let Some(addr) = check_and_add_address(web3, to, redis, address_count, nonce).await? {
            new_addresses.push(addr);
        }
    }

    Ok(new_addresses)
}

async fn process_block(
    web3: &Web3<Http>,
    block_number: u64,
    redis: &RedisPool,
    address_count: &Arc<Mutex<usize>>,
) -> Result<()> {
    let block = web3
        .eth()
        .block_with_txs(BlockId::Number(BlockNumber::Number(block_number.into())))
        .await?
        .ok_or_else(|| anyhow::anyhow!("Block not found"))?;

    let new_addresses: Vec<String> = futures::stream::iter(block.transactions)
        .map(|tx| process_transaction(web3, tx, redis, address_count))
        .buffer_unordered(3)
        .try_collect::<Vec<Vec<String>>>()
        .await?
        .into_iter()
        .flatten()
        .collect();

    if !new_addresses.is_empty() {
        let mut conn = redis.get().await?;
        let mut pipe = bb8_redis::redis::pipe();
        for address in new_addresses {
            pipe.zadd("scroll_addresses", address, 0);
        }
        pipe.query_async(&mut *conn).await?;
    }

    Ok(())
}

async fn save_progress(redis: &RedisPool, progress: &Progress) -> Result<()> {
    let mut conn = redis.get().await?;
    let progress_clone = progress.clone();
    let compressed = tokio::task::spawn_blocking(move || {
        bincode::serialize(&progress_clone)
            .map(|data| zstd::encode_all(&*data, 3))
            .map_err(anyhow::Error::from)
    })
    .await??;

    let compressed_data = compressed?;

    conn.set("scroll_progress", compressed_data).await?;
    Ok(())
}

async fn load_progress(redis: &RedisPool) -> Result<Option<Progress>> {
    let mut conn = redis.get().await?;
    let compressed: Option<Vec<u8>> = conn.get("scroll_progress").await?;
    if let Some(compressed) = compressed {
        let progress = tokio::task::spawn_blocking(move || {
            zstd::decode_all(&compressed[..])
                .and_then(|decompressed| bincode::deserialize(&decompressed).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
                .map_err(anyhow::Error::from)
        })
        .await??;
        Ok(Some(progress))
    } else {
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let mut rpc_manager = RpcManager::new();
    let redis_manager = RedisConnectionManager::new(REDIS_URL)?;
    let redis = bb8::Pool::builder().build(redis_manager).await?;
    let address_count = Arc::new(Mutex::new(0));

    let mut start_block = 5570000;

    if let Some(progress) = load_progress(&redis).await? {
        if progress.last_block >= start_block {
            start_block = progress.last_block + 1;
            let mut count = address_count.lock().await;
            *count = progress.address_count;
        }
    }

    let pb = Arc::new(ProgressBar::new_spinner());
    pb.set_style(ProgressStyle::default_spinner()
        .template("{spinner:.green} [{elapsed_precise}] {msg}")
        .context("Failed to set progress bar template")?);

    loop {
        let rpc_url = rpc_manager.get_next_rpc();
        info!("Connecting to RPC endpoint: {}", rpc_url);

        match create_web3_client(&rpc_url).await {
            Ok(web3) => {
                match get_latest_block(&web3).await {
                    Ok(latest_block) => {
                        info!("Connected to RPC. Latest block: {}", latest_block);

                        while start_block <= latest_block {
                            let end_block = (start_block + BLOCK_BATCH_SIZE - 1).min(latest_block);
                            
                            let block_stream = futures::stream::iter(start_block..=end_block)
                                .map(|block_number| {
                                    let web3 = web3.clone();
                                    let redis = redis.clone();
                                    let address_count = address_count.clone();
                                    async move {
                                        match process_block(&web3, block_number, &redis, &address_count).await {
                                            Ok(_) => Ok(()),
                                            Err(e) => {
                                                warn!("Error processing block {}: {:?}", block_number, e);
                                                Err(e)
                                            }
                                        }
                                    }
                                })
                                .buffer_unordered(3);

                            match block_stream.try_collect::<Vec<()>>().await {
                                Ok(_) => {
                                    let current_count = *address_count.lock().await;
                                    let progress = Progress {
                                        last_block: end_block,
                                        address_count: current_count,
                                    };
                                    if let Err(e) = save_progress(&redis, &progress).await {
                                        error!("Failed to save progress: {:?}", e);
                                    }

                                    pb.set_message(format!("Processed blocks {} to {}. Total addresses: {}", start_block, end_block, current_count));
                                    start_block = end_block + 1;

                                    if current_count >= MAX_ADDRESSES {
                                        pb.finish_with_message(format!("Finished collecting {} addresses", current_count));
                                        return Ok(());
                                    }
                                },
                                Err(e) => {
                                    error!("Error occurred while processing blocks: {:?}", e);
                                    break;
                                }
                            }
                        }
                    },
                    Err(e) => {
                        error!("Failed to get latest block: {:?}", e);
                    }
                }
            },
            Err(e) => {
                error!("Failed to create Web3 client: {:?}", e);
            }
        }

        info!("Waiting 5 seconds before switching to next RPC endpoint...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}