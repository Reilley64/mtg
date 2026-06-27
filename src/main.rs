//! MTG card image puller: Scryfall bulk data -> download -> WebP -> hosted, indexable store.

mod scryfall;

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use futures_util::stream::{self, StreamExt};
use scryfall::{ImageSize, Job};
use tokio::io::AsyncWriteExt as _;

const USER_AGENT: &str = concat!("mtg-image-puller/", env!("CARGO_PKG_VERSION"));

#[derive(Parser)]
#[command(about = "Pull all MTG card prints from Scryfall, convert to WebP, store for web hosting")]
struct Args {
    /// Output directory (images at <out>/<size>/<front|back>/..).
    #[arg(long, default_value = "./out")]
    out: PathBuf,

    /// Scryfall bulk dataset. `default_cards` = every print, English-preferred.
    #[arg(long, default_value = "default_cards")]
    dataset: String,

    /// Source image size to fetch from Scryfall before WebP conversion.
    #[arg(long, value_enum, default_value_t = ImageSize::Large)]
    image: ImageSize,

    /// WebP quality 0-100; 100 = lossless.
    #[arg(long, default_value_t = 80.0)]
    quality: f32,

    /// Concurrent image downloads.
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Re-download the bulk file even if cached.
    #[arg(long)]
    refresh: bool,

    /// Only process the first N cards (for testing).
    #[arg(long)]
    limit: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()?;

    tokio::fs::create_dir_all(&args.out).await?;

    // 1. Discover the bulk feed; bail early if nothing new has shipped since last run.
    let info = scryfall::bulk_info(&client, &args.dataset).await?;
    let state_path = args.out.join("last-updated");
    // State is one timestamp string -> plain text file, no struct/JSON needed.
    let last_pulled = std::fs::read_to_string(&state_path).ok();
    let bulk_path = args.out.join(format!(".cache/{}.json", args.dataset));
    if !args.refresh
        && bulk_path.exists()
        && last_pulled.as_deref() == Some(info.updated_at.as_str())
    {
        tracing::info!("no new cards since {}; nothing to do", info.updated_at);
        return Ok(());
    }

    // Bulk feed is new/changed (or forced) -> (re)download it.
    download_to_file(&client, &info.download_uri, &bulk_path).await?;

    // 2. Pull the shared card back first: it's the asset every deck needs, and a
    //    failure here means connectivity is broken before we commit to the full run.
    let back = scryfall::card_back_job(args.image);
    if !dest_path(&args.out, &back, args.image).exists() {
        fetch_convert_store(&client, &back, &args.out, args.image, args.quality)
            .await
            .context("pulling card back")?;
    }

    // 3. Parse + derive image jobs.
    let mut cards = scryfall::parse_bulk(&bulk_path)?;
    if let Some(n) = args.limit {
        cards.truncate(n);
    }
    let jobs: Vec<Job> = cards.iter().flat_map(|c| c.jobs(args.image)).collect();
    tracing::info!("{} cards -> {} images", cards.len(), jobs.len());

    // 4. Skip already-stored images (resumable); the rest become download tasks.
    let n = jobs.len();
    let todo: Vec<Job> = jobs
        .into_iter()
        .filter(|j| !dest_path(&args.out, j, args.image).exists())
        .collect();
    tracing::info!("{} already stored, {} to fetch", n - todo.len(), todo.len());

    // 5. Fetch -> decode -> WebP -> store, bounded concurrency.
    let done = Arc::new(AtomicUsize::new(0));
    let total = todo.len();
    stream::iter(todo)
        .map(|job| {
            let client = client.clone();
            let root = args.out.clone();
            let done = done.clone();
            async move {
                match fetch_convert_store(&client, &job, &root, args.image, args.quality).await {
                    Ok(()) => {
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        if n % 500 == 0 {
                            tracing::info!("{n}/{total} stored");
                        }
                    }
                    Err(e) => tracing::warn!("{} ({}): {e:#}", job.id, job.url),
                }
            }
        })
        .buffer_unordered(args.concurrency)
        .for_each(|_| async {})
        .await;
    let stored = done.load(Ordering::Relaxed);

    // 6. Remember this feed version only on a clean run, so failures get retried next time.
    let failed = total - stored;
    if failed > 0 {
        tracing::warn!("{failed} failed; not recording feed version so next run retries");
    } else if args.limit.is_some() {
        tracing::warn!("--limit set: partial pull, not recording feed version");
    } else {
        std::fs::write(&state_path, &info.updated_at)
            .with_context(|| format!("writing {state_path:?}"))?;
    }

    tracing::info!("done: {stored} stored, {failed} failed");
    Ok(())
}

/// Mirror Scryfall's URL layout: <size>/<front|back>/<id[0]>/<id[1]>/<id>.webp
fn dest_path(root: &Path, job: &Job, size: ImageSize) -> PathBuf {
    // ponytail: face 0 -> front, any other face -> back. MTG has no >2-face cards.
    let face = if job.face == 0 { "front" } else { "back" };
    // ValueEnum names (small/normal/large/png) are exactly Scryfall's size segments.
    let seg = size.to_possible_value().unwrap().get_name().to_owned();
    let (c0, c1) = (&job.id[0..1], &job.id[1..2]);
    root.join(seg)
        .join(face)
        .join(c0)
        .join(c1)
        .join(format!("{}.webp", job.id))
}

async fn fetch_convert_store(
    client: &reqwest::Client,
    job: &Job,
    root: &Path,
    size: ImageSize,
    quality: f32,
) -> Result<()> {
    let bytes = client
        .get(&job.url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    // Decode + encode are CPU-bound and synchronous -> off the async runtime.
    let webp = tokio::task::spawn_blocking(move || encode_webp(&bytes, quality)).await??;

    let dest = dest_path(root, job, size);
    tokio::fs::create_dir_all(dest.parent().unwrap()).await?;
    tokio::fs::write(&dest, webp).await?;
    Ok(())
}

fn encode_webp(bytes: &[u8], quality: f32) -> Result<Vec<u8>> {
    let img = image::load_from_memory(bytes).context("decoding image")?;
    let encoder = webp::Encoder::from_image(&img).map_err(|e| anyhow!("webp encode: {e}"))?;
    let mem = if quality >= 100.0 {
        encoder.encode_lossless()
    } else {
        encoder.encode(quality)
    };
    Ok(mem.to_vec())
}

async fn download_to_file(client: &reqwest::Client, url: &str, path: &Path) -> Result<()> {
    tracing::info!("downloading bulk file -> {}", path.display());
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let resp = client.get(url).send().await?.error_for_status()?;
    let mut stream = resp.bytes_stream();
    // Write to a temp file then rename, so an interrupted download never poses as a cache hit.
    let tmp = path.with_extension("json.partial");
    let mut file = tokio::fs::File::create(&tmp).await?;
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}
