//! MTG card image puller: Scryfall bulk data -> download -> WebP -> hosted, indexable store.

mod scryfall;

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
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

/// Per-run config shared by every download: the HTTP client and where/how to store.
struct Puller {
    client: reqwest::Client,
    out: PathBuf,
    image: ImageSize,
    quality: f32,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    let puller = Puller {
        client: reqwest::Client::builder().user_agent(USER_AGENT).build()?,
        out: args.out.clone(),
        image: args.image,
        quality: args.quality,
    };
    tokio::fs::create_dir_all(&puller.out).await?;

    // 1. Discover the bulk feed. Two plain-text version markers (no struct/JSON):
    //    `last-updated` = feed version of the last fully-clean run -> drives the short-circuit;
    //    `.cache/<dataset>.version` = feed version of the cached bulk file -> drives re-download.
    let info = scryfall::bulk_info(&puller.client, &args.dataset).await?;
    let version = info.updated_at.as_str();
    let bulk_path = puller.out.join(format!(".cache/{}.json", args.dataset));
    let bulk_version_path = bulk_path.with_extension("version");
    // "done" is per dataset+size (images live under <out>/<size>/); the bulk cache above is
    // size-independent, so it stays keyed by dataset only.
    let state_path = puller.out.join(format!(
        ".cache/{}-{}.last-updated",
        args.dataset,
        args.image.to_possible_value().unwrap().get_name()
    ));

    // Nothing to do: the last clean run already covered this exact feed.
    if !args.refresh && std::fs::read_to_string(&state_path).ok().as_deref() == Some(version) {
        tracing::info!("no new cards since {version}; nothing to do");
        return Ok(());
    }

    // (Re)download the bulk only when the cache is missing, stale, or forced. A partial-failure
    // retry (e.g. a few unreleased-set 404s) thus reuses the cached file instead of re-fetching ~500MB.
    if args.refresh
        || !bulk_path.exists()
        || std::fs::read_to_string(&bulk_version_path).ok().as_deref() != Some(version)
    {
        download_to_file(&puller.client, &info.download_uri, &bulk_path).await?;
        std::fs::write(&bulk_version_path, version)
            .with_context(|| format!("writing {bulk_version_path:?}"))?;
    } else {
        tracing::info!("reusing cached bulk {version}");
    }

    // 2. Pull the shared card back first: it's the asset every deck needs, and a
    //    failure here means connectivity is broken before we commit to the full run.
    let back = scryfall::card_back_job(args.image);
    if !puller.dest_path(&back).exists() {
        puller.store(&back).await.context("pulling card back")?;
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
        .filter(|j| !puller.dest_path(j).exists())
        .collect();
    tracing::info!("{} already stored, {} to fetch", n - todo.len(), todo.len());

    // 5. Fetch -> decode -> WebP -> store, bounded concurrency. Borrowed, not spawned,
    //    so the futures can share &puller / &done without Arc or per-task clones.
    let done = AtomicUsize::new(0);
    let total = todo.len();
    let (puller, done) = (&puller, &done);
    stream::iter(todo)
        .map(|job| async move {
            match puller.store(&job).await {
                Ok(()) => {
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % 500 == 0 {
                        tracing::info!("{n}/{total} stored");
                    }
                }
                Err(e) => tracing::warn!("{} ({}): {e:#}", job.id, job.url),
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

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

impl Puller {
    /// Mirror Scryfall's URL layout: <size>/<front|back>/<id[0]>/<id[1]>/<id>.webp
    fn dest_path(&self, job: &Job) -> PathBuf {
        // ponytail: face 0 -> front, any other face -> back. MTG has no >2-face cards.
        let face = if job.face == 0 { "front" } else { "back" };
        // ValueEnum names (small/normal/large/png) are exactly Scryfall's size segments.
        let seg = self.image.to_possible_value().unwrap().get_name().to_owned();
        let (c0, c1) = (&job.id[0..1], &job.id[1..2]);
        self.out
            .join(seg)
            .join(face)
            .join(c0)
            .join(c1)
            .join(format!("{}.webp", job.id))
    }

    /// Download one image, convert to WebP, write it to its deterministic path.
    async fn store(&self, job: &Job) -> Result<()> {
        let bytes = self
            .client
            .get(&job.url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;

        // Decode + encode are CPU-bound and synchronous -> off the async runtime.
        let quality = self.quality;
        let webp = tokio::task::spawn_blocking(move || encode_webp(&bytes, quality)).await??;

        // Write to a temp sibling then rename: a kill mid-write must not leave a
        // truncated file that the skip-existing check would treat as complete.
        let dest = self.dest_path(job);
        tokio::fs::create_dir_all(dest.parent().unwrap()).await?;
        let tmp = dest.with_extension("webp.partial");
        tokio::fs::write(&tmp, webp).await?;
        tokio::fs::rename(&tmp, &dest).await?;
        Ok(())
    }
}

fn encode_webp(bytes: &[u8], quality: f32) -> Result<Vec<u8>> {
    let img = image::load_from_memory(bytes).context("decoding image")?;
    // webp's from_image only takes RGB8/RGBA8; normalize grayscale/16-bit/etc. first.
    let img = if img.color().has_alpha() {
        image::DynamicImage::ImageRgba8(img.to_rgba8())
    } else {
        image::DynamicImage::ImageRgb8(img.to_rgb8())
    };
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
    // Write to a temp file, then rename it, so an interrupted download never poses as a cache hit.
    let tmp = path.with_extension("json.partial");
    let mut file = tokio::fs::File::create(&tmp).await?;
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::encode_webp;

    /// Grayscale (Luma8) source used to fail webp encoding with "Unimplemented".
    #[test]
    fn encodes_grayscale() {
        let gray = image::DynamicImage::ImageLuma8(image::GrayImage::new(8, 8));
        let mut png = std::io::Cursor::new(Vec::new());
        gray.write_to(&mut png, image::ImageFormat::Png).unwrap();
        assert!(!encode_webp(png.get_ref(), 80.0).unwrap().is_empty());
    }
}
