//! MTG card image puller: Scryfall bulk data -> download -> WebP -> hosted, indexable store.

mod scryfall;

use std::{
    collections::HashMap,
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
    #[arg(long, default_value_t = 100.0)]
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
    // Per dataset+size state (images live under <out>/<size>/): the manifest tracks each stored
    // image's source size + quality; `last-updated` records the feed+quality of the last fully-ideal
    // run. The bulk cache above is size-independent, so it stays keyed by dataset only.
    let key = format!(
        "{}-{}",
        args.dataset,
        args.image.to_possible_value().unwrap().get_name()
    );
    let state_path = puller.out.join(format!(".cache/{key}.last-updated"));
    let manifest_path = puller.out.join(format!(".cache/{key}.manifest"));
    // Bust the short-circuit when quality changes, not just when the feed does.
    let clean_marker = format!("{version} q{}", args.quality);

    // Nothing to do: the last clean run already covered this exact feed at this quality.
    if !args.refresh && std::fs::read_to_string(&state_path).ok().as_deref() == Some(&clean_marker) {
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

    // 4. Skip images already stored at the requested size and quality. Re-pull anything missing,
    //    stored from a smaller fallback (to upgrade it), or stored at a different quality.
    let manifest = load_manifest(&manifest_path);
    let want_h = args.image.dims().1;
    let n = jobs.len();
    let todo: Vec<Job> = jobs
        .into_iter()
        .filter(|j| match manifest.get(&(j.id.clone(), j.face)) {
            Some(&(src_h, q)) => {
                !(src_h >= want_h && q == args.quality && puller.dest_path(j).exists())
            }
            None => true,
        })
        .collect();
    tracing::info!("{} up to date, {} to fetch", n - todo.len(), todo.len());

    // 5. Fetch -> decode -> WebP -> store, bounded concurrency. Borrowed, not spawned,
    //    so the futures can share &puller / &done without Arc or per-task clones.
    let done = AtomicUsize::new(0);
    let total = todo.len();
    let (puller, done) = (&puller, &done);
    let results: Vec<((String, usize), u32)> = stream::iter(todo)
        .map(|job| async move {
            match puller.store(&job).await {
                Ok(size) => {
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % 500 == 0 {
                        tracing::info!("{n}/{total} stored");
                    }
                    Some(((job.id, job.face), size.dims().1))
                }
                Err(e) => {
                    tracing::warn!("{} ({}): {e:#}", job.id, job.candidates[0].1);
                    None
                }
            }
        })
        .buffer_unordered(args.concurrency)
        .filter_map(|r| async move { r })
        .collect()
        .await;

    // 6. Record each stored image's source height + quality; count those still below the
    //    requested size (a fallback) so we know the run isn't fully ideal yet.
    let mut manifest = manifest;
    let mut degraded = 0;
    for (k, src_h) in &results {
        if *src_h < want_h {
            degraded += 1;
        }
        manifest.insert(k.clone(), (*src_h, args.quality));
    }
    save_manifest(&manifest_path, &manifest)?;

    // 7. Mark the feed+quality fully done only when every image is at full size and nothing failed,
    //    so fallbacks and failures keep getting retried on later runs.
    let (stored, failed) = (results.len(), total - results.len());
    if failed > 0 {
        tracing::warn!("{failed} failed; not recording feed version so next run retries");
    } else if degraded > 0 {
        tracing::warn!("{degraded} stored at a smaller fallback size; will retry for full size");
    } else if args.limit.is_some() {
        tracing::warn!("--limit set: partial pull, not recording feed version");
    } else {
        std::fs::write(&state_path, &clean_marker)
            .with_context(|| format!("writing {state_path:?}"))?;
    }

    tracing::info!("done: {stored} stored ({degraded} via fallback), {failed} failed");
    Ok(())
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Maps (card id, face) -> (source image height stored, quality used). Plain text, one per line:
/// `<id> <face> <height> <quality>`. Missing/corrupt -> empty (everything gets re-pulled).
type Manifest = HashMap<(String, usize), (u32, f32)>;

fn load_manifest(path: &Path) -> Manifest {
    let mut m = Manifest::new();
    let Ok(text) = std::fs::read_to_string(path) else {
        return m;
    };
    for line in text.lines() {
        let mut f = line.split_whitespace();
        if let (Some(id), Some(face), Some(h), Some(q)) = (f.next(), f.next(), f.next(), f.next())
            && let (Ok(face), Ok(h), Ok(q)) = (face.parse(), h.parse(), q.parse())
        {
            m.insert((id.to_string(), face), (h, q));
        }
    }
    m
}

fn save_manifest(path: &Path, m: &Manifest) -> Result<()> {
    let mut out = String::with_capacity(m.len() * 48);
    for ((id, face), (h, q)) in m {
        out.push_str(&format!("{id} {face} {h} {q}\n"));
    }
    std::fs::write(path, out).with_context(|| format!("writing {path:?}"))
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

    /// GET image bytes; `Ok(None)` means 404, so the caller can try a fallback.
    async fn fetch(&self, url: &str) -> Result<Option<Vec<u8>>> {
        let resp = self.client.get(url).send().await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(resp.error_for_status()?.bytes().await?.to_vec()))
    }

    /// Download one image, convert to WebP, write it; returns the size actually used.
    async fn store(&self, job: &Job) -> Result<ImageSize> {
        // Try each candidate size in order; skip 404s (a size not yet live on the CDN) and
        // take the first that resolves. Non-404 errors propagate so the job retries next run.
        for (size, url) in &job.candidates {
            let Some(bytes) = self.fetch(url).await? else {
                continue;
            };

            // Decode + encode are CPU-bound and synchronous -> off the async runtime.
            let (quality, max) = (self.quality, self.image.dims());
            let webp =
                tokio::task::spawn_blocking(move || encode_webp(&bytes, quality, max)).await??;

            // Write to a temp sibling then rename: a kill mid-write must not leave a
            // truncated file that the skip-existing check would treat as complete.
            let dest = self.dest_path(job);
            tokio::fs::create_dir_all(dest.parent().unwrap()).await?;
            let tmp = dest.with_extension("webp.partial");
            tokio::fs::write(&tmp, webp).await?;
            tokio::fs::rename(&tmp, &dest).await?;
            return Ok(*size);
        }
        anyhow::bail!("image not found (404 for every size)")
    }
}

fn encode_webp(bytes: &[u8], quality: f32, max: (u32, u32)) -> Result<Vec<u8>> {
    let mut img = image::load_from_memory(bytes).context("decoding image")?;
    // Downscale only if oversized (the png fallback): Lanczos3 preserves aspect and keeps text
    // crisp; primary images already match `max` so this is a no-op for them.
    if img.width() > max.0 || img.height() > max.1 {
        img = img.resize(max.0, max.1, image::imageops::FilterType::Lanczos3);
    }
    // webp's from_image only takes RGB8/RGBA8; normalize grayscale/16-bit/etc. to RGB8 and
    // flatten any transparency (png corners) onto black so the output is consistently opaque.
    let rgb = if img.color().has_alpha() {
        flatten_over_black(&img.to_rgba8())
    } else {
        img.to_rgb8()
    };
    let img = image::DynamicImage::ImageRgb8(rgb);
    let encoder = webp::Encoder::from_image(&img).map_err(|e| anyhow!("webp encode: {e}"))?;
    let mem = if quality >= 100.0 {
        encoder.encode_lossless()
    } else {
        encoder.encode(quality)
    };
    Ok(mem.to_vec())
}

/// Composite an RGBA image onto a black background, dropping the alpha channel.
fn flatten_over_black(rgba: &image::RgbaImage) -> image::RgbImage {
    image::RgbImage::from_fn(rgba.width(), rgba.height(), |x, y| {
        let p = rgba.get_pixel(x, y).0;
        let a = p[3] as u16;
        image::Rgb([
            (p[0] as u16 * a / 255) as u8,
            (p[1] as u16 * a / 255) as u8,
            (p[2] as u16 * a / 255) as u8,
        ])
    })
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
        assert!(!encode_webp(png.get_ref(), 80.0, (4096, 4096)).unwrap().is_empty());
    }

    #[test]
    fn flatten_drops_transparency_to_black() {
        let mut img = image::RgbaImage::new(2, 2);
        img.put_pixel(0, 0, image::Rgba([255, 255, 255, 0])); // transparent -> black
        img.put_pixel(1, 1, image::Rgba([10, 20, 30, 255])); // opaque -> unchanged
        let rgb = super::flatten_over_black(&img);
        assert_eq!(rgb.get_pixel(0, 0).0, [0, 0, 0]);
        assert_eq!(rgb.get_pixel(1, 1).0, [10, 20, 30]);
    }

    /// A transparent png (the shape of the png fallback) must encode without error.
    #[test]
    fn encodes_transparent_png() {
        let img = image::DynamicImage::ImageRgba8(image::RgbaImage::new(8, 8));
        let mut png = std::io::Cursor::new(Vec::new());
        img.write_to(&mut png, image::ImageFormat::Png).unwrap();
        assert!(!encode_webp(png.get_ref(), 80.0, (4096, 4096)).unwrap().is_empty());
    }
}
