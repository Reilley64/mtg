//! Scryfall bulk-data discovery, card model, and image-job derivation.

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const BULK_DATA_URL: &str = "https://api.scryfall.com/bulk-data";

#[derive(Deserialize)]
struct BulkList {
    data: Vec<BulkEntry>,
}

#[derive(Deserialize)]
struct BulkEntry {
    #[serde(rename = "type")]
    kind: String,
    download_uri: String,
    /// ISO timestamp Scryfall bumps whenever this dataset gets new/changed cards.
    updated_at: String,
    size: Option<u64>,
}

pub struct BulkInfo {
    pub download_uri: String,
    pub updated_at: String,
}

/// Resolve the download URI + freshness stamp for a named bulk dataset.
pub async fn bulk_info(client: &reqwest::Client, dataset: &str) -> Result<BulkInfo> {
    let list: BulkList = client
        .get(BULK_DATA_URL)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("fetching bulk-data list")?;

    let entry = list
        .data
        .into_iter()
        .find(|e| e.kind == dataset)
        .with_context(|| format!("no bulk dataset named {dataset:?}"))?;

    if let Some(bytes) = entry.size {
        tracing::info!("bulk dataset {dataset} is {} MB", bytes / 1_000_000);
    }
    Ok(BulkInfo {
        download_uri: entry.download_uri,
        updated_at: entry.updated_at,
    })
}

#[derive(Deserialize)]
pub struct Card {
    pub id: String,
    #[serde(default)]
    image_uris: Option<ImageUris>,
    #[serde(default)]
    card_faces: Vec<CardFace>,
}

#[derive(Deserialize)]
struct CardFace {
    #[serde(default)]
    image_uris: Option<ImageUris>,
}

#[derive(Deserialize)]
struct ImageUris {
    small: Option<String>,
    normal: Option<String>,
    large: Option<String>,
    png: Option<String>,
}

impl ImageUris {
    fn pick(&self, size: ImageSize) -> Option<&str> {
        let chain = match size {
            ImageSize::Png => [&self.png, &self.large, &self.normal, &self.small],
            ImageSize::Large => [&self.large, &self.normal, &self.png, &self.small],
            ImageSize::Normal => [&self.normal, &self.large, &self.small, &self.png],
            ImageSize::Small => [&self.small, &self.normal, &self.large, &self.png],
        };
        chain.into_iter().flatten().next().map(String::as_str)
    }
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum ImageSize {
    Small,
    Normal,
    Large,
    Png,
}

impl ImageSize {
    /// Scryfall's fixed pixel dimensions for each size — the target a png fallback is downscaled to.
    pub fn dims(self) -> (u32, u32) {
        match self {
            ImageSize::Small => (146, 204),
            ImageSize::Normal => (488, 680),
            ImageSize::Large => (672, 936),
            ImageSize::Png => (745, 1040),
        }
    }
}

/// One downloadable image (a card, or one face of a multi-faced card).
pub struct Job {
    pub url: String,
    /// `png` URL to try if `url` 404s — new sets often lack some sizes on the CDN while png exists.
    pub fallback: Option<String>,
    pub id: String,
    pub face: usize,
}

impl Card {
    /// Image jobs for this card. Single-faced & split cards yield one (face 0);
    /// transform/MDFC cards yield one per face that carries its own art.
    pub fn jobs(&self, size: ImageSize) -> Vec<Job> {
        if let Some(uris) = &self.image_uris
            && let Some(url) = uris.pick(size)
        {
            return vec![self.job(uris, url, size, 0)];
        }
        self.card_faces
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                let uris = f.image_uris.as_ref()?;
                Some(self.job(uris, uris.pick(size)?, size, i))
            })
            .collect()
    }

    fn job(&self, uris: &ImageUris, url: &str, size: ImageSize, face: usize) -> Job {
        let fallback = match size {
            ImageSize::Png => None,
            _ => uris.png.clone().filter(|p| p != url),
        };
        Job { url: url.to_string(), fallback, id: self.id.clone(), face }
    }
}

/// The shared Magic card back — the same image on every normal card, used by
/// TTS as a deck's back. Stored like any other card so importers can reference it.
const CARD_BACK_ID: &str = "0aeebaf5-8c7d-4636-9e82-8c27447861f7";

pub fn card_back_job(size: ImageSize) -> Job {
    let (seg, ext) = match size {
        ImageSize::Png => ("png", "png"),
        ImageSize::Large => ("large", "jpg"),
        // backs.scryfall.io has no `small`; normal is the smallest available.
        ImageSize::Normal | ImageSize::Small => ("normal", "jpg"),
    };
    let (c0, c1) = (&CARD_BACK_ID[0..1], &CARD_BACK_ID[1..2]);
    Job {
        url: format!("https://backs.scryfall.io/{seg}/{c0}/{c1}/{CARD_BACK_ID}.{ext}"),
        fallback: None,
        id: CARD_BACK_ID.to_string(),
        face: 0,
    }
}

/// Parse a bulk-data file (a JSON array of card objects) from disk.
pub fn parse_bulk(path: &std::path::Path) -> Result<Vec<Card>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {path:?}"))?;
    let reader = std::io::BufReader::new(file);
    let cards: Vec<Card> = serde_json::from_reader(reader).context("parsing bulk JSON")?;
    if cards.is_empty() {
        bail!("bulk file parsed to zero cards");
    }
    Ok(cards)
}
