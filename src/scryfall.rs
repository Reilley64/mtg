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
    /// Candidate (size, url) pairs to try in order: the requested size first, then the rest by
    /// descending resolution (a bigger image downscales cleanly). New sets often have only some
    /// sizes live on the CDN, so we try every available one rather than giving up on the first 404.
    fn urls(&self, size: ImageSize) -> Vec<(ImageSize, String)> {
        use ImageSize::*;
        let chain = match size {
            Png => [(Png, &self.png), (Large, &self.large), (Normal, &self.normal), (Small, &self.small)],
            Large => [(Large, &self.large), (Png, &self.png), (Normal, &self.normal), (Small, &self.small)],
            Normal => [(Normal, &self.normal), (Png, &self.png), (Large, &self.large), (Small, &self.small)],
            Small => [(Small, &self.small), (Png, &self.png), (Large, &self.large), (Normal, &self.normal)],
        };
        chain
            .into_iter()
            .filter_map(|(s, u)| u.clone().map(|u| (s, u)))
            .collect()
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
    /// Candidate (size, url) pairs in preference order; `store` tries each until one isn't a 404.
    pub candidates: Vec<(ImageSize, String)>,
    pub id: String,
    pub face: usize,
}

impl Card {
    /// Image jobs for this card. Single-faced & split cards yield one (face 0);
    /// transform/MDFC cards yield one per face that carries its own art.
    pub fn jobs(&self, size: ImageSize) -> Vec<Job> {
        if let Some(uris) = &self.image_uris {
            let candidates = uris.urls(size);
            if !candidates.is_empty() {
                return vec![Job { candidates, id: self.id.clone(), face: 0 }];
            }
        }
        self.card_faces
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                let candidates = f.image_uris.as_ref()?.urls(size);
                (!candidates.is_empty()).then(|| Job { candidates, id: self.id.clone(), face: i })
            })
            .collect()
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
        // The universal back is long-released and exists at every size, so no fallbacks needed.
        candidates: vec![(size, format!("https://backs.scryfall.io/{seg}/{c0}/{c1}/{CARD_BACK_ID}.{ext}"))],
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
