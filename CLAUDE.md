# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project goal

Rust CLI that pulls all Magic: The Gathering card images — including every print/variant of a card, not just one — converts them to WebP, and stores them so a web server can host them with a layout that's easily indexable by Tabletop Simulator (TTS) importers.

Scope is **download → convert to WebP → store**. This tool does *not* do the TTS import itself, build deck sheets, or generate TTS object JSON. It produces the hosted image library + index that downstream TTS importers consume.

Pipeline:
1. Fetch card metadata + image URLs from Scryfall (all prints).
2. Download each image, convert to WebP.
3. Store on disk in a stable, predictable path layout a static web server can serve, plus an index the importers can look cards up by.

## Status

Fresh scaffold — `src/main.rs` is still hello-world. Architecture below is intent, not yet built.

## Commands

```
cargo run -- <args>      # run the CLI
cargo build --release    # release binary at target/release/mtg
cargo test               # all tests
cargo test <name>        # single test by substring
cargo clippy             # lint
cargo fmt                # format
```

Edition is 2024 — needs a recent stable toolchain.

## Notes for building this out

- Card data source: Scryfall (`api.scryfall.com`). All prints of a card = the `prints_search_uri` / `unique=prints` query. Prefer the bulk data dumps (`/bulk-data`, "All Cards") over per-card API calls to enumerate everything in one download.
- Respect Scryfall rate limits (~10 req/s) when downloading images; cache by Scryfall card ID so re-runs skip already-fetched cards.
- Storage layout drives everything downstream — pick a stable scheme (e.g. by Scryfall ID) so the web server path and the index stay aligned and re-runnable.
