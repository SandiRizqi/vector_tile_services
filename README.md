# High-Performance Vector Tile Service (Rust + PostGIS)

A **high-performance vector tile service** written in **Rust**, designed to publish
**PostGIS / geospatial data** as **Mapbox Vector Tiles (MVT)**.

This service is optimized for:
- Large geospatial datasets
- On-the-fly tile generation
- Low latency & high concurrency
- Production-grade deployment

---

## Why This Project?

Most vector tile servers are either:
- Heavy (Java-based, large memory footprint), or
- Hard to customize deeply

This project aims to provide:
- ⚡ **Blazing fast performance** using Rust
- 🧠 Fine-grained control over SQL, geometry simplification, and indexing
- 🌍 Native support for PostGIS spatial queries
- 🧩 Easy integration with web maps (MapLibre, Mapbox GL, Leaflet, Flutter, etc.)

---

## Features

- 🚀 Serve **Mapbox Vector Tiles (`.pbf`)** directly from PostGIS
- 🗺️ Automatic geometry simplification per zoom level
- 📐 Dynamic bounding box calculation
- 🔍 Uses spatial index (`GIST`) for fast tile queries
- 🧵 Async & concurrent (Actix Web + SQLx)
- 🔧 Simple configuration via `.env`
- 🐧🪟 Cross-platform (Linux & Windows)
