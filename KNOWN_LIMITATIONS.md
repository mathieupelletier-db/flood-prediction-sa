# Known Limitations

This is a reference architecture / solution accelerator, not a production flood
model. The list below captures the limitations we are aware of and have
explicitly chosen to live with for the demo. Each item points at the file(s)
where the trade-off is taken so a future contributor knows where to start if
they want to lift it.

## Data and modelling

### H3 cells over open water are not always filtered out

**Symptom.** Over wide rivers (e.g. the Hudson when the AOI is retargeted at
NYC, or the Saint-Laurent in the default Greater Montreal AOI) you can see
H3 hexagons rendered on top of the river surface in the app, even though
flooding semantics don't apply to a cell that is already 100% water.

**Root cause.** Two compounding issues in the silver pipeline:

1. **OSM multipolygon relations are dropped at ingest.**
   `notebooks/01_ingest.py` queries Overpass for both
   `way["natural"="water"]` and `relation["natural"="water"]`, but the
   feature-conversion loop only handles `el["type"] == "way"`. Wide rivers
   and large lakes are commonly modelled in OSM as **multipolygon
   relations**, so their polygon geometry never makes it into
   `bronze_hydrography`. Only `waterway=river` and `waterway=stream` lines
   (the river *centerlines*) survive, as `LineString` features.

2. **The "drop in-water cells" filter uses centroid-to-feature distance.**
   `notebooks/02_silver_geobrix.py` (section 6) drops cells where
   `dist_to_water_m < 5 m`, computed from the cell *centroid* to the nearest
   bronze water feature. With only river centerlines available, an H3 cell
   sitting in the middle of a 1 km wide river — but offset 200 m from the
   centerline — has `dist_to_water_m ≈ 200 m` and passes the filter, even
   though the entire hexagon is over water.

**Why we are leaving it.** Fixing this end-to-end requires (a) reassembling
multipolygon relations from Overpass `out geom;` member arrays in the ingest
step, and (b) replacing the centroid-distance mask with a hexagon-vs-water
*area-overlap* mask (build the H3 boundary polygon via
`h3.cell_to_boundary`, intersect with the unioned water layer, drop cells
where `intersection_area / hex_area > 0.5`). Both are tractable but add
non-trivial complexity to a pipeline whose primary purpose is to demonstrate
the GeoBrix + Spatial SQL + H3 + MLflow + Apps story end-to-end. The visual
artefact does not affect the reported precision/recall metrics, since the
real flood polygons used for validation also do not cover open river
surfaces.

**Workarounds for users who care.**

- Tighten the AOI bbox so it doesn't span large open-water bodies.
- Lower `WATER_DIST_THRESHOLD_M` in `02_silver_geobrix.py` from `5.0` to
  something larger (e.g. `100.0`). This is crude — it will also remove
  legitimately flood-prone riparian cells — but it visibly improves the
  app for screenshots.
- Implement the proper fix described above; the work is contained to
  `01_ingest.py` and the silver "drop in-water cells" section.

### Hybrid labels mix synthetic and real

The model trains on a rainfall-aware **synthetic susceptibility label**, with
the real 2017/2019 flood polygons held out for validation only. This is a
deliberate demo pattern (see `BLOG.md` → "Hybrid labels: honest about what is
synthetic"), not a bug, but it means the absolute probability values from
the model are not calibrated against real flood frequencies. Production use
would substitute insurance claims, expanded historical events, or radar QPE
for the synthetic signal.

### SRTM 30 m DEM, not HRDEM 1 m

Elevation comes from the global SRTM 1-arc-second tiles on AWS Open Data,
which is roughly 30 m horizontal resolution. Canada's HRDEM is 1 m where it
exists, which would dramatically sharpen slope and low-point detection in
urban areas. The pipeline is tile-agnostic — swapping the DEM source only
changes `notebooks/01_ingest.py`.

### Precipitation is climatology, not forecast

The rainfall-scenario slider blends the model with **historical climatology**
(Open-Meteo ERA5 P99 24h / 5d aggregated to a 0.2° grid), not a live
forecast. There is no streaming ingestion of HRDPS / GFS / ECMWF forecasts.
For real-time alerting you would point silver step 5 at a live source.

### Slope is approximated, not derived from a raster kernel

Per-cell slope is computed as
`atan(max(|elev_diff_to_neighbour|) / 174 m)` over the H3 res-9 1-ring
(`02_silver_geobrix.py`, section 3). This is robust across GeoBrix versions
but coarser than a true raster slope kernel. For most of Greater Montreal
(low relief) the difference is negligible.

### Distance-to-water is computed on the driver

The `silver_h3_dist_water` step pulls all bronze water geometries to the
driver, builds an STRtree, and iterates H3 cells in Python. This is fast
enough for a Montreal-sized AOI (~50 km × 55 km, low tens of thousands of
cells) but is not how you would do this at province / country scale —
you'd push it down to Spatial SQL with `ST_Distance` after a proper SRID
fix-up, or use `gbx_st_*` functions if your GeoBrix build exposes them
consistently.

## Operational

### Free-tier API rate limits

Both Overpass (OSM) and Open-Meteo ERA5 are free, unauthenticated services
with rate limits. The ingest notebook caches everything to the AOI volume
on first run, so re-runs are fast, but the *first* run for a new AOI can
take several minutes and will fail loudly if the upstream service is
temporarily unavailable. Expand the retry budget in `fetch_overpass` and
`_fetch_open_meteo_daily` if you hit transient 429s.

### MELCC flood polygons are Quebec-only

`flood_source="melcc"` (the default) only returns features for AOIs that
intersect the Quebec MDDELCC ZIS extents. Retargeting the demo at a city
outside Quebec requires switching to `flood_source="cems"` (Copernicus EMS
Rapid Mapping, global, but you must supply activation URLs) or
`flood_source="none"` (no real-flood overlay; the model still trains on
synthetic labels). See the dispatch block in `01_ingest.py` for details.

### GeoBrix version drift

`02_silver_geobrix.py` deliberately calls `DESCRIBE FUNCTION EXTENDED` on
the GeoBrix RasterX functions it relies on at the top of the notebook so
the job log captures the exact signatures of the installed build.
Function signatures (especially `rst_clip`, `rst_mapalgebra`,
`rst_derivedband`) have moved across GeoBrix versions; if a future build
breaks the pipeline, the job log makes the diagnosis a one-line read.
