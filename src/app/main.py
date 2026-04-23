"""FastAPI backend for the flood prediction Databricks App.

Serves three JSON endpoints (`/api/aoi`, `/api/predictions`, `/api/flood_events`,
`/api/metrics`) backed by a Databricks SQL warehouse, plus the compiled React SPA
from `./client/dist` at `/`.

Authentication on Databricks Apps is provided by the app resource's service
principal - we pick up the PAT/host from the `DATABRICKS_*` env vars that the
platform injects.
"""

from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import h3
import requests
from databricks import sql as dbsql
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("flood-app")

CATALOG = os.environ.get("DATABRICKS_CATALOG", "flood_demo")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "montreal")
DEFAULT_AOI = os.environ.get("DEFAULT_AOI", "greater_montreal")
MAX_CELLS = int(os.environ.get("MAX_CELLS_PER_REQUEST", "50000"))

NS = f"`{CATALOG}`.`{SCHEMA}`"

# Databricks Apps injects DATABRICKS_HOST, DATABRICKS_HTTP_PATH (warehouse) and
# DATABRICKS_TOKEN / or oauth automatically when a SQL Warehouse resource is attached.
HOST = os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_WORKSPACE_HOSTNAME")
HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH") or os.environ.get("DATABRICKS_WAREHOUSE_HTTP_PATH")
TOKEN = os.environ.get("DATABRICKS_TOKEN")
CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET")

if not (HOST and HTTP_PATH):
    log.warning("DATABRICKS_HOST / DATABRICKS_HTTP_PATH not set - API calls will fail")


def _connect():
    """Open a SQL warehouse connection using whichever auth mode is available."""
    kwargs: dict[str, Any] = {"server_hostname": HOST, "http_path": HTTP_PATH}
    if TOKEN:
        kwargs["access_token"] = TOKEN
    elif CLIENT_ID and CLIENT_SECRET:
        from databricks.sdk.core import Config, oauth_service_principal

        cfg = Config(host=f"https://{HOST}", client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        kwargs["credentials_provider"] = lambda: oauth_service_principal(cfg)
    else:
        raise RuntimeError("No Databricks auth available (set DATABRICKS_TOKEN or CLIENT_ID/SECRET)")
    return dbsql.connect(**kwargs)


def _fetch(query: str, params: tuple | None = None) -> list[dict[str, Any]]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(query, params or ())
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _h3_hex(v: Any) -> str:
    """Normalize an H3 cell id (stored as BIGINT in Delta) to the hex string
    format deck.gl's H3HexagonLayer and h3-js expect."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    # Python int / Decimal -> 15-char hex (H3 is 64-bit; leading zeroes matter)
    return f"{int(v):x}"


# Pydantic schemas ---------------------------------------------------------------


class AOI(BaseModel):
    aoi_name: str
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float


class PredictionCell(BaseModel):
    h3: str
    flood_prob: float
    min_elev: float | None = None
    slope_deg: float | None = None
    dist_to_water_m: float | None = None
    annual_precip_mm: float | None = None
    max24h_precip_mm: float | None = None
    building_count: int | None = None
    residential_count: int | None = None
    expected_buildings_at_risk: float | None = None
    label_real: int = 0
    geometry: dict[str, Any] = Field(description="GeoJSON Polygon for the H3 cell")


class PredictionsResponse(BaseModel):
    aoi_name: str
    scenario_24h_mm: int
    threshold: float
    count: int
    cells: list[PredictionCell]


class ScenariosResponse(BaseModel):
    aoi_name: str
    scenarios_24h_mm: list[int]


class FloodEvent(BaseModel):
    year: str
    geometry: dict[str, Any]


class FloodEventsResponse(BaseModel):
    aoi_name: str
    count: int
    events: list[FloodEvent]


class Metrics(BaseModel):
    aoi_name: str
    scenario_24h_mm: int
    total_cells: int
    mean_prob: float
    high_risk_rate: float
    real_flood_cells: int
    precision_vs_real: float | None = None
    recall_vs_real: float | None = None
    expected_buildings_at_risk: float = 0.0
    expected_residential_at_risk: float = 0.0
    high_risk_cells_with_buildings: int = 0


class AddressLookup(BaseModel):
    query: str
    resolved_name: str
    lat: float
    lon: float
    h3: str
    scenario_24h_mm: int
    flood_prob: float
    min_elev: float | None = None
    slope_deg: float | None = None
    dist_to_water_m: float | None = None
    annual_precip_mm: float | None = None
    max24h_precip_mm: float | None = None
    building_count: int | None = None
    residential_count: int | None = None
    expected_buildings_at_risk: float | None = None
    sweep: list[tuple[int, float]] = Field(
        default_factory=list,
        description="[(scenario_24h_mm, flood_prob), ...] across all available scenarios",
    )


# App --------------------------------------------------------------------------

app = FastAPI(title="Flood Prediction API", version="0.1.0")


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"ok": True, "catalog": CATALOG, "schema": SCHEMA, "default_aoi": DEFAULT_AOI}


@app.get("/api/aoi", response_model=list[AOI])
def list_aoi() -> list[AOI]:
    rows = _fetch(f"SELECT aoi_name, min_lon, min_lat, max_lon, max_lat FROM {NS}.gold_aoi")
    return [AOI(**r) for r in rows]


@app.get("/api/scenarios", response_model=ScenariosResponse)
def list_scenarios(aoi: str = Query(default=DEFAULT_AOI)) -> ScenariosResponse:
    rows = _fetch(
        f"SELECT DISTINCT scenario_24h_mm FROM {NS}.gold_scenarios"
        " WHERE aoi_name = ? ORDER BY scenario_24h_mm",
        (aoi,),
    )
    return ScenariosResponse(
        aoi_name=aoi,
        scenarios_24h_mm=[int(r["scenario_24h_mm"]) for r in rows],
    )


def _nearest_scenario(aoi: str, mm: int) -> int:
    """Snap arbitrary slider values to one of the pre-scored scenarios."""
    rows = _fetch(
        f"SELECT DISTINCT scenario_24h_mm FROM {NS}.gold_scenarios WHERE aoi_name = ?",
        (aoi,),
    )
    available = sorted(int(r["scenario_24h_mm"]) for r in rows)
    if not available:
        return mm
    return min(available, key=lambda s: abs(s - mm))


@app.get("/api/predictions", response_model=PredictionsResponse)
def predictions(
    aoi: str = Query(default=DEFAULT_AOI),
    scenario_mm: int = Query(default=60, ge=0, le=500,
                              description="24-hour rainfall scenario in mm"),
    threshold: float = Query(default=0.0, ge=0.0, le=1.0,
                              description="Only return cells with flood_prob >= threshold"),
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    limit: int = Query(default=MAX_CELLS, le=MAX_CELLS),
) -> PredictionsResponse:
    scenario = _nearest_scenario(aoi, scenario_mm)

    where = ["aoi_name = ?", "scenario_24h_mm = ?", "flood_prob >= ?"]
    params: list[Any] = [aoi, scenario, threshold]

    if None not in (min_lon, min_lat, max_lon, max_lat):
        # Use Spatial SQL to bbox-filter by the H3 centroid - keeps payloads small.
        where.append(
            "ST_Intersects(ST_GeomFromGeoJSON(h3_centerasgeojson(h3)),"
            " ST_GeomFromText(?, 4326))"
        )
        params.append(
            "POLYGON(({a} {b}, {c} {b}, {c} {d}, {a} {d}, {a} {b}))".format(
                a=min_lon, b=min_lat, c=max_lon, d=max_lat
            )
        )

    where_sql = " AND ".join(where)
    q = f"""
        SELECT h3, flood_prob, min_elev, slope_deg, dist_to_water_m,
               annual_precip_mm, max24h_precip_mm,
               building_count, residential_count, expected_buildings_at_risk,
               label_real, geometry_geojson
        FROM {NS}.gold_h3_flood_predictions
        WHERE {where_sql}
        ORDER BY flood_prob DESC
        LIMIT {int(limit)}
    """
    rows = _fetch(q, tuple(params))
    cells = [
        PredictionCell(
            h3=_h3_hex(r["h3"]),
            flood_prob=float(r["flood_prob"] or 0.0),
            min_elev=float(r["min_elev"]) if r["min_elev"] is not None else None,
            slope_deg=float(r["slope_deg"]) if r["slope_deg"] is not None else None,
            dist_to_water_m=float(r["dist_to_water_m"]) if r["dist_to_water_m"] is not None else None,
            annual_precip_mm=float(r["annual_precip_mm"]) if r["annual_precip_mm"] is not None else None,
            max24h_precip_mm=float(r["max24h_precip_mm"]) if r["max24h_precip_mm"] is not None else None,
            building_count=int(r["building_count"]) if r.get("building_count") is not None else None,
            residential_count=int(r["residential_count"]) if r.get("residential_count") is not None else None,
            expected_buildings_at_risk=(
                float(r["expected_buildings_at_risk"])
                if r.get("expected_buildings_at_risk") is not None else None
            ),
            label_real=int(r["label_real"] or 0),
            geometry=json.loads(r["geometry_geojson"]) if r["geometry_geojson"] else {},
        )
        for r in rows
    ]
    return PredictionsResponse(
        aoi_name=aoi, scenario_24h_mm=scenario,
        threshold=threshold, count=len(cells), cells=cells,
    )


@app.get("/api/flood_events", response_model=FloodEventsResponse)
def flood_events(aoi: str = Query(default=DEFAULT_AOI), year: str | None = None) -> FloodEventsResponse:
    params: list[Any] = [aoi]
    extra = ""
    if year:
        extra = " AND year = ?"
        params.append(year)
    rows = _fetch(
        f"SELECT year, geometry_geojson FROM {NS}.gold_flood_events WHERE aoi_name = ?{extra}",
        tuple(params),
    )
    events = [FloodEvent(year=r["year"] or "", geometry=json.loads(r["geometry_geojson"])) for r in rows]
    return FloodEventsResponse(aoi_name=aoi, count=len(events), events=events)


@app.get("/api/metrics", response_model=Metrics)
def metrics(
    aoi: str = Query(default=DEFAULT_AOI),
    scenario_mm: int = Query(default=60, ge=0, le=500),
    threshold: float = 0.5,
) -> Metrics:
    scenario = _nearest_scenario(aoi, scenario_mm)
    row = _fetch(
        f"""
        SELECT COUNT(*)                                                         AS total,
               AVG(flood_prob)                                                  AS mean_prob,
               AVG(CASE WHEN flood_prob >= ? THEN 1.0 ELSE 0.0 END)             AS high_rate,
               SUM(label_real)                                                  AS real_pos,
               SUM(CASE WHEN flood_prob >= ? AND label_real = 1 THEN 1 ELSE 0 END) AS tp,
               SUM(CASE WHEN flood_prob >= ? AND label_real = 0 THEN 1 ELSE 0 END) AS fp,
               SUM(CASE WHEN flood_prob <  ? AND label_real = 1 THEN 1 ELSE 0 END) AS fn,
               SUM(expected_buildings_at_risk)                                    AS exp_bld,
               SUM(flood_prob * COALESCE(residential_count, 0))                   AS exp_res,
               SUM(CASE WHEN flood_prob >= ? AND COALESCE(building_count,0) > 0
                        THEN 1 ELSE 0 END)                                        AS hi_bld_cells
        FROM {NS}.gold_h3_flood_predictions
        WHERE aoi_name = ? AND scenario_24h_mm = ?
        """,
        (threshold, threshold, threshold, threshold, threshold, aoi, scenario),
    )[0]
    tp, fp, fn = (int(row[k] or 0) for k in ("tp", "fp", "fn"))
    precision = tp / (tp + fp) if (tp + fp) else None
    recall = tp / (tp + fn) if (tp + fn) else None
    return Metrics(
        aoi_name=aoi,
        scenario_24h_mm=scenario,
        total_cells=int(row["total"] or 0),
        mean_prob=float(row["mean_prob"] or 0.0),
        high_risk_rate=float(row["high_rate"] or 0.0),
        real_flood_cells=int(row["real_pos"] or 0),
        precision_vs_real=precision,
        recall_vs_real=recall,
        expected_buildings_at_risk=float(row["exp_bld"] or 0.0),
        expected_residential_at_risk=float(row["exp_res"] or 0.0),
        high_risk_cells_with_buildings=int(row["hi_bld_cells"] or 0),
    )


# Address lookup --------------------------------------------------------------

NOMINATIM_URL = os.environ.get("NOMINATIM_URL", "https://nominatim.openstreetmap.org/search")
NOMINATIM_USER_AGENT = os.environ.get(
    "NOMINATIM_USER_AGENT",
    "flood-prediction-demo/0.1 (github.com/mathieupelletier-db/flood-prediction-sa)",
)


@lru_cache(maxsize=512)
def _geocode(query: str, bbox: tuple[float, float, float, float] | None) -> dict[str, Any] | None:
    params = {
        "q": query,
        "format": "jsonv2",
        "limit": 1,
        "addressdetails": 0,
    }
    if bbox is not None:
        # Nominatim `viewbox` is minlon,maxlat,maxlon,minlat (top-left + bottom-right)
        params["viewbox"] = f"{bbox[0]},{bbox[3]},{bbox[2]},{bbox[1]}"
        params["bounded"] = 1
    r = requests.get(
        NOMINATIM_URL, params=params,
        headers={"User-Agent": NOMINATIM_USER_AGENT, "Accept-Language": "en,fr"},
        timeout=15,
    )
    r.raise_for_status()
    hits = r.json() or []
    return hits[0] if hits else None


def _aoi_bbox(aoi: str) -> tuple[float, float, float, float] | None:
    rows = _fetch(
        f"SELECT min_lon, min_lat, max_lon, max_lat FROM {NS}.gold_aoi WHERE aoi_name = ?",
        (aoi,),
    )
    if not rows:
        return None
    r = rows[0]
    return (float(r["min_lon"]), float(r["min_lat"]),
            float(r["max_lon"]), float(r["max_lat"]))


@app.get("/api/lookup", response_model=AddressLookup)
def lookup(
    q: str = Query(..., min_length=3, description="Address or place name to search"),
    aoi: str = Query(default=DEFAULT_AOI),
    scenario_mm: int = Query(default=100, ge=0, le=500),
) -> AddressLookup:
    bbox = _aoi_bbox(aoi)
    hit = _geocode(q, bbox)
    if hit is None:
        # Retry once without the bbox constraint so out-of-AOI addresses still
        # resolve (the map won't show a prediction for them, but the caller
        # can display a "not in AOI" message).
        hit = _geocode(q, None)
    if hit is None:
        raise HTTPException(status_code=404, detail=f"No geocoding result for '{q}'")

    lat, lon = float(hit["lat"]), float(hit["lon"])
    cell_str = h3.latlng_to_cell(lat, lon, 9)
    # h3-py v4 returns the canonical hex string already.
    cell_str = cell_str if isinstance(cell_str, str) else f"{int(cell_str):x}"
    cell_int = int(cell_str, 16)

    scenario = _nearest_scenario(aoi, scenario_mm)
    cur = _fetch(
        f"""
        SELECT flood_prob, min_elev, slope_deg, dist_to_water_m,
               annual_precip_mm, max24h_precip_mm,
               building_count, residential_count, expected_buildings_at_risk
        FROM {NS}.gold_h3_flood_predictions
        WHERE aoi_name = ? AND h3 = ? AND scenario_24h_mm = ?
        """,
        (aoi, cell_int, scenario),
    )
    cur_row = cur[0] if cur else {}

    sweep_rows = _fetch(
        f"""
        SELECT scenario_24h_mm, flood_prob
        FROM {NS}.gold_h3_flood_predictions
        WHERE aoi_name = ? AND h3 = ?
        ORDER BY scenario_24h_mm
        """,
        (aoi, cell_int),
    )
    sweep = [(int(r["scenario_24h_mm"]), float(r["flood_prob"])) for r in sweep_rows]

    return AddressLookup(
        query=q,
        resolved_name=str(hit.get("display_name") or q),
        lat=lat, lon=lon,
        h3=cell_str,
        scenario_24h_mm=scenario,
        flood_prob=float(cur_row.get("flood_prob") or 0.0),
        min_elev=float(cur_row["min_elev"]) if cur_row.get("min_elev") is not None else None,
        slope_deg=float(cur_row["slope_deg"]) if cur_row.get("slope_deg") is not None else None,
        dist_to_water_m=(
            float(cur_row["dist_to_water_m"])
            if cur_row.get("dist_to_water_m") is not None else None
        ),
        annual_precip_mm=(
            float(cur_row["annual_precip_mm"])
            if cur_row.get("annual_precip_mm") is not None else None
        ),
        max24h_precip_mm=(
            float(cur_row["max24h_precip_mm"])
            if cur_row.get("max24h_precip_mm") is not None else None
        ),
        building_count=(
            int(cur_row["building_count"])
            if cur_row.get("building_count") is not None else None
        ),
        residential_count=(
            int(cur_row["residential_count"])
            if cur_row.get("residential_count") is not None else None
        ),
        expected_buildings_at_risk=(
            float(cur_row["expected_buildings_at_risk"])
            if cur_row.get("expected_buildings_at_risk") is not None else None
        ),
        sweep=sweep,
    )


# Static SPA -------------------------------------------------------------------

CLIENT_DIR = Path(__file__).parent / "client" / "dist"
if CLIENT_DIR.exists():
    app.mount("/assets", StaticFiles(directory=CLIENT_DIR / "assets"), name="assets")

    @app.get("/")
    @app.get("/{_path:path}")
    def spa(_path: str = ""):
        index = CLIENT_DIR / "index.html"
        if not index.exists():
            raise HTTPException(status_code=404, detail="SPA build missing")
        return FileResponse(index)
else:
    @app.get("/")
    def no_spa():
        return {"message": "SPA not built yet. Run `bun install && bun run build` in src/app/client/."}
