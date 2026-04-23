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
from pathlib import Path
from typing import Any

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
               SUM(CASE WHEN flood_prob <  ? AND label_real = 1 THEN 1 ELSE 0 END) AS fn
        FROM {NS}.gold_h3_flood_predictions
        WHERE aoi_name = ? AND scenario_24h_mm = ?
        """,
        (threshold, threshold, threshold, threshold, aoi, scenario),
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
