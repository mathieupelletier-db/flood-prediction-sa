export type AOI = {
  aoi_name: string;
  min_lon: number; min_lat: number; max_lon: number; max_lat: number;
};

export type PredictionCell = {
  h3: string;
  flood_prob: number;
  min_elev: number | null;
  slope_deg: number | null;
  dist_to_water_m: number | null;
  annual_precip_mm: number | null;
  max24h_precip_mm: number | null;
  label_real: number;
  geometry: GeoJSON.Polygon | Record<string, unknown>;
};

export type PredictionsResponse = {
  aoi_name: string;
  scenario_24h_mm: number;
  threshold: number;
  count: number;
  cells: PredictionCell[];
};

export type FloodEvent = { year: string; geometry: GeoJSON.Geometry };
export type FloodEventsResponse = { aoi_name: string; count: number; events: FloodEvent[] };

export type ScenariosResponse = { aoi_name: string; scenarios_24h_mm: number[] };

export type Metrics = {
  aoi_name: string;
  scenario_24h_mm: number;
  total_cells: number;
  mean_prob: number;
  high_risk_rate: number;
  real_flood_cells: number;
  precision_vs_real: number | null;
  recall_vs_real: number | null;
};

async function fetchJson<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} -> ${r.status} ${r.statusText}`);
  return (await r.json()) as T;
}

export const api = {
  listAoi: () => fetchJson<AOI[]>("/api/aoi"),
  listScenarios: (aoi: string) =>
    fetchJson<ScenariosResponse>(`/api/scenarios?aoi=${encodeURIComponent(aoi)}`),
  predictions: (aoi: string, scenarioMm: number, threshold: number, limit = 20000) =>
    fetchJson<PredictionsResponse>(
      `/api/predictions?aoi=${encodeURIComponent(aoi)}&scenario_mm=${scenarioMm}` +
        `&threshold=${threshold}&limit=${limit}`,
    ),
  floodEvents: (aoi: string) =>
    fetchJson<FloodEventsResponse>(`/api/flood_events?aoi=${encodeURIComponent(aoi)}`),
  metrics: (aoi: string, scenarioMm: number, threshold: number) =>
    fetchJson<Metrics>(
      `/api/metrics?aoi=${encodeURIComponent(aoi)}&scenario_mm=${scenarioMm}&threshold=${threshold}`,
    ),
};
