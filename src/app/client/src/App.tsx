import { useCallback, useEffect, useMemo, useState } from "react";
import type { PickingInfo } from "@deck.gl/core";
import { FloodMap, THEMES, type ViewState, type MapTheme } from "./FloodMap";
import { api, type AOI, type PredictionCell, type FloodEvent, type Metrics } from "./api";

const DEFAULT_VIEW: ViewState = {
  longitude: -73.65,
  latitude: 45.55,
  zoom: 10,
  pitch: 30,
  bearing: 0,
};

function snap(target: number, options: number[]): number {
  if (!options.length) return target;
  return options.reduce((best, v) =>
    Math.abs(v - target) < Math.abs(best - target) ? v : best,
  );
}

export default function App() {
  const [aois, setAois] = useState<AOI[]>([]);
  const [aoi, setAoi] = useState<string>("greater_montreal");
  const [scenarios, setScenarios] = useState<number[]>([]);
  const [scenarioMm, setScenarioMm] = useState(60);
  const [threshold, setThreshold] = useState(0.4);
  const [showFloods, setShowFloods] = useState(true);
  const [theme, setTheme] = useState<MapTheme>(() => {
    const saved = localStorage.getItem("flood-map-theme");
    return (saved === "dark" || saved === "light" || saved === "voyager") ? saved : "dark";
  });

  useEffect(() => {
    localStorage.setItem("flood-map-theme", theme);
    document.documentElement.dataset.theme = theme;
  }, [theme]);
  const [cells, setCells] = useState<PredictionCell[]>([]);
  const [events, setEvents] = useState<FloodEvent[]>([]);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [loading, setLoading] = useState(false);
  const [view, setView] = useState<ViewState>(DEFAULT_VIEW);
  const [hover, setHover] = useState<PickingInfo | null>(null);

  useEffect(() => {
    api.listAoi()
      .then((rows) => {
        setAois(rows);
        if (rows.length && !rows.find((r) => r.aoi_name === aoi)) setAoi(rows[0].aoi_name);
      })
      .catch(console.error);
  }, []);

  useEffect(() => {
    const found = aois.find((a) => a.aoi_name === aoi);
    if (found) {
      setView({
        longitude: (found.min_lon + found.max_lon) / 2,
        latitude: (found.min_lat + found.max_lat) / 2,
        zoom: 10, pitch: 30, bearing: 0,
      });
    }
  }, [aoi, aois]);

  useEffect(() => {
    api.listScenarios(aoi)
      .then((r) => {
        setScenarios(r.scenarios_24h_mm);
        if (r.scenarios_24h_mm.length) {
          setScenarioMm((prev) => snap(prev, r.scenarios_24h_mm));
        }
      })
      .catch(console.error);
  }, [aoi]);

  useEffect(() => {
    if (!scenarios.length) return;
    let cancelled = false;
    setLoading(true);
    const snapped = snap(scenarioMm, scenarios);
    Promise.all([
      api.predictions(aoi, snapped, threshold),
      api.floodEvents(aoi),
      api.metrics(aoi, snapped, Math.max(0.5, threshold)),
    ])
      .then(([p, e, m]) => {
        if (cancelled) return;
        setCells(p.cells);
        setEvents(e.events);
        setMetrics(m);
      })
      .catch((err) => console.error(err))
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, [aoi, scenarioMm, threshold, scenarios]);

  const onHover = useCallback((info: PickingInfo) => setHover(info), []);

  const snappedScenario = useMemo(
    () => (scenarios.length ? snap(scenarioMm, scenarios) : scenarioMm),
    [scenarioMm, scenarios],
  );

  const tooltip = useMemo(() => {
    if (!hover || !hover.object) return null;
    const d = hover.object as PredictionCell;
    return (
      <div className="tooltip" style={{ left: hover.x + 12, top: hover.y + 12 }}>
        <div>H3: <b>{d.h3}</b></div>
        <div>Flood prob: <b>{(d.flood_prob * 100).toFixed(1)}%</b></div>
        {d.min_elev !== null && <div>Min elev: {d.min_elev.toFixed(1)} m</div>}
        {d.slope_deg !== null && <div>Slope: {d.slope_deg.toFixed(2)}&deg;</div>}
        {d.dist_to_water_m !== null && (
          <div>Distance to water: {Math.round(d.dist_to_water_m)} m</div>
        )}
        {d.annual_precip_mm !== null && (
          <div>Annual precip: {Math.round(d.annual_precip_mm)} mm</div>
        )}
        {d.max24h_precip_mm !== null && (
          <div>Historic max 24h: {Math.round(d.max24h_precip_mm)} mm</div>
        )}
        <div>Historical flood: {d.label_real ? "yes" : "no"}</div>
      </div>
    );
  }, [hover]);

  const scenarioMin = scenarios[0] ?? 0;
  const scenarioMax = scenarios[scenarios.length - 1] ?? 200;
  const scenarioStep = scenarios.length > 1
    ? Math.max(1, Math.min(...scenarios.slice(1).map((v, i) => v - scenarios[i])))
    : 10;

  return (
    <div className="app">
      <div className="map">
        <FloodMap
          viewState={view}
          onViewStateChange={setView}
          cells={cells}
          events={events}
          showRealFloods={showFloods}
          theme={theme}
          onHover={onHover}
        />
      </div>

      <aside className="panel">
        <h1>Montreal Flood Risk</h1>
        <p className="subtitle">GeoBrix + Spatial SQL + Spark ML</p>

        <div className="row">
          <label htmlFor="aoi">AOI</label>
          <select id="aoi" value={aoi} onChange={(e) => setAoi(e.target.value)}>
            {aois.map((a) => (
              <option key={a.aoi_name} value={a.aoi_name}>{a.aoi_name}</option>
            ))}
          </select>
        </div>

        <div className="row">
          <label>Basemap</label>
          <div className="segmented">
            {(Object.keys(THEMES) as MapTheme[]).map((k) => (
              <button
                key={k}
                type="button"
                className={theme === k ? "seg-on" : ""}
                onClick={() => setTheme(k)}
              >
                {THEMES[k].label}
              </button>
            ))}
          </div>
        </div>

        <div className="row">
          <label htmlFor="rain">24h rainfall</label>
          <input
            id="rain"
            type="range"
            min={scenarioMin}
            max={scenarioMax}
            step={scenarioStep}
            value={scenarioMm}
            onChange={(e) => setScenarioMm(parseInt(e.target.value, 10))}
          />
          <span className="value">{snappedScenario} mm</span>
        </div>

        <div className="row">
          <label htmlFor="thr">Min flood probability</label>
          <input
            id="thr"
            type="range"
            min={0}
            max={1}
            step={0.05}
            value={threshold}
            onChange={(e) => setThreshold(parseFloat(e.target.value))}
          />
          <span className="value">{threshold.toFixed(2)}</span>
        </div>

        <label className="toggle">
          <input
            type="checkbox"
            checked={showFloods}
            onChange={(e) => setShowFloods(e.target.checked)}
          />
          Overlay 2017/2019 historical floods
        </label>

        <div className="metrics">
          <div className="kpi">
            <span>Scenario</span>
            <span className="v">{snappedScenario} mm / 24h</span>
          </div>
          <div className="kpi">
            <span>Cells shown</span><span className="v">{cells.length.toLocaleString()}</span>
          </div>
          {metrics && (
            <>
              <div className="kpi">
                <span>Mean probability</span>
                <span className="v">{(metrics.mean_prob * 100).toFixed(1)}%</span>
              </div>
              <div className="kpi">
                <span>High-risk rate @ 0.5</span>
                <span className="v">{(metrics.high_risk_rate * 100).toFixed(1)}%</span>
              </div>
              <div className="kpi">
                <span>Real flood cells</span>
                <span className="v">{metrics.real_flood_cells.toLocaleString()}</span>
              </div>
              {metrics.precision_vs_real !== null && (
                <div className="kpi">
                  <span>Precision vs 2017/2019</span>
                  <span className="v">{(metrics.precision_vs_real * 100).toFixed(1)}%</span>
                </div>
              )}
              {metrics.recall_vs_real !== null && (
                <div className="kpi">
                  <span>Recall vs 2017/2019</span>
                  <span className="v">{(metrics.recall_vs_real * 100).toFixed(1)}%</span>
                </div>
              )}
            </>
          )}
        </div>
      </aside>

      <div className="legend">
        <div>Flood probability - {snappedScenario} mm / 24 h scenario</div>
        <div className="bar" />
        <div className="bar-labels"><span>0</span><span>0.5</span><span>1</span></div>
        {showFloods && events.length > 0 && (
          <div className="legend-years">
            <span className="swatch swatch-2017" /> 2017 flood
            <span className="swatch swatch-2019" /> 2019 flood
          </div>
        )}
      </div>

      {loading && <div className="spinner">Loading predictions...</div>}
      {tooltip}
    </div>
  );
}
