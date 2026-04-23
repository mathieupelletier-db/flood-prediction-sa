import { useMemo } from "react";
import DeckGL from "@deck.gl/react";
import { H3HexagonLayer } from "@deck.gl/geo-layers";
import { GeoJsonLayer } from "@deck.gl/layers";
import { Map as MapLibre } from "react-map-gl/maplibre";
import type { PickingInfo } from "@deck.gl/core";
import type { PredictionCell, FloodEvent } from "./api";
import { viridis } from "./colors";

export type ViewState = {
  longitude: number;
  latitude: number;
  zoom: number;
  pitch?: number;
  bearing?: number;
};

export type MapTheme = "dark" | "light" | "voyager";

export const THEMES: Record<MapTheme, { label: string; url: string }> = {
  dark:    { label: "Dark",    url: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json" },
  light:   { label: "Light",   url: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json" },
  voyager: { label: "Voyager", url: "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json" },
};

type Props = {
  viewState: ViewState;
  onViewStateChange: (v: ViewState) => void;
  cells: PredictionCell[];
  events: FloodEvent[];
  showRealFloods: boolean;
  theme: MapTheme;
  onHover?: (info: PickingInfo) => void;
};

export function FloodMap({
  viewState,
  onViewStateChange,
  cells,
  events,
  showRealFloods,
  theme,
  onHover,
}: Props) {
  const hexLayer = useMemo(
    () =>
      new H3HexagonLayer<PredictionCell>({
        id: "flood-hex",
        data: cells,
        pickable: true,
        extruded: true,
        elevationScale: 20,
        getHexagon: (d) => d.h3,
        getFillColor: (d) => viridis(d.flood_prob),
        getElevation: (d) => d.flood_prob * 40,
        wireframe: false,
        opacity: 0.75,
        updateTriggers: {
          getFillColor: cells.length,
          getElevation: cells.length,
        },
      }),
    [cells],
  );

  // Split the overlay by year so overlapping extents render as a two-colour
  // blend (red + amber) instead of z-fighting into horizontal stripes. Also
  // disable depth-testing on the overlay layers - they are coplanar and
  // depthTest: false is the canonical deck.gl fix for that artefact.
  const floodLayers = useMemo(() => {
    const byYear: Record<string, typeof events> = {};
    if (showRealFloods) {
      for (const ev of events) {
        const y = ev.year || "unknown";
        (byYear[y] ??= []).push(ev);
      }
    }

    const palette: Record<string, { fill: [number, number, number, number]; line: [number, number, number, number] }> = {
      "2017":   { fill: [229,  72,  77, 100], line: [229,  72,  77, 230] },
      "2019":   { fill: [255, 153,  51, 100], line: [255, 153,  51, 230] },
      "unknown":{ fill: [200, 200, 200, 100], line: [200, 200, 200, 230] },
    };

    return Object.entries(byYear).map(([year, list]) =>
      new GeoJsonLayer({
        id: `real-floods-${year}`,
        data: {
          type: "FeatureCollection",
          features: list.map((e) => ({
            type: "Feature",
            properties: { year: e.year },
            geometry: e.geometry,
          })),
        },
        stroked: true,
        filled: true,
        getFillColor: (palette[year] ?? palette.unknown).fill,
        getLineColor: (palette[year] ?? palette.unknown).line,
        lineWidthMinPixels: 1.5,
        pickable: false,
        parameters: { depthTest: false },
      }),
    );
  }, [events, showRealFloods]);

  return (
    <DeckGL
      viewState={viewState}
      controller
      layers={[hexLayer, ...floodLayers]}
      onViewStateChange={(e) => onViewStateChange(e.viewState as ViewState)}
      onHover={onHover}
    >
      <MapLibre mapStyle={THEMES[theme].url} reuseMaps />
    </DeckGL>
  );
}
