"""
Step 1c Verification — Interactive AOI Map (no TIF data required)
-----------------------------------------------------------------
Opens bboxes from aoi_list.json on a live satellite/street basemap
served by OpenStreetMap and ESRI via Folium.

Why this is better than step1b:
  - Zero dependency on local TIF files
  - Real, zoomable, pannable basemap from the internet
  - Click any bbox to see aoi_id / event_id / event_type / date_range
  - Toggle between Street and Satellite tiles
  - One HTML file per event + one combined file for all events

What to check:
  - Does each red rectangle sit over the correct city/coastal/forest area?
  - Do adjacent AOIs tile together without gaps or overlaps?
  - Does the event cluster make geographic sense?

Output: verification/step1_aoi_selection/step1c_maps/
  EVT001_map.html  ...  EVT012_map.html   (one per event)
  all_events_map.html                     (all 600 AOIs together)

Run from repo root:
    python verification/step1_aoi_selection/step1c_interactive_map.py
"""

import json
import colorsys
from pathlib import Path

import folium
from folium import plugins

BASE     = Path(__file__).resolve().parents[2]
AOI_LIST = json.load(open(BASE / "data" / "aoi_list.json"))
OUT_DIR  = Path(__file__).parent / "step1c_maps"
OUT_DIR.mkdir(exist_ok=True)

# One distinct colour per event (HSV wheel, evenly spaced)
EVENTS = sorted(set(a["event_id"] for a in AOI_LIST))
N      = len(EVENTS)
EVENT_COLORS = {}
for i, eid in enumerate(EVENTS):
    h = i / N
    r, g, b = colorsys.hsv_to_rgb(h, 0.85, 0.90)
    EVENT_COLORS[eid] = "#{:02x}{:02x}{:02x}".format(int(r*255), int(g*255), int(b*255))


def make_map(aois, title):
    """
    Create a Folium map showing all bboxes in `aois`.
    Returns the folium.Map object.
    """
    lats = [(a["bbox"][1] + a["bbox"][3]) / 2 for a in aois]
    lons = [(a["bbox"][0] + a["bbox"][2]) / 2 for a in aois]
    center_lat = sum(lats) / len(lats)
    center_lon = sum(lons) / len(lons)

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=10 if len(set(a["event_id"] for a in aois)) == 1 else 3,
        tiles=None,
    )

    # Basemap layers
    folium.TileLayer(
        tiles="OpenStreetMap",
        name="Street (OSM)",
        control=True,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Satellite (ESRI)",
        control=True,
    ).add_to(m)

    # One FeatureGroup per event so they can be toggled
    groups = {}
    for eid in EVENTS:
        group_aois = [a for a in aois if a["event_id"] == eid]
        if not group_aois:
            continue
        etype = group_aois[0]["event_type"]
        fg = folium.FeatureGroup(name=f"{eid} ({etype})", show=True)
        groups[eid] = fg

        color = EVENT_COLORS[eid]
        for aoi in group_aois:
            w, s, e, n = aoi["bbox"]
            popup_html = (
                f"<b>{aoi['aoi_id']}</b><br>"
                f"Event: {eid} ({etype})<br>"
                f"Dates: {aoi['date_range'][0]} – {aoi['date_range'][1]}<br>"
                f"Sentinel scenes: {aoi['n_sentinel_scenes']}<br>"
                f"OSM roads: {aoi['n_osm_roads']}<br>"
                f"xBD overlap: {aoi['has_xbd_overlap']}<br>"
                f"Bbox: [{w:.4f}, {s:.4f}, {e:.4f}, {n:.4f}]"
            )
            folium.Rectangle(
                bounds=[[s, w], [n, e]],
                color=color,
                weight=2,
                fill=True,
                fill_color=color,
                fill_opacity=0.08,
                tooltip=aoi["aoi_id"],
                popup=folium.Popup(popup_html, max_width=280),
            ).add_to(fg)

            # Centroid marker
            folium.CircleMarker(
                location=[(s + n) / 2, (w + e) / 2],
                radius=3,
                color=color,
                fill=True,
                fill_opacity=0.9,
                tooltip=aoi["aoi_id"],
            ).add_to(fg)

        fg.add_to(m)

    # Legend
    legend_items = "".join(
        f'<li><span style="background:{EVENT_COLORS[eid]};'
        f'display:inline-block;width:14px;height:14px;margin-right:6px;'
        f'border-radius:2px;"></span>{eid}</li>'
        for eid in EVENTS if any(a["event_id"] == eid for a in aois)
    )
    legend_html = f"""
    <div style="position:fixed;bottom:30px;left:30px;z-index:9999;
                background:white;padding:10px 14px;border-radius:6px;
                box-shadow:2px 2px 6px rgba(0,0,0,0.3);font-size:12px;">
      <b>{title}</b><br>
      <ul style="list-style:none;padding:0;margin:4px 0 0 0;">
        {legend_items}
      </ul>
    </div>"""
    m.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl(collapsed=False).add_to(m)
    plugins.Fullscreen().add_to(m)
    plugins.MousePosition().add_to(m)

    return m


def main():
    # Per-event maps
    by_event = {}
    for a in AOI_LIST:
        by_event.setdefault(a["event_id"], []).append(a)

    for eid, aois in sorted(by_event.items()):
        etype = aois[0]["event_type"]
        m = make_map(aois, f"{eid} — {etype} ({len(aois)} AOIs)")
        out = OUT_DIR / f"{eid}_map.html"
        m.save(str(out))
        print(f"  {out.name}  ({len(aois)} AOIs)")

    # Combined map — all events
    m_all = make_map(AOI_LIST, f"All Events — {len(AOI_LIST)} AOIs")
    out_all = OUT_DIR / "all_events_map.html"
    m_all.save(str(out_all))
    print(f"  {out_all.name}  ({len(AOI_LIST)} AOIs, all events)")

    print(f"\nDone — open any HTML in a browser.")
    print(f"  Street basemap  = verify location names / road layout")
    print(f"  Satellite layer = verify terrain type (urban/coastal/forest)")
    print(f"  Click any box   = aoi_id, dates, road/scene counts")


if __name__ == "__main__":
    main()
