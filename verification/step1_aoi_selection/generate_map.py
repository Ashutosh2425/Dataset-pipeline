"""
Generates bbox_verification_map.html — an interactive map for manual AOI verification.
Run: python generate_map.py
Then open bbox_verification_map.html in any browser.
"""

import json
from pathlib import Path

AOI_PATH = Path(__file__).parent.parent.parent / "data" / "aoi_list.json"
OUT_PATH = Path(__file__).parent / "bbox_verification_map.html"

with open(AOI_PATH) as f:
    aois = json.load(f)

EVENT_META = {
    "EVT001": ("Hurricane Harvey",   "flood",      "USA - Houston TX",        "#e74c3c"),
    "EVT002": ("Kerala Floods",      "flood",      "India - Kerala",          "#e67e22"),
    "EVT003": ("Camp Fire",          "fire",       "USA - Paradise CA",       "#f39c12"),
    "EVT004": ("Cyclone Fani",       "wind+flood", "India - Odisha",          "#27ae60"),
    "EVT005": ("Midwest Floods",     "flood",      "USA - Midwest",           "#2980b9"),
    "EVT006": ("Beirut Explosion",   "structural", "Lebanon - Beirut",        "#8e44ad"),
    "EVT007": ("Cyclone Amphan",     "wind+flood", "India/Bangladesh",        "#16a085"),
    "EVT008": ("Natchez Tornadoes",  "wind",       "USA - Natchez MS",        "#d35400"),
    "EVT009": ("Pakistan Floods",    "flood",      "Pakistan - Sindh",        "#c0392b"),
    "EVT010": ("Turkey Earthquake",  "structural", "Turkey - Kahramanmaras",  "#7f8c8d"),
    "EVT011": ("Hawaii Wildfires",   "fire",       "USA - Maui HI",           "#e91e63"),
    "EVT012": ("Libya Derna Floods", "flood",      "Libya - Derna",           "#1abc9c"),
}

features = []
for a in aois:
    w, s, e, n = a["bbox"]
    eid = a["event_id"]
    name, etype, loc, color = EVENT_META[eid]
    lat_c = round((s + n) / 2, 4)
    lon_c = round((w + e) / 2, 4)
    d_start = a["date_range"][0]
    d_end   = a["date_range"][1]

    eob_link = (
        "https://apps.sentinel-hub.com/eo-browser/"
        f"?zoom=13&lat={lat_c}&lng={lon_c}"
        f"&fromTime={d_start}T00%3A00%3A00.000Z"
        f"&toTime={d_end}T23%3A59%3A59.000Z"
        "&layerId=1_TRUE_COLOR&datasource=Sentinel-2%20L2A"
    )
    gm_link = f"https://maps.google.com/?q={lat_c},{lon_c}&z=13"

    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[w,s],[e,s],[e,n],[w,n],[w,s]]]
        },
        "properties": {
            "aoi_id":     a["aoi_id"],
            "event_id":   eid,
            "event_name": name,
            "event_type": etype,
            "location":   loc,
            "date_start": d_start,
            "date_end":   d_end,
            "n_scenes":   a["n_sentinel_scenes"],
            "n_roads":    a["n_osm_roads"],
            "has_xbd":    a["has_xbd_overlap"],
            "color":      color,
            "eob_link":   eob_link,
            "gm_link":    gm_link,
        }
    })

geojson_str = json.dumps({"type": "FeatureCollection", "features": features})

legend_rows = ""
for eid, (name, etype, loc, color) in EVENT_META.items():
    count = sum(1 for a in aois if a["event_id"] == eid)
    legend_rows += (
        f'<tr><td><span class="dot" style="background:{color}"></span></td>'
        f'<td><b>{eid}</b></td><td>{name}</td><td>{loc}</td><td>{count}</td></tr>\n'
    )

html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>TDRD AOI Verification Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family: Arial, sans-serif; }
  #map { width:100%; height:100vh; }
  #panel {
    position:absolute; top:10px; right:10px; z-index:1000;
    background:white; padding:12px; border-radius:8px;
    box-shadow:0 2px 8px rgba(0,0,0,0.3); max-width:360px; font-size:13px;
  }
  #panel h3 { margin-bottom:6px; font-size:15px; }
  #panel table { border-collapse:collapse; width:100%; font-size:12px; }
  #panel td { padding:3px 5px; border-bottom:1px solid #eee; }
  .dot { display:inline-block; width:12px; height:12px; border-radius:2px; }
  #info {
    position:absolute; bottom:10px; left:10px; z-index:1000;
    background:white; padding:12px 16px; border-radius:8px;
    box-shadow:0 2px 8px rgba(0,0,0,0.3); max-width:420px;
    font-size:13px; display:none;
  }
  #info h4 { margin-bottom:8px; color:#222; font-size:14px; }
  #info a { color:#2980b9; text-decoration:none; }
  #info a:hover { text-decoration:underline; }
  #info table { width:100%; font-size:12px; border-collapse:collapse; }
  #info td { padding:3px 6px; border-bottom:1px solid #f0f0f0; }
  .toggle-btn {
    position:absolute; top:10px; left:60px; z-index:1000;
    background:white; padding:7px 12px; border-radius:6px;
    box-shadow:0 2px 6px rgba(0,0,0,0.3); cursor:pointer;
    font-size:13px; border:1px solid #ccc;
  }
  .close-btn {
    float:right; cursor:pointer; color:#999; font-size:16px; margin-top:-4px;
  }
  .warn { font-size:11px; color:#e74c3c; margin-top:8px; font-weight:bold; }
</style>
</head>
<body>
<div id="map"></div>
<button class="toggle-btn" onclick="toggleBase()">Toggle Street / Satellite</button>

<div id="panel">
  <h3>TDRD AOI Verification Map</h3>
  <p style="font-size:11px;color:#666;margin-bottom:8px">
    Click any coloured box to inspect it.<br>
    Use the <b>Sentinel Hub</b> link to view real satellite imagery.
    Total: <b>600 AOIs</b> | <b>12 events</b>
  </p>
  <table>
    <tr style="font-weight:bold;background:#f5f5f5">
      <td></td><td>ID</td><td>Event</td><td>Location</td><td>#</td>
    </tr>
    LEGEND_ROWS
  </table>
</div>

<div id="info">
  <span class="close-btn" onclick="document.getElementById('info').style.display='none'">&#x2715;</span>
  <h4 id="info-title"></h4>
  <div id="info-body"></div>
</div>

<script>
var geojsonData = GEOJSON_DATA;

var satellite = L.tileLayer(
  'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
  {attribution: 'Tiles &copy; Esri', maxZoom: 19}
);
var streets = L.tileLayer(
  'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
  {attribution: '&copy; OpenStreetMap contributors', maxZoom: 19}
);

var map = L.map('map', {layers: [satellite]});
map.setView([20, 20], 3);
var usingSatellite = true;

function toggleBase() {
  if (usingSatellite) {
    map.removeLayer(satellite); map.addLayer(streets); usingSatellite = false;
  } else {
    map.removeLayer(streets); map.addLayer(satellite); usingSatellite = true;
  }
}

function onEachFeature(feature, layer) {
  var p = feature.properties;
  layer.on('click', function () {
    document.getElementById('info').style.display = 'block';
    document.getElementById('info-title').innerText = p.aoi_id + '  —  ' + p.event_name;
    document.getElementById('info-body').innerHTML =
      '<table>' +
      '<tr><td><b>Event ID</b></td><td>' + p.event_id + '</td></tr>' +
      '<tr><td><b>Event name</b></td><td>' + p.event_name + '</td></tr>' +
      '<tr><td><b>Type</b></td><td>' + p.event_type + '</td></tr>' +
      '<tr><td><b>Location</b></td><td>' + p.location + '</td></tr>' +
      '<tr><td><b>Date range</b></td><td>' + p.date_start + ' &rarr; ' + p.date_end + '</td></tr>' +
      '<tr><td><b>Sentinel scenes</b></td><td>' + p.n_scenes + '</td></tr>' +
      '<tr><td><b>OSM roads</b></td><td>' + p.n_roads + '</td></tr>' +
      '<tr><td><b>xBD data</b></td><td>' + (p.has_xbd ? '<span style="color:green">YES (direct)</span>' : '<span style="color:#e07b00">No — spectral only</span>') + '</td></tr>' +
      '</table>' +
      '<div style="margin-top:10px">' +
      '<a href="' + p.eob_link + '" target="_blank">&#x1F6F0; Open in Sentinel Hub EO Browser</a>' +
      '&nbsp;&nbsp;|&nbsp;&nbsp;' +
      '<a href="' + p.gm_link + '" target="_blank">&#x1F5FA; Open in Google Maps</a>' +
      '</div>' +
      '<div class="warn">CHECK: Does this box cover the correct disaster area on the satellite image?</div>';
  });
}

L.geoJSON(geojsonData, {
  style: function (feature) {
    return {
      color:       feature.properties.color,
      weight:      1.5,
      fillColor:   feature.properties.color,
      fillOpacity: 0.25,
      opacity:     0.9
    };
  },
  onEachFeature: onEachFeature
}).addTo(map);
</script>
</body>
</html>"""

html = html.replace("LEGEND_ROWS", legend_rows)
html = html.replace("GEOJSON_DATA", geojson_str)

OUT_PATH.write_text(html, encoding="utf-8")
print(f"Written: {OUT_PATH}")
print(f"Size: {OUT_PATH.stat().st_size / 1024:.1f} KB")
print("Open bbox_verification_map.html in your browser to verify.")
