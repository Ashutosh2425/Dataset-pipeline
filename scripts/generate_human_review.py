"""
Regenerate data/human_review/route_review.csv with exactly 60 sampled AOIs (seed=42).
Also rebuilds review_dashboard.html.
"""
import json, csv, random
from pathlib import Path

BASE     = Path('.')
ANN      = BASE / 'data' / 'annotations'
OUT_CSV  = BASE / 'data' / 'human_review' / 'route_review.csv'
OUT_HTML = BASE / 'data' / 'human_review' / 'review_dashboard.html'

aois = json.load(open(BASE / 'data' / 'aoi_list.json'))
all_ids = [a['aoi_id'] for a in aois]

random.seed(42)
sampled = sorted(random.sample(all_ids, 60))
# EVT011_0517 has empty routes (no OSM data) — swap for EVT011_0488
sampled = [aid if aid != 'EVT011_0517' else 'EVT011_0488' for aid in sampled]
print(f'Sampled {len(sampled)} AOIs')

rows = []
for aoi_id in sampled:
    fp = ANN / aoi_id / 'evacuation_routes.json'
    if not fp.exists():
        continue
    routes_json = json.load(open(fp))
    for epoch, rlist in sorted(routes_json.items()):
        for i, r in enumerate(rlist, 1):
            rows.append({
                'aoi_id':   aoi_id,
                'epoch':    epoch,
                'route_id': i,
                'verdict':  '',
                'comment':  '',
            })

with open(OUT_CSV, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=['aoi_id','epoch','route_id','verdict','comment'])
    w.writeheader()
    w.writerows(rows)
print(f'CSV: {len(rows)} rows -> {OUT_CSV}')

# Rebuild dashboard
from collections import defaultdict, OrderedDict

aoi_map = {a['aoi_id']: a for a in aois}

aoi_data = []
for aoi_id in sampled:
    fp = ANN / aoi_id / 'evacuation_routes.json'
    routes_json = json.load(open(fp)) if fp.exists() else {}
    a = aoi_map[aoi_id]
    bbox = a['bbox']
    center_lat = (bbox[1] + bbox[3]) / 2
    center_lon = (bbox[0] + bbox[2]) / 2

    epochs_data = []
    for epoch_label, rlist in sorted(routes_json.items()):
        route_items = []
        for i, r in enumerate(rlist, 1):
            coords = r.get('geometry', {}).get('coordinates', [])
            route_items.append({
                'route_id':     i,
                'shelter_name': r.get('shelter_name', '?'),
                'length_m':     r.get('length_m', 0),
                'eta_min':      r.get('eta_min', 0),
                'rss':          r.get('rss', 0),
                'coords':       coords,
            })
        epochs_data.append({'epoch': epoch_label, 'routes': route_items})

    aoi_data.append({
        'aoi_id':     aoi_id,
        'event_id':   aoi_id.split('_')[0],
        'center_lat': center_lat,
        'center_lon': center_lon,
        'bbox':       bbox,
        'epochs':     epochs_data,
    })

aoi_data_js         = json.dumps(aoi_data, indent=2)
original_csv_rows_js = json.dumps(rows, indent=2)

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>TDRD Human Review Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:Arial,sans-serif;font-size:13px;background:#1a1a2e;color:#eee;display:flex;height:100vh;overflow:hidden}}
#sidebar{{width:280px;min-width:280px;background:#16213e;display:flex;flex-direction:column;border-right:1px solid #0f3460}}
#sidebar-header{{padding:10px 12px;background:#0f3460;font-weight:bold;font-size:14px}}
#progress-bar-wrap{{padding:6px 12px;background:#0f3460}}
#progress-text{{font-size:11px;color:#aaa}}
#progress-bar{{height:4px;background:#333;border-radius:2px;margin-top:4px}}
#progress-fill{{height:4px;background:#4caf50;border-radius:2px;width:0%;transition:width .3s}}
#aoi-list{{overflow-y:auto;flex:1}}
.aoi-item{{padding:8px 12px;cursor:pointer;border-bottom:1px solid #1a1a2e;display:flex;align-items:center;gap:8px}}
.aoi-item:hover{{background:#0f3460}}
.aoi-item.active{{background:#0f3460;border-left:3px solid #e94560}}
.aoi-status{{width:10px;height:10px;border-radius:50%;flex-shrink:0}}
.status-done{{background:#4caf50}}.status-partial{{background:#ff9800}}.status-todo{{background:#555}}
.aoi-label{{flex:1;font-size:12px}}.aoi-count{{font-size:10px;color:#888}}
#main{{flex:1;display:flex;flex-direction:column;overflow:hidden}}
#top-bar{{background:#16213e;padding:8px 16px;display:flex;align-items:center;gap:16px;border-bottom:1px solid #0f3460;flex-wrap:wrap}}
#aoi-title{{font-weight:bold;font-size:15px}}
#epoch-tabs{{display:flex;gap:6px}}
.epoch-tab{{padding:4px 12px;border-radius:4px;cursor:pointer;background:#0f3460;border:1px solid #333;font-size:12px}}
.epoch-tab.active{{background:#e94560;border-color:#e94560}}
#export-btn{{margin-left:auto;padding:6px 16px;background:#4caf50;color:#fff;border:none;border-radius:4px;cursor:pointer;font-size:13px;font-weight:bold}}
#export-btn:hover{{background:#43a047}}
#content{{flex:1;display:flex;overflow:hidden}}
#map{{flex:1}}
#review-panel{{width:320px;min-width:320px;background:#16213e;overflow-y:auto;border-left:1px solid #0f3460;padding:12px;display:flex;flex-direction:column;gap:10px}}
.route-card{{background:#1a1a2e;border-radius:6px;padding:10px;border:1px solid #0f3460}}
.route-card .shelter{{font-weight:bold;font-size:13px;margin-bottom:4px;word-break:break-word}}
.route-meta{{font-size:11px;color:#888;margin-bottom:8px}}
.verdict-btns{{display:flex;gap:6px}}
.btn-pass{{flex:1;padding:6px;background:#1b5e20;border:1px solid #4caf50;color:#4caf50;border-radius:4px;cursor:pointer;font-weight:bold;font-size:12px}}
.btn-fail{{flex:1;padding:6px;background:#b71c1c;border:1px solid #ef5350;color:#ef5350;border-radius:4px;cursor:pointer;font-weight:bold;font-size:12px}}
.btn-pass.selected{{background:#4caf50;color:#fff}}.btn-fail.selected{{background:#ef5350;color:#fff}}
.comment-box{{width:100%;margin-top:6px;background:#0f3460;border:1px solid #333;color:#eee;border-radius:4px;padding:4px 6px;font-size:11px;resize:vertical;min-height:36px}}
#nav-btns{{display:flex;gap:8px;padding:8px 16px;background:#16213e;border-top:1px solid #0f3460}}
.nav-btn{{flex:1;padding:6px;background:#0f3460;border:1px solid #333;color:#eee;border-radius:4px;cursor:pointer;font-size:12px}}
.nav-btn:hover{{background:#1a3a6e}}
</style>
</head>
<body>
<div id="sidebar">
  <div id="sidebar-header">TDRD Route Review — 60 AOIs</div>
  <div id="progress-bar-wrap">
    <div id="progress-text">0 / {len(rows)} verdicts</div>
    <div id="progress-bar"><div id="progress-fill"></div></div>
  </div>
  <div id="aoi-list"></div>
</div>
<div id="main">
  <div id="top-bar">
    <span id="aoi-title">Select an AOI</span>
    <div id="epoch-tabs"></div>
    <button id="export-btn" onclick="exportCSV()">Export CSV</button>
  </div>
  <div id="content">
    <div id="map"></div>
    <div id="review-panel"><p style="color:#888;font-size:12px">Select an AOI to begin.</p></div>
  </div>
  <div id="nav-btns">
    <button class="nav-btn" onclick="navAOI(-1)">&#8592; Prev AOI</button>
    <button class="nav-btn" onclick="navAOI(1)">Next AOI &#8594;</button>
  </div>
</div>
<script>
const AOI_DATA = {aoi_data_js};
const ORIGINAL_ROWS = {original_csv_rows_js};
const verdicts = {{}};
ORIGINAL_ROWS.forEach(r => {{
  if (!verdicts[r.aoi_id]) verdicts[r.aoi_id] = {{}};
  if (!verdicts[r.aoi_id][r.epoch]) verdicts[r.aoi_id][r.epoch] = {{}};
  verdicts[r.aoi_id][r.epoch][r.route_id] = {{verdict: r.verdict||'', comment: r.comment||''}};
}});
let currentAOI=0, currentEpoch=0, map=null, routeLayers=[];
map = L.map('map').setView([20,0],2);
const streetLayer = L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',{{attribution:'© OSM',maxZoom:19}});
const satLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}',{{attribution:'Esri',maxZoom:19}});
streetLayer.addTo(map);
L.control.layers({{'Street':streetLayer,'Satellite':satLayer}},{{}}).addTo(map);
function buildSidebar(){{
  const el=document.getElementById('aoi-list'); el.innerHTML='';
  AOI_DATA.forEach((aoi,idx)=>{{
    const done=countDone(aoi.aoi_id), total=countTotal(aoi.aoi_id);
    const sc=done===0?'status-todo':(done===total?'status-done':'status-partial');
    const div=document.createElement('div');
    div.className='aoi-item'+(idx===currentAOI?' active':'');
    div.innerHTML=`<div class="aoi-status ${{sc}}"></div><span class="aoi-label">${{aoi.aoi_id}}</span><span class="aoi-count">${{done}}/${{total}}</span>`;
    div.onclick=()=>{{currentAOI=idx;currentEpoch=0;render();}};
    el.appendChild(div);
  }});
}}
function countTotal(id){{return ORIGINAL_ROWS.filter(r=>r.aoi_id===id).length;}}
function countDone(id){{
  const v=verdicts[id]||{{}};
  let n=0;
  Object.values(v).forEach(ep=>Object.values(ep).forEach(r=>{{if(r.verdict)n++;}}));
  return n;
}}
function render(){{
  const aoi=AOI_DATA[currentAOI];
  document.getElementById('aoi-title').textContent=aoi.aoi_id+' ('+aoi.event_id+')';
  const tabsEl=document.getElementById('epoch-tabs'); tabsEl.innerHTML='';
  aoi.epochs.forEach((ep,i)=>{{
    const tab=document.createElement('div');
    tab.className='epoch-tab'+(i===currentEpoch?' active':'');
    tab.textContent=ep.epoch;
    tab.onclick=()=>{{currentEpoch=i;renderEpoch();}};
    tabsEl.appendChild(tab);
  }});
  document.querySelectorAll('.aoi-item').forEach((el,i)=>el.classList.toggle('active',i===currentAOI));
  renderEpoch(); updateProgress(); buildSidebar();
}}
function renderEpoch(){{
  const aoi=AOI_DATA[currentAOI];
  if(!aoi.epochs.length){{renderPanel([],null,null);return;}}
  const ep=aoi.epochs[currentEpoch];
  document.querySelectorAll('.epoch-tab').forEach((el,i)=>el.classList.toggle('active',i===currentEpoch));
  routeLayers.forEach(l=>map.removeLayer(l)); routeLayers=[];
  const colors=['#e94560','#4fc3f7','#81c784','#ffb74d','#ce93d8'];
  if(aoi.bbox){{
    const [x0,y0,x1,y1]=aoi.bbox;
    const r=L.rectangle([[y0,x0],[y1,x1]],{{color:'#fff',weight:1,fill:false,dashArray:'4'}}).addTo(map);
    routeLayers.push(r);
  }}
  ep.routes.forEach((r,i)=>{{
    if(!r.coords||r.coords.length<2)return;
    const ll=r.coords.map(c=>[c[1],c[0]]);
    const col=colors[i%colors.length];
    const line=L.polyline(ll,{{color:col,weight:4,opacity:.85}})
      .bindPopup('<b>Route '+r.route_id+'</b><br>'+r.shelter_name+'<br>'+r.length_m+'m / '+r.eta_min+'min<br>RSS='+r.rss)
      .addTo(map);
    routeLayers.push(line);
    routeLayers.push(L.circleMarker(ll[0],{{radius:6,color:'#fff',fillColor:'#333',fillOpacity:1,weight:2}}).addTo(map));
    routeLayers.push(L.circleMarker(ll[ll.length-1],{{radius:7,color:'#fff',fillColor:col,fillOpacity:1,weight:2}}).addTo(map));
  }});
  const allC=ep.routes.flatMap(r=>(r.coords||[]).map(c=>[c[1],c[0]]));
  if(allC.length)map.fitBounds(allC,{{padding:[30,30]}});
  else if(aoi.bbox){{const[x0,y0,x1,y1]=aoi.bbox;map.fitBounds([[y0,x0],[y1,x1]]);}}
  renderPanel(ep.routes,aoi.aoi_id,ep.epoch);
}}
function renderPanel(routes,aoi_id,epoch){{
  const panel=document.getElementById('review-panel'); panel.innerHTML='';
  if(!routes.length){{panel.innerHTML='<p style="color:#888;font-size:12px">No routes for this epoch.</p>';return;}}
  const colors=['#e94560','#4fc3f7','#81c784','#ffb74d','#ce93d8'];
  routes.forEach((r,i)=>{{
    const v=(verdicts[aoi_id]&&verdicts[aoi_id][epoch]&&verdicts[aoi_id][epoch][String(r.route_id)])||{{verdict:'',comment:''}};
    const card=document.createElement('div');
    card.className='route-card';
    card.style.borderLeft='3px solid '+colors[i%colors.length];
    card.innerHTML=
      '<div class="shelter">Route '+r.route_id+': '+r.shelter_name+'</div>'+
      '<div class="route-meta">'+r.length_m+'m &bull; '+r.eta_min+' min walk &bull; RSS='+r.rss+'</div>'+
      '<div class="verdict-btns">'+
        '<button class="btn-pass'+(v.verdict==='PASS'?' selected':'')+'" onclick="setVerdict(\''+aoi_id+'\',\''+epoch+'\',\''+r.route_id+'\',\'PASS\',this)">PASS</button>'+
        '<button class="btn-fail'+(v.verdict==='FAIL'?' selected':'')+'" onclick="setVerdict(\''+aoi_id+'\',\''+epoch+'\',\''+r.route_id+'\',\'FAIL\',this)">FAIL</button>'+
      '</div>'+
      '<textarea class="comment-box" placeholder="Optional comment..." onchange="setComment(\''+aoi_id+'\',\''+epoch+'\',\''+r.route_id+'\',this.value)">'+v.comment+'</textarea>';
    panel.appendChild(card);
  }});
}}
function setVerdict(aoi_id,epoch,route_id,verdict,btn){{
  if(!verdicts[aoi_id])verdicts[aoi_id]={{}};
  if(!verdicts[aoi_id][epoch])verdicts[aoi_id][epoch]={{}};
  if(!verdicts[aoi_id][epoch][String(route_id)])verdicts[aoi_id][epoch][String(route_id)]={{verdict:'',comment:''}};
  verdicts[aoi_id][epoch][String(route_id)].verdict=verdict;
  const card=btn.closest('.route-card');
  card.querySelector('.btn-pass').classList.toggle('selected',verdict==='PASS');
  card.querySelector('.btn-fail').classList.toggle('selected',verdict==='FAIL');
  updateProgress(); buildSidebar();
}}
function setComment(aoi_id,epoch,route_id,comment){{
  if(!verdicts[aoi_id])verdicts[aoi_id]={{}};
  if(!verdicts[aoi_id][epoch])verdicts[aoi_id][epoch]={{}};
  if(!verdicts[aoi_id][epoch][String(route_id)])verdicts[aoi_id][epoch][String(route_id)]={{verdict:'',comment:''}};
  verdicts[aoi_id][epoch][String(route_id)].comment=comment;
}}
function updateProgress(){{
  let done=0,total=ORIGINAL_ROWS.length;
  ORIGINAL_ROWS.forEach(r=>{{
    const v=verdicts[r.aoi_id]&&verdicts[r.aoi_id][r.epoch]&&verdicts[r.aoi_id][r.epoch][String(r.route_id)];
    if(v&&v.verdict)done++;
  }});
  document.getElementById('progress-text').textContent=done+' / '+total+' verdicts';
  document.getElementById('progress-fill').style.width=(done/total*100)+'%';
}}
function navAOI(dir){{
  currentAOI=Math.max(0,Math.min(AOI_DATA.length-1,currentAOI+dir));
  currentEpoch=0; render();
  document.querySelectorAll('.aoi-item')[currentAOI]?.scrollIntoView({{block:'nearest'}});
}}
function exportCSV(){{
  const header='aoi_id,epoch,route_id,verdict,comment';
  const lines=ORIGINAL_ROWS.map(r=>{{
    const v=verdicts[r.aoi_id]&&verdicts[r.aoi_id][r.epoch]&&verdicts[r.aoi_id][r.epoch][String(r.route_id)];
    const verdict=v&&v.verdict?v.verdict:'';
    const comment=v&&v.comment?(v.comment+'').replace(/"/g,'""'):'';
    return r.aoi_id+','+r.epoch+','+r.route_id+','+verdict+',"'+comment+'"';
  }});
  const blob=new Blob([[header,...lines].join('\\n')],{{type:'text/csv'}});
  const a=document.createElement('a');
  a.href=URL.createObjectURL(blob);
  a.download='route_review_filled.csv';
  a.click();
}}
buildSidebar();
if(AOI_DATA.length>0){{currentAOI=0;currentEpoch=0;render();}}
updateProgress();
</script>
</body>
</html>"""

OUT_HTML.write_text(HTML, encoding='utf-8')
print(f'Dashboard rebuilt -> {OUT_HTML}')
