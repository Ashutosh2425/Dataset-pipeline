"""Build review dashboard — CDN Leaflet + inlined data."""
import json, csv
from pathlib import Path

BASE     = Path('.')
ANN      = BASE / 'data' / 'annotations'
HR_DIR   = BASE / 'data' / 'human_review'
OUT_HTML = HR_DIR / 'review_dashboard.html'

rows    = list(csv.DictReader(open(HR_DIR / 'route_review.csv')))
sampled = sorted(set(r['aoi_id'] for r in rows))
print(f'AOIs: {len(sampled)}, rows: {len(rows)}')

aoi_map = {a['aoi_id']: a for a in json.load(open(BASE / 'data' / 'aoi_list.json'))}

aoi_data = []
for aoi_id in sampled:
    fp  = ANN / aoi_id / 'evacuation_routes.json'
    rj  = json.load(open(fp)) if fp.exists() else {}
    a   = aoi_map[aoi_id]
    b   = a['bbox']
    eps = []
    for lbl, rlist in sorted(rj.items()):
        eps.append({'epoch': lbl, 'routes': [
            {'route_id': i+1,
             'shelter_name': r.get('shelter_name','?'),
             'length_m': r.get('length_m', 0),
             'eta_min':  r.get('eta_min', 0),
             'rss':      r.get('rss', 0),
             'coords':   r.get('geometry',{}).get('coordinates',[])}
            for i, r in enumerate(rlist)]})
    aoi_data.append({'aoi_id': aoi_id, 'event_id': aoi_id[:6],
                     'bbox': b, 'epochs': eps})

# json.dumps with ensure_ascii so no </script risk in data
data_blob = (json.dumps({'aois': aoi_data, 'rows': rows}, ensure_ascii=True)
             .replace('</', '<\\/'))   # belt-and-suspenders

html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>TDRD Route Review</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Arial,sans-serif;font-size:13px;background:#1a1a2e;color:#eee;display:flex;height:100vh;overflow:hidden}
#sb{width:270px;min-width:270px;background:#16213e;display:flex;flex-direction:column;border-right:1px solid #0f3460}
#sh{padding:10px 12px;background:#0f3460;font-weight:bold;font-size:14px}
#pw{padding:5px 12px;background:#0f3460}
#pt{font-size:11px;color:#aaa}
#pb{height:4px;background:#333;border-radius:2px;margin-top:3px}
#pf{height:4px;background:#4caf50;border-radius:2px;width:0%}
#al{overflow-y:auto;flex:1}
.ai{padding:7px 10px;cursor:pointer;border-bottom:1px solid #1a1a2e;display:flex;align-items:center;gap:7px}
.ai:hover,.ai.active{background:#0f3460}.ai.active{border-left:3px solid #e94560}
.dot{width:9px;height:9px;border-radius:50%;flex-shrink:0}
.done{background:#4caf50}.part{background:#ff9800}.todo{background:#555}
.al{flex:1;font-size:12px}.ac{font-size:10px;color:#888}
#mn{flex:1;display:flex;flex-direction:column;overflow:hidden}
#tb{background:#16213e;padding:7px 12px;display:flex;align-items:center;gap:10px;border-bottom:1px solid #0f3460;flex-wrap:wrap}
#at{font-weight:bold;font-size:14px}
#et{display:flex;gap:4px}
.et{padding:3px 10px;border-radius:4px;cursor:pointer;background:#0f3460;border:1px solid #333;font-size:12px}
.et.active{background:#e94560;border-color:#e94560}
#bmt{padding:3px 9px;background:#0f3460;border:1px solid #555;color:#eee;border-radius:4px;cursor:pointer;font-size:11px}
#exp{margin-left:auto;padding:5px 14px;background:#4caf50;color:#fff;border:none;border-radius:4px;cursor:pointer;font-size:13px;font-weight:bold}
#co{flex:1;display:flex;overflow:hidden}
#map{flex:1}
#pn{width:300px;min-width:300px;background:#16213e;overflow-y:auto;border-left:1px solid #0f3460;padding:10px;display:flex;flex-direction:column;gap:7px}
.rc{background:#1a1a2e;border-radius:6px;padding:9px;border:1px solid #0f3460}
.sn{font-weight:bold;font-size:13px;margin-bottom:3px;word-break:break-word}
.rm{font-size:11px;color:#888;margin-bottom:6px}
.vb{display:flex;gap:5px}
.bp{flex:1;padding:5px;background:#1b5e20;border:1px solid #4caf50;color:#4caf50;border-radius:4px;cursor:pointer;font-weight:bold;font-size:12px}
.bf{flex:1;padding:5px;background:#b71c1c;border:1px solid #ef5350;color:#ef5350;border-radius:4px;cursor:pointer;font-weight:bold;font-size:12px}
.bp.sel{background:#4caf50;color:#fff}.bf.sel{background:#ef5350;color:#fff}
.cb{width:100%;margin-top:5px;background:#0f3460;border:1px solid #333;color:#eee;border-radius:4px;padding:4px;font-size:11px;resize:vertical;min-height:30px}
#nv{display:flex;gap:8px;padding:7px 12px;background:#16213e;border-top:1px solid #0f3460}
.nb{flex:1;padding:5px;background:#0f3460;border:1px solid #333;color:#eee;border-radius:4px;cursor:pointer;font-size:12px}
</style>
</head>
<body>
<div id="sb">
  <div id="sh">TDRD Route Review &mdash; 60 AOIs</div>
  <div id="pw"><div id="pt">Loading...</div><div id="pb"><div id="pf"></div></div></div>
  <div id="al"></div>
</div>
<div id="mn">
  <div id="tb">
    <span id="at">Select an AOI</span>
    <div id="et"></div>
    <button id="bmt" onclick="toggleBM()">Satellite</button>
    <button id="exp" onclick="exportCSV()">Export CSV</button>
  </div>
  <div id="co"><div id="map"></div>
    <div id="pn"><p style="color:#888;font-size:12px">Select an AOI to begin.</p></div>
  </div>
  <div id="nv">
    <button class="nb" onclick="nav(-1)">&#8592; Prev</button>
    <button class="nb" onclick="nav(1)">Next &#8594;</button>
  </div>
</div>
<script>
var _D=JSON.parse('DATA_PLACEHOLDER');
var AOI_DATA=_D.aois, ORIG=_D.rows;
var V={}, curA=0, curE=0, lrs=[], sat=false;
var C=['#e94560','#4fc3f7','#81c784','#ffb74d','#ce93d8'];
ORIG.forEach(function(r){
  if(!V[r.aoi_id])V[r.aoi_id]={};
  if(!V[r.aoi_id][r.epoch])V[r.aoi_id][r.epoch]={};
  V[r.aoi_id][r.epoch][r.route_id]={verdict:r.verdict||'',comment:r.comment||''};
});
var map=L.map('map').setView([20,0],2);
var st=L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{attribution:'OSM',maxZoom:19});
var sa=L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',{attribution:'Esri',maxZoom:19});
st.addTo(map);
function toggleBM(){if(sat){map.removeLayer(sa);st.addTo(map);document.getElementById('bmt').textContent='Satellite';}else{map.removeLayer(st);sa.addTo(map);document.getElementById('bmt').textContent='Street';}sat=!sat;}
function cntT(id){return ORIG.filter(function(r){return r.aoi_id===id;}).length;}
function cntD(id){var v=V[id]||{},n=0;Object.keys(v).forEach(function(e){Object.keys(v[e]).forEach(function(r){if(v[e][r].verdict)n++;});});return n;}
function buildSB(){
  var el=document.getElementById('al');el.innerHTML='';
  AOI_DATA.forEach(function(a,i){
    var d=cntD(a.aoi_id),t=cntT(a.aoi_id);
    var sc=d===0?'todo':(d===t?'done':'part');
    var div=document.createElement('div');
    div.className='ai'+(i===curA?' active':'');
    div.innerHTML='<div class="dot '+sc+'"></div><span class="al">'+a.aoi_id+'</span><span class="ac">'+d+'/'+t+'</span>';
    div.onclick=(function(ii){return function(){curA=ii;curE=0;render();};})(i);
    el.appendChild(div);
  });
}
function render(){
  var a=AOI_DATA[curA];
  document.getElementById('at').textContent=a.aoi_id+' ('+a.event_id+')';
  var te=document.getElementById('et');te.innerHTML='';
  a.epochs.forEach(function(ep,i){
    var t=document.createElement('div');t.className='et'+(i===curE?' active':'');
    t.textContent=ep.epoch;t.onclick=(function(ii){return function(){curE=ii;renderEp();};})(i);
    te.appendChild(t);
  });
  document.querySelectorAll('.ai').forEach(function(el,i){el.classList.toggle('active',i===curA);});
  renderEp();prog();buildSB();
}
function renderEp(){
  var a=AOI_DATA[curA];
  if(!a.epochs.length){renderPn([],null,null);return;}
  var ep=a.epochs[curE];
  document.querySelectorAll('.et').forEach(function(el,i){el.classList.toggle('active',i===curE);});
  lrs.forEach(function(l){map.removeLayer(l);});lrs=[];
  if(a.bbox){var b=a.bbox;lrs.push(L.rectangle([[b[1],b[0]],[b[3],b[2]]],{color:'#fff',weight:1,fill:false,dashArray:'4'}).addTo(map));}
  ep.routes.forEach(function(r,i){
    if(!r.coords||r.coords.length<2)return;
    var ll=r.coords.map(function(c){return[c[1],c[0]];});
    var col=C[i%C.length];
    lrs.push(L.polyline(ll,{color:col,weight:4,opacity:.85}).bindPopup('<b>Route '+r.route_id+'</b><br>'+r.shelter_name+'<br>'+r.length_m+'m / '+r.eta_min+'min').addTo(map));
    lrs.push(L.circleMarker(ll[0],{radius:5,color:'#fff',fillColor:'#333',fillOpacity:1,weight:2}).addTo(map));
    lrs.push(L.circleMarker(ll[ll.length-1],{radius:6,color:'#fff',fillColor:col,fillOpacity:1,weight:2}).addTo(map));
  });
  var ac=[];ep.routes.forEach(function(r){(r.coords||[]).forEach(function(c){ac.push([c[1],c[0]]);});});
  if(ac.length)map.fitBounds(ac,{padding:[30,30]});
  else if(a.bbox){var b=a.bbox;map.fitBounds([[b[1],b[0]],[b[3],b[2]]]);}
  renderPn(ep.routes,a.aoi_id,ep.epoch);
}
function renderPn(routes,aid,epoch){
  var pn=document.getElementById('pn');pn.innerHTML='';
  if(!routes||!routes.length){pn.innerHTML='<p style="color:#888;font-size:12px">No routes.</p>';return;}
  routes.forEach(function(r,i){
    var vm=V[aid]&&V[aid][epoch]?V[aid][epoch]:{};
    var v=vm[String(r.route_id)]||{verdict:'',comment:''};
    var key=aid+'|'+epoch+'|'+r.route_id;
    var c=document.createElement('div');c.className='rc';c.style.borderLeft='3px solid '+C[i%C.length];
    c.innerHTML='<div class="sn">Route '+r.route_id+': '+r.shelter_name+'</div>'+
      '<div class="rm">'+r.length_m+'m &bull; '+r.eta_min+'min &bull; RSS='+r.rss+'</div>'+
      '<div class="vb">'+
        '<button class="bp'+(v.verdict==='PASS'?' sel':'')+'" data-key="'+key+'" data-v="PASS" onclick="setV(this)">PASS</button>'+
        '<button class="bf'+(v.verdict==='FAIL'?' sel':'')+'" data-key="'+key+'" data-v="FAIL" onclick="setV(this)">FAIL</button>'+
      '</div>'+
      '<textarea class="cb" data-key="'+key+'" placeholder="Comment..." onchange="setC(this)">'+v.comment+'</textarea>';
    pn.appendChild(c);
  });
}
function setV(btn){
  var parts=btn.getAttribute('data-key').split('|');
  var aid=parts[0],epoch=parts[1],rid=parts[2],verdict=btn.getAttribute('data-v');
  if(!V[aid])V[aid]={};if(!V[aid][epoch])V[aid][epoch]={};
  if(!V[aid][epoch][rid])V[aid][epoch][rid]={verdict:'',comment:''};
  V[aid][epoch][rid].verdict=verdict;
  var card=btn.closest('.rc');
  card.querySelectorAll('.bp,.bf').forEach(function(b){b.classList.remove('sel');});
  btn.classList.add('sel');
  prog();buildSB();
}
function setC(ta){
  var parts=ta.getAttribute('data-key').split('|');
  var aid=parts[0],epoch=parts[1],rid=parts[2];
  if(!V[aid])V[aid]={};if(!V[aid][epoch])V[aid][epoch]={};
  if(!V[aid][epoch][rid])V[aid][epoch][rid]={verdict:'',comment:''};
  V[aid][epoch][rid].comment=ta.value;
}
function prog(){
  var done=0,tot=ORIG.length;
  ORIG.forEach(function(r){var v=V[r.aoi_id]&&V[r.aoi_id][r.epoch]&&V[r.aoi_id][r.epoch][String(r.route_id)];if(v&&v.verdict)done++;});
  document.getElementById('pt').textContent=done+' / '+tot+' verdicts';
  document.getElementById('pf').style.width=(done/tot*100)+'%';
}
function nav(d){curA=Math.max(0,Math.min(AOI_DATA.length-1,curA+d));curE=0;render();document.querySelectorAll('.ai')[curA].scrollIntoView({block:'nearest'});}
function exportCSV(){
  var lines=['aoi_id,epoch,route_id,verdict,comment'];
  ORIG.forEach(function(r){
    var v=V[r.aoi_id]&&V[r.aoi_id][r.epoch]&&V[r.aoi_id][r.epoch][String(r.route_id)];
    var vd=v&&v.verdict?v.verdict:'', cm=v&&v.comment?(v.comment+'').replace(/"/g,'""'):'';
    lines.push(r.aoi_id+','+r.epoch+','+r.route_id+','+vd+',"'+cm+'"');
  });
  var a=document.createElement('a');a.href=URL.createObjectURL(new Blob([lines.join('\\n')],{type:'text/csv'}));
  a.download='route_review_filled.csv';a.click();
}
buildSB();render();prog();
</script>
</body>
</html>"""

html = html.replace('DATA_PLACEHOLDER', data_blob)
OUT_HTML.write_text(html, encoding='utf-8')
print(f'Done -> {OUT_HTML.stat().st_size//1024} KB')
