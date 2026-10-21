function esc(s){return String(s??"").replace(/[&<>"']/g,x=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[x]))}
function fmt(s){if(s===null||s===undefined)return "lernt…";let m=Math.max(0,Math.round(s/60));if(m>=60)return Math.floor(m/60)+"h "+String(m%60).padStart(2,"0")+"m";return m+" min"}
function clockFromNow(s){if(s===null||s===undefined)return "";return new Date(Date.now()+s*1000).toLocaleTimeString("de-DE",{hour:"2-digit",minute:"2-digit"})}
function forecastText(f){if(!f)return '<span class="muted">Forecast lernt…</span>';
  let target=f.target==="lowbattery"?"bis LowBattery":"bis 0 % (Trend)";
  let range=(f.low!=null&&f.high!=null)?` · ${fmt(f.low)}–${fmt(f.high)}`:"";
  let basis=f.samples?` · ${f.samples} Runs · ${f.scope==="device"?"Gerät":"SK/SKM"}`:" · aktueller Run";
  return `<span>${target} <strong>${fmt(f.seconds)}</strong> <em>~${clockFromNow(f.seconds)}</em>${range}${basis}</span>`}

async function api(path,opts){let r=await fetch(path,opts);if(!r.ok)throw new Error(await r.text());return r.json()}
async function batteryChanged(ip,ch){if(!confirm("Neuen Batteriesatz für diesen Kanal starten?"))return;await api('/api/battery-changed',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ip,channel:ch})});}
async function setProfile(ip,ch,type,current){let label=prompt("Physischer Sender / Profilname, z. B. SK 01 oder SKM 03. Leer = Zuordnung entfernen.",current||"");if(label===null)return;await api('/api/profile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ip,channel:ch,tx_type:type,label})});setTimeout(tick,400)}

function card(rx,c){if(!c.online)return `<div class="card offline"><div class="name">CH ${c.channel}</div><div class="meta">Kein Sender / offline</div></div>`;
  let p=c.profile?.label||"Profil zuweisen";let run=c.run||{};let warn=(c.warnings||[]).length?`<span class="warning">${esc((c.warnings||[]).join(', '))}</span>`:"";
  return `<div class="card ${c.gauge<=10?'low':''}">
    <div class="top"><div><div class="name">${esc(c.name)}</div><div class="meta">CH ${c.channel} · ${esc(c.type)} · <button class="link" onclick='setProfile(${JSON.stringify(rx.ip)},${c.channel},${JSON.stringify(c.type)},${JSON.stringify(c.profile?.label||"")})'>${esc(p)}</button></div></div><div class="pct">${c.gauge}%</div></div>
    <div class="bar"><div class="fill" style="width:${Math.max(0,Math.min(100,c.gauge))}%"></div></div>
    <div class="stats"><span>Aktiv <strong>${fmt(run.active_seconds)}</strong></span><span>Run #${run.id||'–'}</span><span>Recoveries ${run.recovery_count||0}</span>${warn}</div>
    <div class="forecast">${forecastText(c.forecast)}</div>
    <div class="graphwrap"><canvas id="g-${rx.ip.replaceAll('.','-')}-${c.channel}" height="150"></canvas></div>
    <div class="actions"><button onclick='batteryChanged(${JSON.stringify(rx.ip)},${c.channel})'>Batterie gewechselt</button><span class="hint">Kleine Erholung nach Pause bleibt automatisch derselbe Run.</span></div>
  </div>`}

async function drawGraph(canvas,runId){if(!canvas||!runId)return;let rows=await api('/api/history?run_id='+runId);let pts=rows.filter(x=>x.online&&x.gauge!=null);let ctx=canvas.getContext('2d');let dpr=window.devicePixelRatio||1;let w=canvas.clientWidth,h=150;canvas.width=w*dpr;canvas.height=h*dpr;ctx.scale(dpr,dpr);ctx.clearRect(0,0,w,h);
  ctx.strokeStyle='#2b3440';ctx.lineWidth=1;ctx.font='11px -apple-system';ctx.fillStyle='#758191';
  [0,25,50,75,100].forEach(v=>{let y=8+(100-v)/100*(h-24);ctx.beginPath();ctx.moveTo(28,y);ctx.lineTo(w-6,y);ctx.stroke();ctx.fillText(v+'%',1,y+4)});
  if(pts.length<2){ctx.fillText('Sammle Verlauf …',40,75);return}
  let t0=pts[0].active_elapsed,t1=pts[pts.length-1].active_elapsed;if(t1<=t0)t1=t0+1;
  function xy(p){return [28+(p.active_elapsed-t0)/(t1-t0)*(w-36),8+(100-p.gauge)/100*(h-24)]}
  ctx.strokeStyle='#dce3ed';ctx.lineWidth=2;ctx.beginPath();pts.forEach((p,i)=>{let [x,y]=xy(p);i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.stroke();
  pts.filter(p=>p.recovery).forEach(p=>{let[x,y]=xy(p);ctx.beginPath();ctx.arc(x,y,4,0,Math.PI*2);ctx.fillStyle='#f4c76b';ctx.fill()});
  let last=pts[pts.length-1],[x,y]=xy(last);ctx.beginPath();ctx.arc(x,y,4,0,Math.PI*2);ctx.fillStyle='#ffffff';ctx.fill();
}

async function tick(){try{let d=await api('/api/state');let app=document.getElementById('app');document.getElementById('status').textContent=d.receivers.length+' Empfänger online · '+d.receivers.reduce((n,r)=>n+r.channels.filter(c=>c.online).length,0)+' TX online · Update '+d.poll_seconds+' s';
  if(!d.receivers.length){app.innerHTML='<div class="empty">Suche EW-DX EM 2 im Netzwerk…</div>';return}
  const order=["HS1","HS2","1","2","3","4"];const all=d.receivers.flatMap(rx=>rx.channels.map(c=>({rx,c})));all.sort((a,b)=>{const key=x=>String(x.c.name||"").trim().toUpperCase();const rank=x=>{const i=order.indexOf(key(x));return i<0?999:i};return rank(a)-rank(b)||key(a).localeCompare(key(b),"de")});app.innerHTML=`<section class="receiver"><h2>Senderübersicht</h2><div class="grid">${all.map(({rx,c})=>card(rx,c)).join('')}</div></section>`;
  for(let rx of d.receivers)for(let c of rx.channels)if(c.online&&c.run?.id)drawGraph(document.getElementById(`g-${rx.ip.replaceAll('.','-')}-${c.channel}`),c.run.id);
}catch(e){document.getElementById('status').textContent='Verbindung zum Monitor unterbrochen';console.error(e)}}
tick();setInterval(tick,5000);
