'use strict';
(async()=>{
 const [story,data]=await Promise.all([fetch('/demo-files/storyboard.json').then(r=>r.json()),fetch('/demo-data').then(r=>r.json())]);
 const $=id=>document.getElementById(id),escape=s=>String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
 let typeTimer,version=0;
 function resize(){const box=$('iframe-box'),scale=box.clientWidth/1440;$('app-frame').style.transform=`scale(${scale})`;$('app-frame').style.height=(box.clientHeight/scale)+'px';}
 new ResizeObserver(resize).observe($('iframe-box'));
 function source(scene){
  $('source-title').textContent=scene.source?'Change at the source':'The current row';
  const text=scene.source||"-- Source: demo_sales.orders\n-- Primary key: id\n\n-- Binary log settings\nbinlog_format = ROW\nbinlog_row_image = FULL\n\n-- Capture starts at End\n-- Existing rows are not copied.";
  clearInterval(typeTimer);$('source-code').textContent='';$('source-code').classList.add('typing');let at=0;
  typeTimer=setInterval(()=>{at+=4;$('source-code').textContent=text.slice(0,at);if(at>=text.length){clearInterval(typeTimer);$('source-code').classList.remove('typing');}},35);
  $('source-note').textContent=scene.id==='delete'?'The source row is absent. Its previous states remain in the target.':scene.id==='dcl'?'Locked demonstration account. No live users or privileges are changed.':scene.id==='ddl'?'A schema change is not an UPDATE event for every existing row.':'Illustrative SQL sequence. No production database is modified.';
 }
 function target(scene){
  let html='<div class="eyebrow">DEMO_AUDIT</div><h2>Audit-history store</h2>';
  if(scene.target==='event'){
   const e=data.events[scene.event];html+=`<span class="op ${e.operation}">${e.operation}</span><div class="meta">Table: demo_sales.orders<br>Record key: {"id":1042}<br>Partition 0 · Offset ${e.queue_offset}</div><div class="image-box before"><span class="small-label">BEFORE</span><pre>${escape(JSON.stringify(e.before_image,null,2))}</pre></div><div class="image-box after"><span class="small-label">AFTER</span><pre>${escape(JSON.stringify(e.after_image,null,2))}</pre></div><p class="note"><strong>${scene.event+1} retained event${scene.event?'s':''}</strong> for this key in the replay.</p>`;
  }else if(scene.target==='ddl'){
   html+='<span class="op UPDATE">SCHEMA · ALTER</span><div class="image-box"><span class="small-label">SEMANTIC DIFFERENCE</span><pre>+ region\n  VARCHAR(16)\n  DEFAULT \'EMEA\'\n\n~ amount precision\n  10 → 14\n  scale remains 2</pre></div><div class="meta">audit_schema_changes<br>before_definition<br>after_definition<br>semantic_delta<br>fidelity: partial</div><p class="note">Partition and index details:<br><strong>unknown if absent from evidence</strong></p>';
  }else if(scene.target==='dcl'){
   html='<div class="eyebrow">SECURITY AUDIT EXTENSION</div><h2>Not collected by this app</h2><div class="callout warning"><strong>No DCL history is claimed</strong>Row CDC is not a complete user and privilege audit.</div><ul class="bullet-list"><li>Actor and client identity</li><li>Account, role and scope</li><li>GRANT / REVOKE action</li><li>Outcome and timestamp</li></ul><p class="note">Requires a separately configured database-audit source and collector.</p>';
  }else if(scene.target==='lookup'){
   html+='<div class="image-box"><span class="small-label">TYPED RECORD KEY</span><pre>{"id":1042}</pre></div><span class="small-label">STABLE KEY FINGERPRINT</span><div class="hash">'+escape(data.key_hash)+'</div><ul class="bullet-list"><li>CREATE · 10:14 UTC</li><li>UPDATE · 10:15 UTC</li><li>DELETE · 10:16 UTC</li></ul><p class="note">Key hash is indexed. Before/after image hashes change with content.</p>';
  }else if(scene.target==='baseline'){
   html+='<div class="callout"><strong>Awaiting capture</strong>Schema baseline is established when this existing table is registered and capture initializes.</div><div class="image-box"><span class="small-label">INITIAL CAPTURE: END</span><pre>Existing row snapshots: 0\n\nSchema metadata: baseline\nFuture row changes: capture</pre></div><p class="note">The earlier CREATE command is not a captured event in this sequence.</p>';
  }else if(scene.target==='streams'){
   html+='<ul class="bullet-list"><li>Data stream: demo-orders</li><li>Schema stream: audit_1</li><li>One mapping → one target table</li></ul><div class="callout"><strong>After target commit</strong>The writer acknowledges its consumed messages.</div>';
  }else if(scene.target==='chart'){
   html+='<div class="callout"><strong>Separate chart sample</strong>Overview counts illustrate a larger retained dataset, beyond the three DML events.</div><ul class="bullet-list"><li>Database and table totals</li><li>Custom UTC ranges</li><li>Operation breakdown</li><li>Download PNG / CSV</li></ul>';
  }else{
   html+='<div class="image-box"><span class="small-label">ROW HISTORY</span><pre>record_key\nbefore_image\nafter_image\noperation\nevent_time\nsource_metadata</pre></div><div class="image-box"><span class="small-label">SCHEMA HISTORY</span><pre>before_definition\nafter_definition\nsemantic_delta</pre></div><p class="note">Retained in the target HeatWave database.</p>';
  }
  $('target-body').innerHTML=html;
 }
 function card(scene){
  if(scene.card==='welcome')return '<div class="hero"><div class="kicker">DATA ROW AUDIT · FIVE MINUTES</div><h2>Every change.<br>A complete history.</h2><p>Follow one order from its creation to its final deletion.</p><div class="step">DML images · Schema evidence · Search · Charts</div><p style="font-size:22px">OCI Streaming is the messaging service in this walkthrough.</p></div>';
  if(scene.card==='safe-harbor')return `<div class="safe"><h2>Safe harbor statement</h2><p>${escape(scene.text)}</p><small>Independent application demonstration. Oracle styling does not imply Oracle authorship or endorsement.</small></div>`;
  if(scene.card==='architecture')return '<div class="architecture"><img src="/demo-files/assets/oci-streaming-architecture.png" alt="OCI Streaming architecture with compartment, VCN and subnet boundaries"><p>Reference topology · CIDRs are examples · Debezium initiates source TLS 3306 · Queue clients use 9092</p></div>';
  if(scene.card==='prepare')return '<div class="process"><div class="box">1. Create the source table<small>Primary key: id</small></div><span class="arrow">↓</span><div class="box">2. Register the table mapping<small>Source table → audit target table</small></div><span class="arrow">↓</span><div class="box">3. Initialize at End<small>Schema baseline; no existing-row snapshot</small></div><span class="arrow">↓</span><div class="box">4. Capture subsequent changes<small>CREATE / UPDATE / DELETE</small></div></div>';
  if(scene.card==='ddl')return '<h2 class="ddl-title">Inspect captured schema evidence</h2><p class="ddl-sub">SQL inspection of the target; no dedicated schema browser is claimed.</p><div class="control-terminal"><pre>SELECT change_kind,\n       before_definition,\n       after_definition,\n       semantic_delta, fidelity\nFROM demo_audit.audit_schema_changes\nWHERE flow_id = 1\n  AND object_name = \'demo_sales.orders\'\nORDER BY observed_at;</pre></div><div class="schema-diff"><div><h3>BEFORE</h3><pre>amount\nDECIMAL(10,2)\n\nregion: absent</pre></div><div><h3>AFTER</h3><pre>amount\nDECIMAL(14,2)\n\nregion: VARCHAR(16)\nDEFAULT \'EMEA\'</pre></div></div>';
  if(scene.card==='dcl')return '<h2>Different evidence for a different question</h2><p class="ddl-sub">“Who changed this user’s privileges?”</p><div class="process" style="height:690px"><div class="box">MySQL Enterprise Audit plugin<small>Explicit filters and account assignment</small></div><span class="arrow">↓</span><div class="box" style="border-left-color:#bc8b49">Separate audit-log collector<small>Extension required · not implemented here</small></div><span class="arrow">↓</span><div class="box">Security event history<small>Actor · account · scope · action · outcome</small></div></div><p class="ddl-sub">Do not infer a complete privilege timeline from row images or the presence of a statement in binlog.</p>';
  return '<div class="summary"><h2>Choose the right evidence.</h2><div class="summary-grid"><div><h3>Rows</h3><p>Before / after images<br>Stable row-key lookup<br>Replay deduplication</p></div><div><h3>Structures</h3><p>Schema baseline<br>Captured definitions<br>Semantic comparison</p></div><div><h3>Operations</h3><p>Database drill-down<br>Custom time ranges<br>Charts and downloads</p></div></div><div class="callout"><strong>Security history remains a separate extension.</strong>This video is an illustrative replay. A live acceptance test must verify source changes, queue delivery and committed target evidence.</div></div>';
 }
 async function setScene(index){
  const scene=story.scenes[index];version++;source(scene);target(scene);$('cursor').hidden=true;
  $('scene-title').textContent=scene.title;$('chapter-number').textContent=String(index+1).padStart(2,'0')+' / '+story.scenes.length;$('caption').textContent=scene.caption;
  $('workspace').className=scene.mode==='browser'?'browser-mode':scene.mode==='full'?'full':'triple';
  $('browser').style.display=scene.page?'block':'none';$('card').style.display=scene.page?'none':'block';
  $('center-mode').textContent=scene.page?'Application workspace':scene.card==='architecture'?'Deployment architecture':scene.card==='ddl'?'Control terminal · schema inspection':'Guided walkthrough';
  if(scene.page){$('app-frame').src='/'+scene.page;$('browser-address').textContent='Demo workspace / '+scene.page.split('?')[0];}else $('card').innerHTML=card(scene);
  tick(scene.start);resize();return scene;
 }
 function tick(seconds){$('elapsed').textContent=String(Math.floor(seconds/60)).padStart(2,'0')+':'+String(Math.floor(seconds%60)).padStart(2,'0');$('progress').style.width=(seconds/300*100)+'%';}
 window.demo={story,data,setScene,tick,expand:()=>{$('workspace').className='browser-mode';},cursor:(x,y,click=false)=>{const c=$('cursor');c.hidden=false;c.style.left=x+'px';c.style.top=y+'px';if(click){c.classList.remove('click');void c.offsetWidth;c.classList.add('click');}}};
 await setScene(0);window.demoReady=true;
})();
