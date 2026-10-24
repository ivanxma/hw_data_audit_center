'use strict';
(() => {
const colors={CREATE:'#24846a',UPDATE:'#3478b8',DELETE:'#c54f3d',SNAPSHOT:'#8863b3',TRUNCATE:'#a67912'};
const iso=date=>date.toISOString().slice(0,16);
document.querySelectorAll('[data-audit-filters]').forEach(form=>{
 const from=form.querySelector('[name=from]'),to=form.querySelector('[name=to]'),preset=form.querySelector('[data-range-preset]'),interval=form.querySelector('[name=interval]');
 preset?.addEventListener('change',()=>{if(!preset.value)return;const end=new Date();to.value=iso(end);from.value=iso(new Date(end.getTime()-Number(preset.value)*3600000));if(interval?.value==='minute'&&Number(preset.value)>1)interval.value=Number(preset.value)>720?'day':'hour';});
 [from,to].forEach(input=>input?.addEventListener('change',()=>{if(preset)preset.value='';}));
 interval?.addEventListener('change',()=>{if(interval.value==='minute'){const end=to.value?new Date(to.value+'Z'):new Date();to.value=iso(end);from.value=iso(new Date(end.getTime()-3600000));if(preset)preset.value='';}});
});
document.querySelectorAll('[data-keep-range]').forEach(a=>a.addEventListener('click',()=>{const form=document.querySelector('[data-audit-filters]');const url=new URL(a.href);for(const key of ['from','to','operation']){const value=form?.elements.namedItem(key)?.value;if(value)url.searchParams.set(key,value);}a.href=url.href;}));
function prepare(canvas,height){const box=canvas.getBoundingClientRect(),ratio=devicePixelRatio||1;if(!box.width)return null;if(height)canvas.style.height=height+'px';const h=height||box.height;canvas.width=Math.round(box.width*ratio);canvas.height=Math.round(h*ratio);const ctx=canvas.getContext('2d');ctx.scale(ratio,ratio);ctx.fillStyle='#fff';ctx.fillRect(0,0,box.width,h);ctx.font='11px system-ui';return {ctx,w:box.width,h};}
const canvas=document.getElementById('audit-chart');
if(canvas){
 const rows=JSON.parse(canvas.dataset.chart),interval=Number(canvas.dataset.interval),anchor=interval===604800?345600:0;
 const available=rows.map(r=>Number(r.bucket)),start=canvas.dataset.start?Number(canvas.dataset.start):Math.min(...available),end=canvas.dataset.end?Number(canvas.dataset.end):Math.max(...available);
 const first=Math.floor((start-anchor)/interval)*interval+anchor,last=Math.floor((end-anchor)/interval)*interval+anchor;
 const buckets=[];if(Number.isFinite(first)&&Number.isFinite(last))for(let t=first;t<=last&&buckets.length<=1000;t+=interval)buckets.push(t);
 const ops=Object.keys(colors), values=Object.fromEntries(ops.map(op=>[op,new Map(rows.filter(r=>r.operation===op).map(r=>[Number(r.bucket),Number(r.n)]))]));
 const legend=document.getElementById('audit-chart-legend');ops.forEach(op=>{const span=document.createElement('span'),dot=document.createElement('i');dot.style.background=colors[op];span.append(dot,document.createTextNode(op+' · '+rows.filter(r=>r.operation===op).reduce((sum,r)=>sum+Number(r.n),0)));legend.append(span);});
 const tooltip=document.createElement('div');tooltip.className='chart-tooltip';tooltip.hidden=true;canvas.parentElement.append(tooltip);let layout;
 const type=()=>document.querySelector('[name=chart_type]')?.value||canvas.dataset.type||'line';
 function draw(){const box=prepare(canvas);if(!box)return;const {ctx,w,h}=box,left=52,right=w-22,top=22,bottom=h-58;layout={left,right,top,bottom};document.getElementById('chart-empty').hidden=rows.length>0;if(!buckets.length)return;
 const sums=buckets.map(t=>ops.reduce((sum,op)=>sum+(values[op].get(t)||0),0));const max=Math.max(1,...(type()==='stacked'?sums:rows.map(r=>Number(r.n))));
 const step=(right-left)/buckets.length,x=i=>left+step*(i+.5),y=n=>bottom-(bottom-top)*n/max;
 for(let i=0;i<=4;i++){const yy=top+(bottom-top)*i/4;ctx.strokeStyle='#e6ebed';ctx.beginPath();ctx.moveTo(left,yy);ctx.lineTo(right,yy);ctx.stroke();ctx.fillStyle='#5f6f79';ctx.fillText(String(Math.round(max*(1-i/4))),5,yy+4);}
 const active=ops.filter(op=>values[op].size);
 active.forEach((op,k)=>{ctx.strokeStyle=colors[op];ctx.fillStyle=colors[op];ctx.lineWidth=2;
 if(type()==='line'){ctx.beginPath();buckets.forEach((t,i)=>{const yy=y(values[op].get(t)||0);i?ctx.lineTo(x(i),yy):ctx.moveTo(x(i),yy);});ctx.stroke();buckets.forEach((t,i)=>{if(values[op].has(t)){ctx.beginPath();ctx.arc(x(i),y(values[op].get(t)),3,0,Math.PI*2);ctx.fill();}});}
 else buckets.forEach((t,i)=>{const n=values[op].get(t)||0;if(type()==='stacked'){const lower=active.slice(0,k).reduce((s,o)=>s+(values[o].get(t)||0),0);ctx.fillRect(x(i)-step*.38,y(lower+n),Math.max(1,step*.76),y(lower)-y(lower+n));}else{const bw=step*.8/Math.max(1,active.length);ctx.fillRect(x(i)-step*.4+k*bw,y(n),Math.max(1,bw-1),bottom-y(n));}});
 });
 ctx.fillStyle='#526670';ctx.textAlign='center';const stride=Math.max(1,Math.ceil(buckets.length/Math.max(1,Math.floor((right-left)/115))));buckets.forEach((t,i)=>{if(i%stride)return;const stamp=new Date(t*1000).toISOString();ctx.fillText(stamp.slice(5,10),x(i),h-32);if(interval<86400)ctx.fillText(stamp.slice(11,16),x(i),h-17);});ctx.textAlign='left';ctx.fillText('UTC · events per '+({60:'minute',3600:'hour',86400:'day',604800:'week'}[interval]||'interval'),left,13);
 }
 canvas.addEventListener('mousemove',e=>{if(!layout||!buckets.length)return;const r=canvas.getBoundingClientRect(),x=e.clientX-r.left;const i=Math.max(0,Math.min(buckets.length-1,Math.floor((x-layout.left)/(layout.right-layout.left)*buckets.length))),t=buckets[i];tooltip.textContent=new Date(t*1000).toISOString().replace('T',' ').slice(0,19)+' UTC · '+ops.map(op=>op+': '+(values[op].get(t)||0)).join(' · ');tooltip.hidden=false;});canvas.addEventListener('mouseleave',()=>tooltip.hidden=true);
 document.querySelector('[name=chart_type]')?.addEventListener('change',draw);new ResizeObserver(draw).observe(canvas);
}
const global=document.getElementById('global-chart');
if(global){const rows=JSON.parse(global.dataset.chart),group=global.dataset.group,groups=new Map(),palette=['#c74634','#286f87','#327d60','#8861a8','#ac791d','#4a64a4','#b3517c','#596b38'];
 rows.forEach(row=>{const key=group==='table'?String(row.flow_id):row.database;let g=groups.get(key);if(!g){g={...row,label:group==='table'?row.database+' · '+row.label:row.database,count:0,missing:0};groups.set(key,g);}if(row.count===null)g.missing++;else g.count+=Number(row.count);});
 const items=[...groups.values()].sort((a,b)=>b.count-a.count),total=items.reduce((s,r)=>s+r.count,0),legend=document.getElementById('global-chart-legend');let hits=[],plot;
 const href=row=>{const url=new URL(location.pathname,location.origin);url.searchParams.set('from',global.dataset.from);url.searchParams.set('to',global.dataset.to);const operation=new URLSearchParams(location.search).get('operation');if(operation)url.searchParams.set('operation',operation);if(group==='table'){url.searchParams.set('flow_id',row.flow_id);url.searchParams.set('tab','audit-trends');url.searchParams.set('chart_type','bar');const hours=(Date.parse(global.dataset.to+'Z')-Date.parse(global.dataset.from+'Z'))/3600000;url.searchParams.set('interval',hours<=120?'hour':hours/24<=120?'day':'week');}else{url.searchParams.set('tab','audit-global');url.searchParams.set('group_by','table');url.searchParams.set('source_id',row.source_id);url.searchParams.set('source_schema',row.schema||row.database.split(' / ').at(-1));url.searchParams.set('global_type','bar');}return url.href;};
 items.forEach((row,i)=>{row.color=palette[i%palette.length];const a=document.createElement('a'),dot=document.createElement('i');a.href=href(row);dot.style.background=row.color;a.append(dot,document.createTextNode(row.label+' · '+row.count.toLocaleString()+' ('+(total?row.count/total*100:0).toFixed(1)+'%)'+(row.missing?' · incomplete':'')));legend.append(a);});
 function draw(){const type=document.querySelector('[name=global_type]')?.value||global.dataset.type;const box=prepare(global,type==='bar'?Math.max(300,items.length*52+45):360);if(!box)return;const {ctx,w,h}=box;hits=[];document.getElementById('global-chart-empty').hidden=total>0;if(!total)return;
 if(type==='pie'){const cx=w/2,cy=h/2,r=Math.min(w*.43,h*.43);plot={cx,cy,r};let angle=-Math.PI/2;items.forEach(row=>{const end=angle+row.count/total*Math.PI*2;ctx.fillStyle=row.color;ctx.beginPath();ctx.moveTo(cx,cy);ctx.arc(cx,cy,r,angle,end);ctx.closePath();ctx.fill();ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();hits.push({start:angle,end,row});angle=end;});}
 else{const left=18,right=w-65,max=Math.max(1,...items.map(r=>r.count));items.forEach((row,i)=>{const y=27+i*52,width=row.count/max*(right-left);ctx.fillStyle='#374c55';let label=row.label;while(label.length>5&&ctx.measureText(label).width>w-40)label=label.slice(0,-2)+'…';ctx.fillText(label,left,y-5);ctx.fillStyle=row.color;ctx.fillRect(left,y,width,24);ctx.fillStyle='#374c55';ctx.fillText(String(row.count),left+width+6,y+17);hits.push({x:left,y,width:Math.max(width,6),height:24,row});});plot=null;}}
 function hit(e){const rect=global.getBoundingClientRect(),x=e.clientX-rect.left,y=e.clientY-rect.top;if(plot){const dx=x-plot.cx,dy=y-plot.cy;if(Math.hypot(dx,dy)>plot.r)return;let a=Math.atan2(dy,dx);if(a<-Math.PI/2)a+=Math.PI*2;return hits.find(h=>a>=h.start&&a<h.end)?.row;}return hits.find(h=>x>=h.x&&x<=h.x+h.width&&y>=h.y&&y<=h.y+h.height)?.row;}
 global.addEventListener('click',e=>{const row=hit(e);if(row)location.href=href(row);});global.addEventListener('mousemove',e=>{const row=hit(e);global.style.cursor=row?'pointer':'default';global.title=row?row.label+': '+row.count+' changes. Click for details.':'';});document.querySelector('[name=global_type]')?.addEventListener('change',draw);new ResizeObserver(draw).observe(global);
}
document.querySelectorAll('[data-download-chart]').forEach(button=>button.addEventListener('click',()=>{const c=document.getElementById(button.dataset.downloadChart);if(!c)return;const a=document.createElement('a');a.download=button.dataset.downloadChart+'.png';a.href=c.toDataURL('image/png');a.click();}));
})();
