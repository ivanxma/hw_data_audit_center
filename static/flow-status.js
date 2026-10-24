'use strict';
(() => {
const watches=[...document.querySelectorAll('[data-flow-watch]')];if(!watches.length)return;
const states=new Map(watches.map(w=>[w.dataset.flowWatch,w.dataset.status]));
const allowed=new Set(['active','attention','draft','retired','stopped','paused','starting','stopping','deploying','pending','unavailable']);
let stopped=false;
function counts(){
 const values=[...states.values()],active=document.querySelector('[data-flow-active-count]'),attention=document.querySelector('[data-flow-attention-count]'),progress=document.querySelector('[data-flow-progress-count]');
 if(active)active.textContent=values.filter(s=>s==='active').length;
 if(attention)attention.textContent=values.filter(s=>['attention','pending','draft','unavailable'].includes(s)).length;
 if(progress)progress.textContent=(values.includes('paused')?values.filter(s=>s==='paused').length+' capture paused · ':'')+values.filter(s=>['starting','stopping','deploying'].includes(s)).length+' in progress · updates every 15 seconds';
}
function render(id,status,detail){
 states.set(id,status);
 document.querySelectorAll('[data-flow-badge]').forEach(b=>{if(b.dataset.flowBadge===id){b.className='badge '+status;b.textContent=status==='paused'?'capture paused':status;b.title=detail;}});
 document.querySelectorAll('[data-flow-detail]').forEach(p=>{if(p.dataset.flowDetail===id)p.textContent=detail;});counts();
}
async function refresh(watch){
 const id=watch.dataset.flowWatch;
 try{
  const response=await fetch(watch.dataset.url,{headers:{Accept:'application/json'},credentials:'same-origin'});
  if(response.redirected||response.status===401){stopped=true;render(id,'unavailable','Sign in again to refresh workload status.');document.dispatchEvent(new CustomEvent('flow:status',{detail:{id,status:'unavailable',workloads:[]}}));return;}
  const data=await response.json();
  if(!response.ok)throw Error(data.message||'Workload status could not be checked.');
  if(!allowed.has(data.status)||!Array.isArray(data.workloads))throw Error('Invalid workload status response.');
  const detail=data.workloads.map(w=>w.role+': '+(w.process==='EXITED'?'exited'+(w.exit_code===null?'':' (code '+w.exit_code+')'):w.process||w.state).toLowerCase().replaceAll('_',' ')+(w.task_state?' / connector task '+w.task_state.toLowerCase().replaceAll('_',' '):'')).join(' · ');
  document.querySelectorAll('[data-process-flow]').forEach(block=>{if(block.dataset.processFlow!==id)return;const w=data.workloads.find(w=>w.role===block.dataset.processRole);if(!w)return;const state=block.querySelector('[data-process-state]');state.textContent=(w.process==='EXITED'?'Exited · code '+w.exit_code:(w.process||w.state).toLowerCase().replaceAll('_',' '))+(w.task_state?' · task '+w.task_state.toLowerCase().replaceAll('_',' '):'');state.classList.toggle('needs-action',(w.state==='INACTIVE'&&data.status!=='paused')||w.process==='EXITED'||w.task_state==='FAILED');block.querySelector('[data-process-image]').textContent=w.update_available?'Image update available':'';block.querySelector('[data-process-tip]').textContent=w.guidance||(w.process==='RUNNING'?(w.role==='Debezium'?'Check queue arrival to verify capture.':'Check newest target events and checkpoint progress.'):'Review readiness and workload logs.');});
  document.querySelectorAll('[data-flow-checked]').forEach(el=>{if(el.dataset.flowChecked===id)el.textContent='Checked '+new Date().toLocaleTimeString();});
  document.dispatchEvent(new CustomEvent('flow:status',{detail:data}));
  const guidance=data.workloads.map(w=>w.guidance).filter(g=>typeof g==='string').join(' ');
  render(id,data.status,detail+' · Checked '+new Date().toLocaleTimeString()+(guidance?' — '+guidance:''));
 }catch(error){render(id,'unavailable',error.message||'Workload status is unavailable.');document.dispatchEvent(new CustomEvent('flow:status',{detail:{id,status:'unavailable',workloads:[]}}));}
}
async function poll(){
 if(stopped)return;
 if(!document.hidden){
  // Keep cloud calls bounded even when the number of registered flows grows.
  for(let i=0;i<watches.length&&!stopped;i+=2)await Promise.all(watches.slice(i,i+2).map(refresh));
 }
 if(!stopped)setTimeout(poll,15000);
}
poll();
})();
