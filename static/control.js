'use strict';
(() => {
const table=document.getElementById('control-flows');if(!table||table.dataset.selectable!=='true')return;let submitting=false;
const health=new Map(), buttons=[...document.querySelectorAll('[data-flow-action]')], more=document.querySelector('[data-flow-more]'), run=document.querySelector('[data-flow-more-run]');
const selected=()=>[...table.tBodies[0].rows].filter(row=>row.querySelector('.row-selection')?.checked);
function allowed(row,action){
 if(!row)return false;
 const retired=row.dataset.savedStatus==='retired', data=health.get(row.dataset.rowId), workloads=data?.workloads||[], connector=row.dataset.connector==='true', writer=row.dataset.writer==='true';
 if(['source','capture','partitions','refresh'].includes(action))return true;
 if(action==='prepare')return !retired&&row.dataset.queue==='true';
 if(action==='delete')return retired&&!connector&&!writer;
 if(action==='terminate')return retired&&(connector||writer);
 if(retired)return false;
 if(action==='deploy')return !connector||!writer;
 if(action==='retire')return connector||writer;
 if(action.startsWith('update_')){const role=action==='update_writer'?'History writer':'Debezium';const w=workloads.find(w=>w.role===role);return ['ACTIVE','INACTIVE'].includes(w?.state)&&w.update_available;}
 if(!data||!workloads.length)return false;
 if(action==='stop')return workloads.some(w=>w.role==='Debezium'&&w.state==='ACTIVE');
 if(action==='stop_all')return workloads.some(w=>w.state==='ACTIVE');
 if(action==='restart')return connector&&writer&&workloads.every(w=>['ACTIVE','INACTIVE'].includes(w.state));
 return connector&&writer&&workloads.some(w=>w.state==='INACTIVE')&&workloads.every(w=>['ACTIVE','INACTIVE'].includes(w.state));
}
function update(){if(submitting)return;const rows=selected(),row=rows.length===1?rows[0]:null;
 buttons.forEach(b=>{b.disabled=!allowed(row,b.dataset.flowAction);b.title=b.disabled?(row?'Check process status and the next-step tips.':'Select exactly one flow.'):b.textContent.trim();});
 if(more){[...more.options].forEach(o=>{if(o.value)o.disabled=!allowed(row,o.value);});run.disabled=!allowed(row,more.value)||!more.value;}
 document.querySelector('[data-control-selection]').textContent=row?'Selected: '+row.querySelector('[data-flow-name]').textContent+'. Stop pauses capture; the writer drains the queue. Update stops and replaces only the chosen workload.':rows.length>1?'Select exactly one flow for lifecycle actions. CSV download can include multiple rows.':'Tick the checkbox beside a flow to enable its available actions.';
}
function act(action){if(submitting)return;const rows=selected();if(rows.length!==1||!allowed(rows[0],action))return;const row=rows[0];
 if(['source','capture','partitions'].includes(action)){location.href=row.dataset[action+'Url'];return;}
 const confirmation={stop:'Pause Debezium capture? The history writer keeps running to drain queued messages, then waits. Start resumes capture from saved offsets.',stop_all:'Stop both workloads for maintenance? Queued messages remain subject to messaging retention. Audit history is retained.',restart:'Restart both deployed workloads using their existing checkpoints?',retire:'Retire this flow? Capture stops and audit history is preserved.',terminate:'Terminate both retired containers? Audit history is retained.',delete:'Delete this retired registration? Audit history is retained.',update_debezium:'Update Debezium to the selected image? This stops only Debezium, waits for shutdown, then creates its replacement using the existing offsets and schema history. The history writer keeps its current state.',update_writer:'Update the history writer to the selected image? This stops only the writer, waits for shutdown, then creates its replacement using existing checkpoints. Debezium keeps its current state and may continue adding queued records.',prepare:'Prepare the schema topic? Existing topics and their contents are retained.'};
 if(confirmation[action]&&!confirm(confirmation[action]))return;
 submitting=true;buttons.forEach(b=>b.disabled=true);if(run)run.disabled=true;if(more)more.disabled=true;
 document.querySelector('[data-control-selection]').textContent=action.startsWith('update_')?'Checking the selected image and waiting for this workload to stop before replacement…':'Submitting action…';
 const form=document.createElement('form');form.method='POST';form.action=['prepare','terminate'].includes(action)?row.dataset[action+'Url']:row.dataset.actionUrl.replace('ACTION',action);
 const csrf=document.createElement('input');csrf.type='hidden';csrf.name='csrf';csrf.value=document.getElementById('control-csrf').value;form.append(csrf);document.body.append(form);form.submit();
}
buttons.forEach(b=>b.addEventListener('click',()=>act(b.dataset.flowAction)));more?.addEventListener('change',update);run?.addEventListener('click',()=>act(more.value));
table.addEventListener('table:selection-change',update);window.addEventListener('pageshow',()=>{submitting=false;if(more)more.disabled=false;update();});
document.addEventListener('flow:status',event=>{health.set(String(event.detail.id),event.detail);update();});update();
})();
