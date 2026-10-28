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
 if(action.startsWith('update_')){const role=action==='update_writer'?'History writer':'Debezium';const w=workloads.find(w=>w.role===role);return ['ACTIVE','INACTIVE'].includes(w?.state);}
 if(!data||!workloads.length)return false;
 if(action==='stop')return workloads.some(w=>w.role==='Debezium'&&w.state==='ACTIVE');
 if(action==='stop_all')return workloads.some(w=>w.state==='ACTIVE');
 if(['restart','apply_stream_settings'].includes(action))return connector&&writer&&workloads.every(w=>['ACTIVE','INACTIVE'].includes(w.state));
 return connector&&writer&&workloads.some(w=>w.state==='INACTIVE')&&workloads.every(w=>['ACTIVE','INACTIVE'].includes(w.state));
}
function update(){if(submitting)return;const rows=selected(),row=rows.length===1?rows[0]:null;
 buttons.forEach(b=>{b.disabled=!allowed(row,b.dataset.flowAction);b.title=b.disabled?(row?'Check process status and the next-step tips.':'Select exactly one flow.'):b.textContent.trim();});
 if(more){[...more.options].forEach(o=>{if(o.value)o.disabled=!allowed(row,o.value);});run.disabled=!allowed(row,more.value)||!more.value;}
 document.querySelector('[data-control-selection]').textContent=row?'Selected: '+row.querySelector('[data-flow-name]').textContent+'. Stop pauses capture; the writer drains the queue. Update stops and replaces only the chosen workload.':rows.length>1?'Select exactly one flow for lifecycle actions. CSV download can include multiple rows.':'Tick the checkbox beside a flow to enable its available actions.';
}
function act(action){if(submitting)return;const rows=selected();if(rows.length!==1||!allowed(rows[0],action))return;const row=rows[0];
 if(['source','capture','partitions'].includes(action)){location.href=row.dataset[action+'Url'];return;}
 if(action.startsWith('update_')){openImages(row,action);return;}
 const confirmation={apply_stream_settings:'Apply the saved Streaming limits and pacing settings using the selected workload images? The writer is replaced first, then Debezium. Checkpoints and audit history are retained. Limit decreases require a separate drain and review.',stop:'Pause Debezium capture? The history writer keeps running to drain queued messages, then waits. Start resumes capture from saved offsets.',stop_all:'Stop both workloads for maintenance? Queued messages remain subject to messaging retention. Audit history is retained.',restart:'Restart both deployed workloads using their existing checkpoints?',retire:'Retire this flow? Capture stops and audit history is preserved.',terminate:'Terminate both retired containers? Audit history is retained.',delete:'Delete this retired registration? Audit history is retained.',prepare:'Prepare the schema stream? Existing streams and their contents are retained.'};
 if(confirmation[action]&&!confirm(confirmation[action]))return;
 submitting=true;buttons.forEach(b=>b.disabled=true);if(run)run.disabled=true;if(more)more.disabled=true;
 document.querySelector('[data-control-selection]').textContent=action.startsWith('update_')?'Checking the selected image and waiting for this workload to stop before replacement…':'Submitting action…';
 const form=document.createElement('form');form.method='POST';form.action=['prepare','terminate'].includes(action)?row.dataset[action+'Url']:row.dataset.actionUrl.replace('ACTION',action);
 const csrf=document.createElement('input');csrf.type='hidden';csrf.name='csrf';csrf.value=document.getElementById('control-csrf').value;form.append(csrf);document.body.append(form);form.submit();
}
const imageDialog=document.getElementById('flow-image-dialog'), imageForm=imageDialog?.querySelector('form'), imageSelect=imageForm?.elements.image_release_id, imageSubmit=imageDialog?.querySelector('[data-image-submit]');
let imageRequest=0, imageRows=[], imageRow=null, imageAction='';
function imageSelection(){
 const release=imageRows.find(r=>r.id===imageSelect.value), status=imageDialog.querySelector('[data-image-status]');
 imageDialog.querySelector('[data-image-details]').hidden=!release;imageSubmit.disabled=!release;
 if(release){imageDialog.querySelector('[data-image-repository]').textContent=release.repository;imageDialog.querySelector('[data-image-created]').textContent=release.created_at;imageDialog.querySelector('[data-image-digest]').textContent=release.digest;status.textContent='The selected digest is verified again before the workload stops.';}
}
async function openImages(row,action){
 const requestId=++imageRequest, role=action==='update_writer'?'processor':'debezium', label=role==='processor'?'History writer':'Debezium';imageRow=row;imageAction=action;imageRows=[];
 imageForm.action=row.dataset.actionUrl.replace('ACTION',action);imageForm.elements.expected_instance_id.value='';
 imageDialog.querySelector('#flow-image-title').textContent='Update '+label+' image';imageDialog.querySelector('[data-image-flow]').textContent='Flow: '+row.querySelector('[data-flow-name]').textContent;
 imageDialog.querySelector('[data-image-current]').textContent='Loading…';imageDialog.querySelector('[data-image-details]').hidden=true;
 imageSelect.replaceChildren(new Option('Loading releases…',''));imageSelect.disabled=true;imageSubmit.disabled=true;imageSubmit.textContent='Update '+label+' image';
 const status=imageDialog.querySelector('[data-image-status]');status.textContent='Loading completed releases from the configured private repository…';
 imageDialog.querySelector('[data-image-impact]').textContent=role==='processor'?'This stops and replaces only the history writer using existing checkpoints. Debezium keeps its current state and may continue adding queued records. The old stopped instance is retained.':'This stops and replaces only Debezium using existing offsets and schema history. The history writer keeps its current state. The old stopped instance is retained.';
 imageDialog.showModal();
 try{
  const response=await fetch(row.dataset.imageChoicesUrl.replace('ROLE',role),{headers:{Accept:'application/json'},credentials:'same-origin',cache:'no-store'});
  if(!response.headers.get('content-type')?.includes('application/json'))throw new Error('Your session may have expired. Reload the page and sign in again.');
  const data=await response.json();if(!response.ok)throw new Error(data.message||data.error||'Image releases could not be loaded.');
  if(requestId!==imageRequest||!imageDialog.open)return;
  imageForm.elements.expected_instance_id.value=data.instance_id;imageDialog.querySelector('[data-image-current]').textContent=data.current_images.join('\n')||'Unavailable';
  imageRows=data.releases.filter(r=>!data.current_images.includes(r.image_url));imageSelect.replaceChildren(new Option(imageRows.length?'Choose a published release':'No different published releases available',''));
  for(const release of imageRows)imageSelect.add(new Option(release.tag+(release.default?' · Workload default':''),release.id));
  imageSelect.disabled=!imageRows.length;status.textContent=imageRows.length?'Choose the exact release to deploy to this flow.':'Build and publish a different release for this component in Container Registry, then reopen this dialog.';
 }catch(error){if(requestId!==imageRequest||!imageDialog.open)return;imageSelect.replaceChildren(new Option('Releases unavailable',''));status.textContent=error.message;}
}
imageSelect?.addEventListener('change',imageSelection);
imageDialog?.addEventListener('close',()=>{imageRequest++;});
imageForm?.addEventListener('submit',event=>{
 const release=imageRows.find(r=>r.id===imageSelect.value);
 if(submitting||!release||!imageForm.reportValidity()||!allowed(imageRow,imageAction)){event.preventDefault();return;}
 submitting=true;imageSubmit.disabled=true;buttons.forEach(b=>b.disabled=true);if(run)run.disabled=true;if(more)more.disabled=true;
 imageDialog.querySelector('[data-image-status]').textContent='Verifying '+release.tag+' and waiting for this workload to stop before replacement…';
});
buttons.forEach(b=>b.addEventListener('click',()=>act(b.dataset.flowAction)));more?.addEventListener('change',update);run?.addEventListener('click',()=>act(more.value));
table.addEventListener('table:selection-change',update);window.addEventListener('pageshow',()=>{submitting=false;if(more)more.disabled=false;update();});
document.addEventListener('flow:status',event=>{health.set(String(event.detail.id),event.detail);update();});update();
})();
