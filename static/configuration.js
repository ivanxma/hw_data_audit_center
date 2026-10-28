'use strict';
(()=>{
const get=async url=>{const response=await fetch(url,{headers:{Accept:'application/json'}});let data;try{data=await response.json();}catch{throw Error('Sign in again or check service availability.');}if(!response.ok)throw Object.assign(Error(data.message||'Service access failed.'),{policy:data.policy,requestId:data.request_id});return data;};
function options(select,rows,label='Select…'){select.replaceChildren(new Option(label,''));for(const row of rows){const option=new Option(row.name||row.id,row.id||row.name);option.disabled=!!row.disabled;select.add(option);}select.disabled=false;}
let poolOptionsPromise;
function streamPools(){if(!poolOptionsPromise)poolOptionsPromise=get('/configuration/options/stream_pool').catch(error=>{poolOptionsPromise=null;throw error;});return poolOptionsPromise;}
const dialog=document.getElementById('flow-dialog');
const mappingLoads={};
async function loadMappingObjects(role,selected={}){
 if(!dialog)return;
 const form=dialog.querySelector('form'),field=n=>form.elements.namedItem(n),status=form.querySelector('[data-mapping-status]');
 const connection=field(role+'_id'),schema=field(role+'_schema'),table=field(role+'_table');
 const version=(mappingLoads[role]||0)+1;mappingLoads[role]=version;
 const id=connection.value;
 options(schema,[],id?'Loading schemas…':'Select a connection first');schema.disabled=true;
 options(table,[],'Select a schema first');table.disabled=true;
 if(!id)return;
 try{
  const data=await get(connection.selectedOptions[0].dataset.objectsUrl);
  if(mappingLoads[role]!==version)return;
  options(schema,data.schemas.map(name=>({name})));
  if(role==='source')schema.add(new Option('All eligible databases (*)','*'),1);
  if(selected.schema){schema.value=selected.schema;await loadMappingTables(role,selected.table);}
 }catch(e){status.textContent=e.message;throw e;}
}
async function loadMappingTables(role,selected=''){
 const form=dialog.querySelector('form'),field=n=>form.elements.namedItem(n),status=form.querySelector('[data-mapping-status]');
 const connection=field(role+'_id'),schema=field(role+'_schema'),table=field(role+'_table'),id=connection.value,name=schema.value;
 if(role==='source'&&name==='*'){options(table,[{id:'*',name:'All eligible tables (*)'}]);table.value='*';return;}
 options(table,[],'Loading tables…');table.disabled=true;if(!id||!name){options(table,[],'Select a schema first');table.disabled=true;return;}
 try{
  const data=await get(connection.selectedOptions[0].dataset.objectsUrl+'?schema='+encodeURIComponent(name));
  if(connection.value!==id||schema.value!==name)return;
  options(table,role==='target'?data.tables.filter(t=>t.compatible):data.tables);
  if(role==='source')table.add(new Option('All tables in this database (*)','*'),1);
  if(role==='target')table.add(new Option('Create new audit table','__new__'),1);
  if(selected){
   table.value=selected;
   if(!table.value){status.textContent='The saved '+role+' table is not available. Choose an existing table'+(role==='target'?' or Create new audit table.':'.');}
  }
 }catch(e){status.textContent=e.message;throw e;}
}
let queueVersion=0;
async function loadMappingQueue(selected={}){
 if(!dialog)return;
 const form=dialog.querySelector('form'),field=n=>form.elements.namedItem(n),status=form.querySelector('[data-mapping-status]');
 const version=++queueVersion,queue=field('stream_pool_id'),topic=field('topic');
 const id=queue.value;
 options(topic,[],id?'Loading OCI streams…':'Select a stream pool first');topic.disabled=true;status.textContent='';
 if(!id)return;
 try{
  const data=await get('/stream-pools/'+encodeURIComponent(id)+'/topics');if(queueVersion!==version)return;
  const active=data.topics.filter(stream=>stream.state==='ACTIVE');
  const choices=active.map(stream=>({id:stream.name,name:stream.name+(stream.schema_stream?' — schema history':stream.assigned_flow?' — assigned to '+stream.assigned_flow:''),disabled:stream.schema_stream||!!stream.assigned_flow&&stream.name!==selected.topic}));
  const available=choices.some(stream=>!stream.disabled);
  options(topic,choices,available?'Select a data stream':'No unassigned data streams');topic.disabled=!available;
  if(selected.topic)topic.value=selected.topic;
  if(!available)status.textContent='Create a separate data stream in OCI resources, then select this pool again. Each flow owns its data stream; audit_<flow ID> streams store schema history.';
 }catch(error){if(queueVersion===version){status.textContent=error.message;throw error;}}

}
if(dialog){
 const form=dialog.querySelector('form'),field=n=>form.elements.namedItem(n);
 for(const role of ['source','target']){
  field(role+'_id').addEventListener('change',()=>loadMappingObjects(role).catch(()=>{}));
  field(role+'_schema').addEventListener('change',()=>loadMappingTables(role).catch(()=>{}));
 }
 field('stream_pool_id').addEventListener('change',()=>loadMappingQueue().catch(()=>{}));
}
async function loadReference(select){const requestId=String(Number(select.dataset.requestId||0)+1);select.dataset.requestId=requestId;if(select.closest('[data-pull-secret], [data-queue-credential]')?.hidden)return;const type=select.dataset.resourceOptions;let url='/configuration/options/'+type;if(type==='mysql_shape'||type==='mysql_configuration'){const form=select.closest('form'),parent=type==='mysql_shape'?'availability_domain':'shape_name',value=form.elements.namedItem(parent).value;if(!value){options(select,[],type==='mysql_shape'?'Select an availability domain first':'Select a shape first');select.disabled=true;return;}url+='?'+parent+'='+encodeURIComponent(value);}if(type==='image'){const repository=select.closest('form').elements.namedItem('repository_id')?.value;if(!repository){options(select,[],'Select a repository first');return;}url+='?repository_id='+encodeURIComponent(repository);}if(type==='key'){const vault=select.closest('form').elements.namedItem('vault_id')?.value;if(!vault){options(select,[],'Select a vault first');return;}url+='?vault_id='+encodeURIComponent(vault);}const data=type==='stream_pool'?await streamPools():await get(url);if(select.dataset.requestId!==requestId||select.closest('[data-queue-credential]')?.hidden)return;if(type==='mysql_shape'||type==='mysql_configuration'){const parent=type==='mysql_shape'?'availability_domain':'shape_name';if(new URL(url,location.origin).searchParams.get(parent)!==select.closest('form').elements.namedItem(parent).value)return;}options(select,type==='stream_pool'?data.options.filter(item=>item.state==='ACTIVE'):data.options);if(select.closest('[data-pull-secret]')?.hidden)select.disabled=true;if(select.dataset.selected){select.value=select.dataset.selected;if(select.name==='repository_id')select.dispatchEvent(new Event('change'));}}
document.querySelectorAll('[data-fingerprint-setup] [data-resource-options="vault"]').forEach(select=>loadReference(select).catch(e=>{options(select,[],e.message);select.closest('form').querySelector('[data-fingerprint-status]').textContent=e.message;}));
let scope=null;
async function loadScope(){if(scope)return scope;scope=await get('/configuration/scope');const select=document.getElementById('resource-compartment');if(select){options(select,scope.compartments);select.value=scope.selected;}document.querySelectorAll('[data-compartment-value]').forEach(input=>input.value=scope.selected);return scope;}
function renderResources(panel,kind,resources,placeholder){
const holder=panel.querySelector('[data-service-table]');
panel.querySelectorAll('.table-toolbar-left [data-dialog],.table-toolbar-left [data-service-refresh]').forEach(action=>panel.querySelector('.panel-heading').append(action));
const scroll=document.createElement('div');scroll.className='table-scroll';
const table=document.createElement('table');table.id='resource-'+kind;table.dataset.selectable='true';table.dataset.deleteUrl='/configuration/services/'+kind+'/delete';table.dataset.deleteLabel='OCI resources';table.dataset.requireManaged='true';if(kind==='heatwave')table.dataset.lifecycleUrl='/configuration/services/heatwave/actions/';
const header=table.createTHead().insertRow();for(const title of ['Name','State','Managed by this app','Resource ID']){const cell=document.createElement('th');cell.textContent=title;header.append(cell);}
const body=table.createTBody();for(const row of resources){const tr=body.insertRow();tr.dataset.rowId=row.id;tr.dataset.managed=String(row.managed);tr.dataset.state=row.state;for(const value of [row.name,row.state,row.managed?'Yes':'No',row.id])tr.insertCell().textContent=value;}
if(!resources.length){const cell=body.insertRow().insertCell();cell.colSpan=4;cell.className='table-empty';cell.textContent=placeholder;}
scroll.append(table);holder.replaceChildren(scroll);window.initializeTables?.(panel);
}
async function loadService(kind){
const panel=document.getElementById('service-panel-'+kind);if(!panel)return;
const version=String(Number(panel.dataset.loadVersion||0)+1);panel.dataset.loadVersion=version;
const message=panel.querySelector('[data-service-message]');message.textContent='Loading resources in the VM compartment…';message.classList.remove('notice','warning');
renderResources(panel,kind,[],'Loading…');
try{
 await loadScope();const data=await get('/configuration/services/'+kind);if(panel.dataset.loadVersion!==version)return;
 renderResources(panel,kind,data.resources,'No resources in this compartment.');message.textContent='';
}catch(error){
 if(panel.dataset.loadVersion!==version)return;
 renderResources(panel,kind,[],'Resource list unavailable. Resolve the error above, then Refresh.');
 message.classList.add('notice','warning');const text=document.createElement('p');text.textContent=error.message;message.replaceChildren(text);
 if(error.policy){const details=document.createElement('details'),summary=document.createElement('summary'),policy=document.createElement('pre');summary.textContent='Required IAM policy for the VM dynamic group';policy.textContent=error.policy;details.append(summary,policy);message.append(details);}
 if(error.requestId){const request=document.createElement('p');request.textContent='OCI request ID: '+error.requestId;message.append(request);}
}
}
const groups=document.querySelector('[data-service-tabs]');
function serviceTab(name){groups?.querySelectorAll('[data-service-tab]').forEach(button=>{const active=button.dataset.serviceTab===name;button.classList.toggle('active',active);button.setAttribute('aria-selected',String(active));});document.querySelectorAll('[data-service-panel]').forEach(panel=>panel.hidden=panel.dataset.servicePanel!==name);document.querySelectorAll('[data-service-panel="'+name+'"] [data-service-refresh]').forEach(button=>loadService(button.dataset.serviceRefresh));if(name==='registry')loadBuilds().catch(()=>{});if(name==='streaming')loadInventoryPool().catch(()=>{});if(name==='containers')document.querySelectorAll('.workload-settings [data-resource-options]').forEach(select=>loadReference(select).catch(e=>options(select,[],e.message)));}
if(groups){groups.addEventListener('click',event=>{const button=event.target.closest('[data-service-tab]');if(button)serviceTab(button.dataset.serviceTab);});document.querySelector('[data-tab="resources"]')?.addEventListener('click',()=>serviceTab(groups.querySelector('[aria-selected="true"]')?.dataset.serviceTab||'streaming'));if(new URLSearchParams(location.search).get('tab')==='resources'){const kind=new URLSearchParams(location.search).get('service');const panel=document.getElementById('service-panel-'+kind);serviceTab(kind==='queues'?'streaming':panel?.closest('[data-service-panel]')?.dataset.servicePanel||'streaming');}}
document.addEventListener('click',event=>{const refresh=event.target.closest('[data-service-refresh]');if(refresh)loadService(refresh.dataset.serviceRefresh);const opener=event.target.closest('[data-dialog]');if(opener){const target=document.getElementById(opener.dataset.dialog);if(!target)return;target.querySelectorAll('[data-resource-options]').forEach(select=>loadReference(select).catch(e=>{options(select,[],e.message);}));if(target.querySelector('[data-compartment-value]'))loadScope().catch(e=>alert(e.message));}});
document.querySelectorAll('select[name="vault_id"]').forEach(select=>select.addEventListener('change',()=>{const key=select.closest('form').querySelector('[data-resource-options="key"]');if(key)loadReference(key).catch(e=>options(key,[],e.message));}));
async function prefillMappingPool(values){const pool=dialog.querySelector('[name=stream_pool_id]');await loadReference(pool);pool.value=values.stream_pool_id||'';}
window.configurationForms={
 cancelLoads(){queueVersion++;for(const role of ['source','target'])mappingLoads[role]=(mappingLoads[role]||0)+1;document.querySelectorAll('#flow-dialog [data-resource-options]').forEach(select=>select.dataset.requestId=String(Number(select.dataset.requestId||0)+1));},
 async prefillMapping(values){
  await prefillMappingPool(values);
  const results=await Promise.allSettled([
   loadMappingObjects('source',{schema:values.source_schema,table:values.source_table}),
   loadMappingObjects('target',{schema:values.target_schema,table:values.target_table}),
   loadMappingQueue({topic:values.topic})
  ]);
  const failure=results.find(result=>result.status==='rejected');if(failure)throw failure.reason;
 },
 prefillPool:prefillMappingPool
};
document.querySelectorAll('.workload-settings [name=repository_id]').forEach(select=>select.addEventListener('change',()=>{const image=select.closest('form').querySelector('[data-resource-options=image]');loadReference(image).catch(e=>options(image,[],e.message));}));
const topicQueue=document.getElementById('topic-inventory-pool');
async function loadInventoryPool(){const pool=document.getElementById('topic-inventory-pool');if(!pool||pool.dataset.loaded)return;await loadReference(pool);pool.dataset.loaded='true';const selected=new URLSearchParams(location.search).get('pool_id');if(selected&&[...pool.options].some(option=>option.value===selected)){pool.value=selected;pool.dispatchEvent(new Event('change'));}}
if(topicQueue)topicQueue.addEventListener('change',async()=>{const id=topicQueue.value,status=document.querySelector('[data-topic-status]'),holder=document.getElementById('topic-inventory');holder.replaceChildren();status.textContent='';if(!id)return;status.textContent='Loading streams…';try{const data=await get('/stream-pools/'+encodeURIComponent(id)+'/topics');if(topicQueue.value!==id)return;const scroll=document.createElement('div');scroll.className='table-scroll';const table=document.createElement('table');table.id='managed-topics';table.dataset.selectable='true';table.dataset.deleteUrl='/queue-topics/delete?pool_id='+encodeURIComponent(id);table.dataset.deleteLabel='streams';const head=table.createTHead().insertRow();for(const label of ['Name','Partitions','State']){const cell=document.createElement('th');cell.textContent=label;head.append(cell);}const body=table.createTBody();for(const topic of data.topics){const row=body.insertRow();row.dataset.rowId=topic.name;for(const value of [topic.name,topic.partitions,topic.state])row.insertCell().textContent=value;}if(!data.topics.length){const cell=body.insertRow().insertCell();cell.colSpan=3;cell.textContent='No streams available.';}scroll.append(table);holder.append(scroll);window.initializeTables?.(holder);status.textContent='';}catch(e){if(topicQueue.value===id)status.textContent=e.message;}});

document.querySelector('[data-topics-refresh]')?.addEventListener('click',()=>topicQueue?.dispatchEvent(new Event('change')));
if(topicQueue){const selected=new URLSearchParams(location.search).get('pool_id');if(selected&&Array.from(topicQueue.options).some(option=>option.value===selected)){topicQueue.value=selected;topicQueue.dispatchEvent(new Event('change'));}}

let buildSignature='';
async function loadBuilds(){const panel=document.getElementById('image-build-panel');if(!panel||panel.closest('[data-service-panel]').hidden)return;const status=panel.querySelector('[data-build-status]');try{const data=await get('/configuration/image-builds');status.textContent=data.worker_available?'Builds run on this OL9 VM and upload to the selected private OCI Registry repository.':'Image builder unavailable. Start heatwave-audit-image-builder.service on the VM.';const signature=JSON.stringify(data.builds);if(signature===buildSignature)return;buildSignature=signature;const holder=panel.querySelector('[data-build-table]');panel.querySelectorAll('.table-toolbar-left [data-dialog], .table-toolbar-left [data-build-refresh]').forEach(button=>panel.querySelector('.panel-heading').append(button));const scroll=document.createElement('div');scroll.className='table-scroll';const table=document.createElement('table');table.id='image-builds';table.dataset.selectable='true';table.dataset.logDownload='true';const head=table.createTHead().insertRow();for(const title of ['Release','Component','Repository','Status','Created (UTC)','Message','Image address']){const th=document.createElement('th');th.textContent=title;head.append(th);}const body=table.createTBody();for(const build of data.builds){const row=body.insertRow();row.dataset.rowId=build.id;row.dataset.logUrl=build.log_url;for(const value of [build.tag,build.role==='processor'?'History writer':'Debezium',build.repository_name,build.status,build.created_at,build.message,build.image_url])row.insertCell().textContent=value;}if(!data.builds.length){const cell=body.insertRow().insertCell();cell.colSpan=7;cell.textContent='No image builds yet. Choose Build & publish to create a release.';}scroll.append(table);holder.replaceChildren(scroll);window.initializeTables?.(panel);}catch(e){status.textContent=e.message;}}
document.querySelector('[data-build-refresh]')?.addEventListener('click',()=>{buildSignature='';loadBuilds();});
setInterval(()=>{if(!document.hidden)loadBuilds();},5000);
document.querySelectorAll('[data-pull-auth]').forEach(select=>{const sync=()=>{const label=select.closest('form').querySelector('[data-pull-secret]'),secret=label.querySelector('select');label.hidden=select.value!=='vault';secret.disabled=label.hidden;secret.required=!label.hidden;if(!label.hidden)loadReference(secret).catch(e=>options(secret,[],e.message));};select.addEventListener('change',sync);sync();});

const heatwaveForm=document.querySelector('#create-heatwave form');
if(heatwaveForm){const domain=heatwaveForm.elements.namedItem('availability_domain'),shape=heatwaveForm.elements.namedItem('shape_name'),config=heatwaveForm.elements.namedItem('configuration_id');domain.addEventListener('change',()=>{options(shape,[],'Loading available shapes…');shape.disabled=true;options(config,[],'Select a shape first');config.disabled=true;loadReference(shape).catch(e=>options(shape,[],e.message));});shape.addEventListener('change',()=>{options(config,[],'Loading matching configurations…');config.disabled=true;loadReference(config).catch(e=>options(config,[],e.message));});}
})();
