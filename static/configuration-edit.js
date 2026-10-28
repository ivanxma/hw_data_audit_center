'use strict';
(() => {
const ids={connection:'connection-dialog',flow:'flow-dialog',queue:'queue-dialog'};
const originals=new Map();
for(const id of Object.values(ids)){
 const dialog=document.getElementById(id);if(!dialog)continue;
 const form=dialog.querySelector('form'),submit=form.querySelector(':scope > .dialog-actions .primary');
 originals.set(id,{
  action:form.getAttribute('action'),title:dialog.querySelector('h2').textContent,submit:submit.innerHTML,
  fields:[...form.elements].filter(field=>field.name).map(field=>({field,disabled:field.disabled,required:field.required,options:field.tagName==='SELECT'?field.innerHTML:null}))
 });
 const syncSave=()=>{if(!form.dataset.editing)return;const disabled=form.dataset.prefilling==='true'||[...form.querySelectorAll('[required]:disabled')].some(field=>!field.hasAttribute('data-edit-locked'));if(submit.disabled!==disabled)submit.disabled=disabled;};
 new MutationObserver(syncSave).observe(form,{subtree:true,attributes:true,attributeFilter:['disabled','data-edit-locked','data-prefilling']});
 form.addEventListener('submit',event=>{
  if(form.dataset.editing&&(submit.disabled||!form.reportValidity()))event.preventDefault();
 });
}
function resetEditor(dialog){
 const saved=originals.get(dialog.id);if(!saved)return;
 const form=dialog.querySelector('form');window.configurationForms?.cancelLoads();if(dialog.id==='connection-dialog')window.connectionSecrets?.cancel();
 form.reset();form.setAttribute('action',saved.action);delete form.dataset.editing;delete form.dataset.prefilling;delete form.dataset.entryId;
 dialog.querySelector('h2').textContent=saved.title;
 const submit=form.querySelector(':scope > .dialog-actions .primary');submit.innerHTML=saved.submit;submit.disabled=false;
 form.querySelectorAll('[data-edit-only]').forEach(node=>node.remove());
 for(const {field,disabled,required,options} of saved.fields){
  if(options!==null)field.innerHTML=options;
  field.disabled=disabled;field.required=required;delete field.dataset.editLocked;
 }
 form.querySelectorAll('[data-connection-result],[data-mapping-status]').forEach(node=>{node.textContent='';delete node.dataset.state;});
}
// Restore Add/Register after closing Edit, including the original POST destination.
document.addEventListener('click',event=>{
 const opener=event.target.closest('[data-dialog]');if(!opener)return;
 const dialog=document.getElementById(opener.dataset.dialog);if(dialog&&originals.has(dialog.id))resetEditor(dialog);
},true);
const node=document.querySelector('[data-edit-entry]');if(!node)return;
const entry=JSON.parse(node.dataset.editEntry),dialog=document.getElementById(ids[entry.kind]);
if(!dialog)return;
const form=dialog.querySelector('form'),submit=form.querySelector(':scope > .dialog-actions .primary');
const field=name=>form.elements.namedItem(name);
form.setAttribute('action',entry.url);form.dataset.editing='true';form.dataset.entryId=String(entry.id);
dialog.querySelector('h2').textContent={connection:'Edit connection',queue:'Edit queue connection',flow:'Edit table mapping'}[entry.kind];
submit.textContent='Save changes';
const version=document.createElement('input');version.type='hidden';version.name='version';version.value=entry.version;version.dataset.editOnly='';form.append(version);
const message=document.createElement('p');message.className='notice';message.dataset.editOnly='';message.setAttribute('role','status');
message.textContent=entry.message||'Update the selected registration. Settings are validated before saving.';
form.querySelector('.dialog-actions').before(message);
for(const [name,value] of Object.entries(entry.values)){
 const input=field(name);if(!input)continue;
 const text=value===null?'':String(value);
 if(input.tagName==='SELECT'&&text&&![...input.options].some(option=>option.value===text))input.add(new Option(text,text));
 input.value=text;
}
function applyLocks(){
 for(const name of entry.locked){const input=field(name);if(input){input.dataset.editLocked='true';input.disabled=true;}}
}
const nameOnly=entry.locked.length===Object.keys(entry.values).length-1;
applyLocks();dialog.showModal();
if(entry.kind==='connection')window.connectionSecrets?.load();
if(nameOnly&&entry.kind==='flow')window.configurationForms.prefillPool(entry.values).catch(error=>{message.textContent=error.message;}).finally(applyLocks);
if(!nameOnly&&(entry.kind==='flow'||entry.kind==='queue')){
 form.dataset.prefilling='true';submit.disabled=true;
 const load=entry.kind==='flow'?window.configurationForms.prefillMapping:window.configurationForms.prefillQueue;
 load(entry.values).catch(error=>{message.textContent=[entry.message,error.message].filter(Boolean).join(' ');}).finally(()=>{
  if(!form.dataset.editing)return;
  applyLocks();form.dataset.prefilling='false';
 });
}
})();
