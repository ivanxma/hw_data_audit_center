'use strict';
(() => {
const dialog=document.getElementById('connection-dialog'),select=dialog?.querySelector('[data-connection-secret]');if(!select)return;
const status=dialog.querySelector('[data-connection-secret-status]');let requestId=0;
const hint='Select an active secret in the VM compartment. Secret values are never listed.';
function selected(value,label){
 requestId++;
 let option=[...select.options].find(option=>option.value===value);
 if(!option){option=new Option(label,value);select.add(option);}
 select.value=value;status.textContent='Dedicated account password saved in Vault and selected for this connection.';
}
async function load(){
 const request=++requestId,value=select.value,saved=select.selectedOptions[0]?.textContent;
 status.textContent='Loading active password secrets from the VM compartment…';
 if(!value)select.replaceChildren(new Option('Loading password secrets…',''));
 try{
  const response=await fetch('/configuration/options/secret',{headers:{Accept:'application/json'},credentials:'same-origin',cache:'no-store'});
  if(!response.headers.get('content-type')?.includes('application/json'))throw Error('Sign in again to load Vault secrets.');
  const data=await response.json();if(!response.ok)throw Error(data.message||'Vault secret inventory could not be loaded.');
  if(request!==requestId)return;
  const rows=data.options.filter(row=>row.state==='ACTIVE');
  select.replaceChildren(new Option(rows.length?'Choose a password secret':'No active secrets in the VM compartment',''));
  for(const row of rows){const option=new Option(row.name,row.id);option.title=row.id;select.add(option);}
  if(value){if(!rows.some(row=>row.id===value))select.add(new Option(saved||'Saved connection secret',value));select.value=value;}
  status.textContent=rows.length?hint:'No active secrets are available. Create a password secret in OCI resources → Vault, then reopen Add connection.';
 }catch(error){
  if(request!==requestId)return;
  if(!value)select.replaceChildren(new Option('Password secrets unavailable',''));
  status.textContent=error.message;
 }
}
window.connectionSecrets={load,select:selected,cancel(){requestId++;status.textContent=hint;}};
document.addEventListener('click',event=>{if(event.target.closest('[data-dialog="connection-dialog"]'))load();});
})();
