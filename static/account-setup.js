'use strict';
(() => {
const dialog=document.getElementById('connection-dialog');if(!dialog)return;
const form=dialog.querySelector('form'),toggle=form.querySelector('[data-create-account]'),section=form.querySelector('[data-account-section]'),field=name=>form.elements.namedItem(name),save=form.querySelector(':scope > .dialog-actions .primary'),create=form.querySelector('[data-account-create]'),result=form.querySelector('[data-account-result]'),schemas=form.querySelector('[data-account-schemas]');
let busy=false,vaultVersion=0,keyVersion=0,schemaVersion=0;
const id=()=>crypto.randomUUID().replaceAll('-','');
async function request(url,body){const response=await fetch(url,{method:body?'POST':'GET',body,headers:{Accept:'application/json'},credentials:'same-origin',cache:'no-store'});if(!response.headers.get('content-type')?.includes('application/json'))throw Error('Sign in again and reopen Add connection.');const data=await response.json();if(!response.ok)throw Error(data.message||'Account setup could not complete.');return data;}
function setOptions(select,rows,label){select.replaceChildren(new Option(label,''));for(const row of rows)select.add(new Option(row.name,row.id));select.disabled=false;}
function selection(){return [...schemas.querySelectorAll('input:checked')].map(input=>input.value);}
function update(){
 const role=field('role').value,editing=!!form.dataset.editing,enabled=toggle.checked&&!editing;
 form.querySelector('[data-account-toggle]').hidden=editing;section.hidden=!enabled;
 section.querySelectorAll('input,select,button').forEach(input=>input.disabled=!enabled||busy);
 form.querySelector('[data-account-maintenance]').hidden=role!=='target';field('account_maintenance').disabled=!enabled||busy||role!=='target';
 form.querySelector('[data-account-toggle-label]').textContent=role==='source'?'Create Debezium user':'Create history writer user';
 form.querySelector('[data-connection-user-label]').textContent=enabled?'Setup administrator username *':'Database username *';
 form.querySelector('[data-connection-secret-label]').textContent=enabled?'Administrator password secret *':'Vault password secret *';
 if(!editing){save.disabled=enabled||busy;for(const name of ['name','role','host','port','database_name','username','secret_ocid'])field(name).disabled=busy;}dialog.querySelectorAll('[data-close]').forEach(button=>button.disabled=busy);
 if(!field('account_username').value)field('account_username').value=role==='source'?'debezium_reader':'history_writer';
 if(!field('account_setup_id').value)field('account_setup_id').value=id();
 const chosen=selection();create.disabled=!enabled||busy||!chosen.length||!field('account_vault_id').value||!field('account_key_id').value;
 form.querySelector('[data-account-grants]').textContent=role==='source'?'Global: REPLICATION SLAVE, REPLICATION CLIENT, SHOW DATABASES, FLUSH_TABLES. Selected databases only: SELECT, LOCK TABLES. No SUPER, CREATE USER or GRANT OPTION. Replication access itself is global; table filters do not restrict what the binlog account can read.':'Selected databases only: SELECT, INSERT, UPDATE, DELETE, CREATE. CREATE supports audit and checkpoint/schema-history preparation. Optional partition maintenance adds ALTER and DROP. No global, replication, user-management or grant-option privileges.';
}
function resetIntent(){if(busy)return;field('account_setup_id').value=id();result.textContent='';}
function resetSchemas(){schemaVersion++;resetIntent();schemas.replaceChildren();field('account_password').value='';update();}
function adminBody(){for(const name of ['name','host','port','database_name','username','secret_ocid'])if(!field(name).reportValidity()||!field(name).value)throw Error('Complete the administrator connection fields above.');return new FormData(form);}
async function loadVaults(){const current=++vaultVersion;try{const data=await request('/configuration/options/vault');if(current!==vaultVersion||!toggle.checked)return;setOptions(field('account_vault_id'),data.options,'Choose a Vault');update();}catch(error){result.textContent=error.message;}}
async function loadKeys(){resetIntent();const current=++keyVersion,vault=field('account_vault_id').value;setOptions(field('account_key_id'),[],'Choose an enabled AES key');if(!vault){update();return;}try{const data=await request('/configuration/options/key?vault_id='+encodeURIComponent(vault));if(current!==keyVersion)return;setOptions(field('account_key_id'),data.options,'Choose an enabled AES key');update();}catch(error){result.textContent=error.message;}}
toggle.addEventListener('change',()=>{resetIntent();update();if(toggle.checked)loadVaults();});
field('role').addEventListener('change',()=>{field('account_username').value='';field('account_maintenance').checked=false;resetSchemas();update();});
for(const name of ['host','port','database_name','username','secret_ocid'])field(name).addEventListener('input',resetSchemas);
for(const name of ['account_username','account_host','account_maintenance','account_password','account_key_id'])field(name).addEventListener('change',()=>{resetIntent();update();});
field('account_vault_id').addEventListener('change',loadKeys);
schemas.addEventListener('change',()=>{resetIntent();update();});
form.querySelector('[data-account-load]').addEventListener('click',async()=>{
 try{const body=adminBody(),current=++schemaVersion;result.textContent='Connecting as administrator and loading accessible databases…';const data=await request('/connections/account-schemas',body);if(current!==schemaVersion)return;schemas.replaceChildren();for(const name of data.schemas){const label=document.createElement('label');label.className='checkbox-field';const checkbox=document.createElement('input');checkbox.type='checkbox';checkbox.name='account_schemas';checkbox.value=name;checkbox.checked=name===field('database_name').value;label.append(checkbox,document.createTextNode(name));schemas.append(label);}result.textContent=data.schemas.length?'Select databases for the dedicated account.':'No eligible databases are accessible.';update();}catch(error){result.textContent=error.message;}
});
create.addEventListener('click',async()=>{
 if(busy)return;
 try{
  const body=adminBody();for(const name of ['account_username','account_host','account_vault_id','account_key_id'])if(!field(name).value||!field(name).reportValidity())throw Error('Complete all dedicated-account fields.');
  if(field('account_password').value&&!field('account_password').reportValidity())return;
  if(!selection().includes(field('database_name').value))throw Error('Select the connection default database as well as any additional databases.');
  busy=true;update();result.textContent='Storing the password in Vault, creating the dedicated account and verifying its grants and login…';
  const data=await request('/connections/create-account',body);
  field('username').value=data.username;window.connectionSecrets.select(data.secret_ocid,'Dedicated account password');toggle.checked=false;
  form.querySelector('[data-connection-result]').textContent=data.message+' Selected databases: '+data.schemas.join(', ')+'.';
  form.querySelector('[data-connection-result]').dataset.state='success';
 }catch(error){result.textContent=error.message;}
 finally{field('account_password').value='';busy=false;update();}
});
form.addEventListener('submit',event=>{if(toggle.checked||busy){event.preventDefault();result.textContent='Create and test the dedicated account before registering this connection.';}});
form.addEventListener('reset',()=>{queueMicrotask(()=>{vaultVersion++;keyVersion++;schemaVersion++;busy=false;result.textContent='';schemas.replaceChildren();update();});});
dialog.addEventListener('close',()=>{field('account_password').value='';});
dialog.addEventListener('cancel',event=>{if(busy)event.preventDefault();});
new MutationObserver(update).observe(form,{attributes:true,attributeFilter:['data-editing']});update();
})();
