'use strict';
document.addEventListener('click',event=>{
const button=event.target.closest('[data-dismiss-flash]');if(!button)return;
const message=button.closest('[data-flash-message]');if(!message)return;
const restoreFocus=message.contains(document.activeElement);message.remove();
if(restoreFocus)(document.querySelector('[data-dismiss-flash]')||document.querySelector('[data-notifications] summary'))?.focus({preventScroll:true});
});
document.querySelectorAll('[data-dialog]').forEach(b=>b.addEventListener('click',()=>document.getElementById(b.dataset.dialog).showModal()));
document.querySelectorAll('[data-close]').forEach(b=>b.addEventListener('click',()=>b.closest('dialog').close()));
document.querySelectorAll('[data-confirm]').forEach(f=>f.addEventListener('submit',e=>{if(!confirm(f.dataset.confirm))e.preventDefault();}));
const tabs=[...document.querySelectorAll('[data-tab]')];
function tab(name){
tabs.forEach(b=>{const active=b.dataset.tab===name;b.classList.toggle('active',active);b.setAttribute('aria-selected',String(active));b.tabIndex=active?0:-1;});
document.querySelectorAll('.tab-content').forEach(p=>p.hidden=p.id!==name);
document.querySelectorAll('[data-tab-selection]').forEach(input=>input.value=name);
if(document.querySelector('[data-tabs-persist]')){
const url=new URL(location.href);url.searchParams.set('tab',name);history.replaceState(history.state,'',url);
document.querySelectorAll('a[data-preserve-tab]').forEach(link=>{const target=new URL(link.href);target.searchParams.set('tab',name);link.href=target.href;});
}
}
if(tabs.length){
tabs.forEach(b=>{b.id=b.id||'tab-'+b.dataset.tab;b.setAttribute('aria-controls',b.dataset.tab);document.getElementById(b.dataset.tab)?.setAttribute('aria-labelledby',b.id);});
const selected=new URLSearchParams(location.search).get('tab');tab(tabs.some(b=>b.dataset.tab===selected)?selected:tabs[0].dataset.tab);
tabs.forEach((b,index)=>{
b.addEventListener('click',()=>tab(b.dataset.tab));
b.addEventListener('keydown',event=>{
const next={ArrowRight:(index+1)%tabs.length,ArrowLeft:(index+tabs.length-1)%tabs.length,Home:0,End:tabs.length-1}[event.key];
if(next===undefined)return;event.preventDefault();tab(tabs[next].dataset.tab);tabs[next].focus();
});
});
}
if(new URLSearchParams(location.search).get('new')==='flow')document.getElementById('flow-dialog')?.showModal();
document.querySelectorAll('[data-search]').forEach(input=>input.addEventListener('input',()=>{const s=input.value.toLowerCase();document.querySelectorAll('#'+input.dataset.search+' tbody tr').forEach(r=>r.hidden=!r.textContent.toLowerCase().includes(s));}));
document.querySelectorAll('[data-test-connection]').forEach(button=>button.addEventListener('click',async()=>{
const form=button.closest('form'),result=form.querySelector('[data-connection-result]');
if(!form.reportValidity())return;
button.disabled=true;button.textContent='Testing…';result.textContent='Connecting to MySQL…';
try{const body=new FormData(form);form.querySelectorAll('[data-edit-locked][name]').forEach(field=>body.set(field.name,field.value));const response=await fetch(button.dataset.testConnection,{method:'POST',body,credentials:'same-origin'});const value=await response.json();result.textContent=value.message||'Connection test could not complete.';result.dataset.state=response.ok&&value.ok?'success':'error';}
catch{result.textContent='Connection test could not complete. Check your session and try again.';result.dataset.state='error';}
finally{button.disabled=false;button.textContent='Test connection';}
}));
