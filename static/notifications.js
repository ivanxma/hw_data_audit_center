'use strict';
(() => {
const menu=document.querySelector('[data-notifications]');if(!menu)return;
const trigger=menu.querySelector('summary'),list=menu.querySelector('[data-notification-list]'),count=menu.querySelector('[data-notification-count]'),empty=menu.querySelector('[data-notification-empty]'),status=menu.querySelector('[data-notification-status]'),clear=menu.querySelector('[data-clear-notifications]');
let generation=0,stopped=false;
function render(items){
 list.replaceChildren();count.textContent=String(items.length);count.hidden=!items.length;clear.disabled=!items.length;empty.hidden=!!items.length;
 trigger.setAttribute('aria-label',items.length?'Notifications, '+items.length+' messages':'Notifications');
 for(const item of items){
  const row=document.createElement('li');row.className='notification-item';
  const meta=document.createElement('div');meta.className='notification-meta';
  const kind=document.createElement('span');kind.className='notification-kind '+(['error','success','warning'].includes(item.category)?item.category:'info');kind.textContent={error:'Error',success:'Success',warning:'Attention',info:'Information'}[item.category]||'Information';
  const time=document.createElement('time');const date=new Date(item.created_at*1000);time.dateTime=date.toISOString();time.textContent=date.toLocaleString();
  const message=document.createElement('p');message.textContent=item.message;meta.append(kind,time);row.append(meta,message);list.append(row);
 }
}
async function refresh(clearAll=false){
 if(stopped)return;const version=++generation;if(clearAll)clear.disabled=true;
 try{
  const options={headers:{Accept:'application/json'},credentials:'same-origin'};
  if(clearAll){options.method='POST';options.body=new URLSearchParams({csrf:menu.dataset.csrf});}
  const response=await fetch(clearAll?'/notifications/clear':'/notifications',options);
  if(response.redirected||response.status===401){stopped=true;throw Error('Sign in again to view notifications.');}
  if(!response.ok)throw Error('Notifications are unavailable. Please try again.');
  const data=await response.json();if(version!==generation)return;render(data.items);status.textContent='';if(clearAll)trigger.focus();
 }catch(error){if(version===generation){status.textContent=error.message;clear.disabled=!list.children.length;}}
}
clear.addEventListener('click',()=>refresh(true));
menu.addEventListener('toggle',()=>{trigger.setAttribute('aria-expanded',String(menu.open));if(menu.open)refresh();});
document.addEventListener('click',event=>{if(menu.open&&!menu.contains(event.target))menu.open=false;});
document.addEventListener('keydown',event=>{if(event.key==='Escape'&&menu.open&&!document.querySelector('dialog[open]')){menu.open=false;trigger.focus();}});
refresh();setInterval(()=>{if(!document.hidden)refresh();},15000);
})();
