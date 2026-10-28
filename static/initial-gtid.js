'use strict';
(() => {
  const dialog=document.getElementById('flow-dialog');
  if(!dialog)return;
  const form=dialog.querySelector('form'),mode=form.elements.namedItem('capture_mode');
  const panel=form.querySelector('[data-initial-gtid]'),input=form.elements.namedItem('capture_gtid');
  const result=panel.querySelector('[data-gtid-result]'),button=panel.querySelector('[data-gtid-check]');
  let version=0;
  const reset=()=>{version++;result.textContent='';button.disabled=false;};
  const sync=()=>{
    const selected=mode.value==='gtid';panel.hidden=!selected;
    input.disabled=!selected||mode.disabled;input.required=selected&&!mode.disabled;
    reset();
  };
  mode.addEventListener('change',sync);
  input.addEventListener('input',reset);
  form.elements.namedItem('source_id').addEventListener('change',reset);
  form.addEventListener('reset',()=>queueMicrotask(sync));
  new MutationObserver(sync).observe(dialog,{attributes:true,attributeFilter:['open']});
  new MutationObserver(sync).observe(mode,{attributes:true,attributeFilter:['disabled']});
  button.addEventListener('click',async()=>{
    const source=form.elements.namedItem('source_id').value;
    if(!source||!input.value.trim()){result.textContent='Select a source and enter the complete executed GTID set.';return;}
    const current=++version;button.disabled=true;result.textContent='Checking source GTID range and schema compatibility…';
    try{
      const body=new FormData();body.set('csrf',form.elements.namedItem('csrf').value);body.set('source_id',source);body.set('gtid',input.value);
      const response=await fetch(button.dataset.url,{method:'POST',headers:{Accept:'application/json'},credentials:'same-origin',body});
      if(response.redirected)throw Error('Sign in again to check the source.');
      const data=await response.json();
      if(current!==version)return;
      const range=data.source_range||data.boundary?.source_range;
      const info=range?'\n\nPurged GTIDs: '+(range.purged||'(empty)')+'\nExecuted GTIDs: '+range.executed+'\n\nThe supplied set must include all purged transactions and contain no unexecuted transactions.':'';
      result.textContent=(response.ok?'Valid retained boundary. Capture starts after the supplied set. The source is checked again at deployment.':data.message||'GTID check failed.')+info;
    }catch(error){if(current===version)result.textContent=error.message;}
    finally{if(current===version)button.disabled=false;}
  });
  sync();
})();
