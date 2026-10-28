'use strict';
(() => {
const NS='http://www.w3.org/2000/svg';
function icon(name){const svg=document.createElementNS(NS,'svg');svg.classList.add('icon');svg.setAttribute('aria-hidden','true');const use=document.createElementNS(NS,'use');use.setAttribute('href','/static/icons.svg#'+name);svg.append(use);return svg;}
function control(label,fn,iconName){const b=document.createElement('button');b.type='button';b.className=iconName?'icon-button':'button';b.title=label;b.setAttribute('aria-label',label);b.append(iconName?icon(iconName):document.createTextNode(label));b.addEventListener('click',fn);return b;}
function get(key){try{return JSON.parse(localStorage.getItem(key));}catch{return null;}}
function save(key,data){try{localStorage.setItem(key,JSON.stringify(data));}catch{}}
function valueContent(cell){
const copy=cell.cloneNode(true);
copy.querySelectorAll('dialog,[hidden],script,style').forEach(node=>node.remove());
return copy;
}
function fullValue(cell){
if(cell.dataset.fullValue!==undefined)return cell.dataset.fullValue;
const copy=valueContent(cell);
copy.querySelectorAll('small,br,.process-state,.process-block code,.process-block .badge').forEach(node=>node.before('\n'));
return copy.textContent.trim();
}
let valueDialog,valueHeading,valueBody,valueOpener;
function closeValue(){
const opener=valueOpener;valueOpener=null;valueDialog.close();valueBody.textContent='';
if(opener?.isConnected)opener.focus({preventScroll:true});
}
function showValue(cell,header,opener){
if(!valueDialog){
valueDialog=document.createElement('dialog');valueDialog.id='table-value-dialog';valueDialog.className='table-value-dialog';valueDialog.setAttribute('aria-labelledby','table-value-heading');
const heading=document.createElement('div');heading.className='dialog-heading';
valueHeading=document.createElement('h2');valueHeading.id='table-value-heading';
const close=control('Close full value',closeValue);close.className='icon-button';close.textContent='×';
heading.append(valueHeading,close);
valueBody=document.createElement('pre');valueBody.className='table-full-value';valueBody.tabIndex=0;
const actions=document.createElement('div');actions.className='dialog-actions';actions.append(control('Close',closeValue));
valueDialog.append(heading,valueBody,actions);document.body.append(valueDialog);
valueDialog.addEventListener('cancel',event=>{event.preventDefault();closeValue();});
}
valueOpener=opener;
valueHeading.textContent=header.textContent.replace(/[↕↑↓]/g,'').trim()||'Field value';
valueBody.textContent=fullValue(cell);
valueDialog.showModal();valueBody.focus();
}
function enableValueDialog(cell,header){
const content=valueContent(cell);
if(!content.textContent.trim()||content.querySelector('a,button,input,select,textarea,summary,[role="button"],[contenteditable]'))return;
const button=document.createElement('button');button.type='button';button.className='table-cell-value';button.title='View full value';button.setAttribute('aria-haspopup','dialog');button.setAttribute('aria-label','View full value: '+(header.textContent.replace(/[↕↑↓]/g,'').trim()||'Field'));
for(const node of [...cell.childNodes]){if(node.nodeType!==1||!node.matches('dialog,[hidden]'))button.append(node);}
cell.prepend(button);cell.removeAttribute('title');
button.addEventListener('click',event=>{if(event.detail&&!window.getSelection()?.isCollapsed)return;showValue(cell,header,button);});
}
function initializeTables(root=document){root.querySelectorAll('table').forEach((table,index)=>{
if(table.dataset.initialized||table.dataset.deferred)return;table.dataset.initialized='true';
if(!table.tHead||!table.tBodies[0])return;
if(!table.id)table.id='table-'+location.pathname.replace(/[^a-z0-9]/gi,'-')+'-'+index;
table.classList.add('compact-table');
const tbody=table.tBodies[0];const original=[...table.tHead.rows[0].cells];
if(original.length<2)return;
original.forEach((th,i)=>th.dataset.column='c'+i);
const empty=[...tbody.rows].filter(r=>r.cells.length!==original.length||r.querySelector('td[colspan]'));
const allRows=[...tbody.rows].filter(r=>!empty.includes(r));
allRows.forEach(row=>[...row.cells].forEach((cell,i)=>{cell.dataset.column='c'+i;enableValueDialog(cell,original[i]);}));
const key='heatwave-table-v2:'+table.id;const state=get(key)||{};let page=1;let size=Number(state.size)||25;let query='';let sortKey=null;let ascending=true;let filtered=allRows.slice();
const multiple=table.dataset.selectable==='true';let selectionHeader;
if(multiple){selectionHeader=document.createElement('th');selectionHeader.className='selection-column';selectionHeader.dataset.column='selection';const check=document.createElement('input');check.type='checkbox';check.setAttribute('aria-label','Select all visible rows');selectionHeader.append(check);table.tHead.rows[0].prepend(selectionHeader);allRows.forEach(row=>{const td=document.createElement('td');td.className='selection-column';td.dataset.column='selection';const input=document.createElement('input');input.type='checkbox';input.className='row-selection';input.setAttribute('aria-label','Select row '+(row.dataset.rowId||''));input.addEventListener('change',()=>render());td.append(input);row.prepend(td);});empty.forEach(r=>r.cells[0].colSpan=original.length+1);check.addEventListener('change',()=>{filtered.filter(r=>!r.hidden).forEach(r=>r.querySelector('.row-selection').checked=check.checked);render();});}
const toolbar=document.createElement('div');toolbar.className='shared-table-toolbar';const left=document.createElement('div');left.className='table-toolbar-left';const right=document.createElement('div');right.className='table-toolbar-right';toolbar.append(left,right);
const panel=table.closest('.panel');const scroll=table.closest('.table-scroll')||table;
scroll.before(toolbar);
if(panel){panel.querySelectorAll('.table-tools').forEach(old=>old.remove());panel.querySelectorAll('.panel-heading [data-dialog], [data-table-action="'+table.id+'"]').forEach(action=>left.append(action));}
const search=document.createElement('input');search.type='search';search.placeholder='Search loaded rows…';search.setAttribute('aria-label','Search table');search.className='table-search';search.addEventListener('input',()=>{query=search.value.toLocaleLowerCase();page=1;render();});
const sizeSelect=document.createElement('select');sizeSelect.setAttribute('aria-label','Rows per page');[10,25,50,100,250].forEach(n=>{const option=new Option(n+' rows',n);sizeSelect.add(option);});sizeSelect.value=String(size);sizeSelect.addEventListener('change',()=>{size=Number(sizeSelect.value);page=1;persist();render();});
const selected=()=>allRows.filter(r=>r.querySelector('.row-selection')?.checked);
if(table.dataset.editable==='true'){const b=control('Edit',()=>{const rows=selected();if(rows.length===1&&rows[0].dataset.editUrl)location.href=rows[0].dataset.editUrl;});b.dataset.selectionAction='one';left.append(b);}
if(table.dataset.deleteUrl){const b=control('Delete selected',()=>{const rows=selected();if(!rows.length||(table.dataset.requireManaged&&rows.some(row=>row.dataset.managed!=='true'))||!confirm('Delete '+rows.length+' selected '+(table.dataset.deleteLabel||'connection registrations')+'?'))return;const form=document.createElement('form');form.method='POST';form.action=table.dataset.deleteUrl;const values={csrf:document.querySelector('input[name="csrf"]')?.value||'',ids:rows.map(r=>r.dataset.rowId).join(',')};for(const [name,value] of Object.entries(values)){const input=document.createElement('input');input.type='hidden';input.name=name;input.value=value;form.append(input);}document.body.append(form);form.submit();});b.classList.add('danger');b.dataset.selectionAction=table.dataset.requireManaged?'managed':'many';left.append(b);}
if(table.dataset.lifecycleUrl){for(const action of ['start','stop']){const b=control(action==='start'?'Start selected':'Stop selected',()=>{const rows=selected();if(!rows.length||!confirm((action==='start'?'Start ':'Stop ')+rows.length+' selected DB system(s)?'+(action==='stop'?' This interrupts database connections and audit capture or writes.':'')))return;const form=document.createElement('form');form.method='POST';form.action=table.dataset.lifecycleUrl+action;for(const [name,value] of Object.entries({csrf:document.querySelector('input[name="csrf"]')?.value||'',ids:rows.map(row=>row.dataset.rowId).join(',')})){const input=document.createElement('input');input.type='hidden';input.name=name;input.value=value;form.append(input);}document.body.append(form);form.submit();});b.dataset.selectionAction=action;left.append(b);}}
if(table.dataset.selectionAction==='validate'){const b=control('Validate source',()=>{const row=selected()[0];if(row)location.href=row.dataset.validateUrl;});b.dataset.selectionAction='one';left.append(b);}
if(table.dataset.prepareTopics){const b=control('Prepare schema stream',()=>{const row=selected()[0];if(!row||!confirm('Create the required schema stream in the selected messaging service?'))return;const form=document.createElement('form');form.method='POST';form.action=row.dataset.prepareUrl;const input=document.createElement('input');input.type='hidden';input.name='csrf';input.value=document.querySelector('input[name="csrf"]')?.value||'';form.append(input);document.body.append(form);form.submit();});b.dataset.selectionAction='one';left.append(b);}
if(table.dataset.selectionAction==='images'){const b=control('Compare images',()=>{const row=selected()[0];if(row)document.getElementById(row.dataset.imageDialog)?.showModal();});b.dataset.selectionAction='one';left.append(b);}
if(table.dataset.logDownload){const b=control('Download build log',()=>{const row=selected()[0];if(row)location.href=row.dataset.logUrl;},'download');b.dataset.selectionAction='one';right.append(b);}
const previous=control('Previous page',()=>{page=Math.max(1,page-1);render();});previous.textContent='‹';previous.className='icon-button';
const next=control('Next page',()=>{page++;render();});next.textContent='›';next.className='icon-button';const pageLabel=document.createElement('span');pageLabel.className='table-page-label';const selectedLabel=document.createElement('span');selectedLabel.className='table-selection-label';
left.append(search,sizeSelect,previous,pageLabel,next,selectedLabel);
function cellText(row,col){return row.querySelector('[data-column="'+col+'"]')?.innerText.trim()||'';}
right.append(control('Download selected or loaded matching rows as CSV',()=>{const rows=selected().length?selected():filtered;const columns=[...table.tHead.rows[0].cells].filter(c=>c.dataset.column!=='selection');const escape=value=>'"'+String(value).replaceAll('"','""')+'"';const safe=value=>/^[=+\-@\t\r]/.test(value)?"'"+value:value;const data=[columns.map(c=>escape(c.textContent.replace(/[↕↑↓]/g,'').trim())).join(','),...rows.map(r=>columns.map(c=>escape(safe(cellText(r,c.dataset.column)))).join(','))].join('\r\n');const blob=new Blob(['\uFEFF'+data],{type:'text/csv;charset=utf-8'});const url=URL.createObjectURL(blob);const a=document.createElement('a');a.href=url;a.download=table.id+'.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),5000);},'download'));
right.append(control('Reset column layout',()=>{original.forEach(th=>move(th.dataset.column,null));table.querySelectorAll('th').forEach(th=>{th.style.width='';th.style.minWidth='';});table.style.width='';save(key,{size});},'refresh'));
function persist(){const headers=[...table.tHead.rows[0].cells].filter(c=>c.dataset.column!=='selection');save(key,{size,order:headers.map(h=>h.dataset.column),widths:Object.fromEntries(headers.map(h=>[h.dataset.column,h.style.width]))});}
function move(column,before){for(const row of [table.tHead.rows[0],...allRows]){const cell=[...row.cells].find(c=>c.dataset.column===column);const dest=before?[...row.cells].find(c=>c.dataset.column===before):null;if(cell&&cell!==dest)row.insertBefore(cell,dest);}}
if(Array.isArray(state.order)&&state.order.length===original.length&&state.order.every(c=>original.some(h=>h.dataset.column===c))){state.order.forEach(c=>move(c,null));}
let dragging=null;original.forEach(th=>{const col=th.dataset.column;th.title='Click to sort · Drag to rearrange · Drag the right edge to resize';th.tabIndex=0;th.draggable=true;th.setAttribute('role','columnheader');if(state.widths?.[col]){th.style.width=state.widths[col];th.style.minWidth=state.widths[col];}
const sort=()=>{ascending=sortKey===col?!ascending:true;sortKey=col;original.forEach(h=>h.removeAttribute('aria-sort'));th.setAttribute('aria-sort',ascending?'ascending':'descending');allRows.sort((a,b)=>cellText(a,col).localeCompare(cellText(b,col),undefined,{numeric:true})*(ascending?1:-1));allRows.forEach(r=>tbody.append(r));page=1;render();};th.addEventListener('click',e=>{if(!e.target.closest('.resize-handle'))sort();});th.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();sort();}else if(e.altKey&&['ArrowLeft','ArrowRight'].includes(e.key)){const current=[...table.tHead.rows[0].cells].filter(c=>c.dataset.column!=='selection');const position=current.indexOf(th);if(e.key==='ArrowLeft'&&position>0)move(col,current[position-1].dataset.column);if(e.key==='ArrowRight'&&position<current.length-1)move(current[position+1].dataset.column,col);persist();}});
th.addEventListener('dragstart',e=>{dragging=col;e.dataTransfer.effectAllowed='move';e.dataTransfer.setData('text/plain',col);});th.addEventListener('dragover',e=>{e.preventDefault();th.classList.add('column-drop-target');});th.addEventListener('dragleave',()=>th.classList.remove('column-drop-target'));th.addEventListener('drop',e=>{e.preventDefault();th.classList.remove('column-drop-target');if(dragging&&dragging!==col){move(dragging,col);persist();}dragging=null;});
const handle=document.createElement('span');handle.className='resize-handle';handle.setAttribute('aria-hidden','true');th.append(handle);handle.addEventListener('pointerdown',e=>{e.preventDefault();e.stopPropagation();th.draggable=false;const x=e.clientX,width=th.getBoundingClientRect().width;handle.setPointerCapture(e.pointerId);const move=ev=>{const n=Math.max(80,Math.min(900,width+ev.clientX-x));th.style.width=n+'px';th.style.minWidth=n+'px';};const done=()=>{handle.removeEventListener('pointermove',move);handle.removeEventListener('pointerup',done);th.draggable=true;persist();};handle.addEventListener('pointermove',move);handle.addEventListener('pointerup',done);});});
function render(){filtered=allRows.filter(r=>r.innerText.toLocaleLowerCase().includes(query));const pages=Math.max(1,Math.ceil(filtered.length/size));page=Math.min(page,pages);allRows.forEach(r=>r.hidden=true);filtered.slice((page-1)*size,page*size).forEach(r=>r.hidden=false);pageLabel.textContent=page+' / '+pages;previous.disabled=page===1;next.disabled=page===pages;const count=selected().length;selectedLabel.textContent=count?count+' selected':filtered.length+' loaded rows';toolbar.querySelectorAll('[data-selection-action]').forEach(b=>{const action=b.dataset.selectionAction;b.disabled=action==='one'?count!==1:count===0;if(action==='managed'){b.disabled=count===0||selected().some(row=>row.dataset.managed!=='true');b.title=count&&b.disabled?'Delete is available only for resources managed by this app installation.':'Delete selected';}if(action==='start'||action==='stop')b.disabled=count===0||selected().some(row=>row.dataset.state!==(action==='start'?'INACTIVE':'ACTIVE'));});if(selectionHeader){const visible=filtered.filter(r=>!r.hidden);const chosen=visible.filter(r=>r.querySelector('.row-selection').checked);const box=selectionHeader.querySelector('input');box.checked=visible.length>0&&chosen.length===visible.length;box.indeterminate=chosen.length>0&&chosen.length<visible.length;}empty.forEach(r=>r.hidden=allRows.length>0);table.dispatchEvent(new CustomEvent('table:selection-change'));}
render();
});}
window.initializeTables=initializeTables;initializeTables(document.querySelector('main')||document);
})();
