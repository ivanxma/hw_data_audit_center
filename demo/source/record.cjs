/* Record the loopback demo only. Never signs in to a live service. */
const fs=require('fs'),path=require('path');
const root=path.resolve(__dirname,'..');
const playwright=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const quick=process.argv.includes('--quick'),scale=quick?.12:1;const selectedScene=process.argv.find(a=>a.startsWith('--scene='))?.split('=')[1];
const output=path.join(root,'.build',quick?'review':selectedScene?'replacement-'+selectedScene:'recording');fs.mkdirSync(output,{recursive:true});
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
(async()=>{
 const browser=await playwright.chromium.launch({executablePath:process.env.CHROME_PATH||undefined,headless:true});
 const context=await browser.newContext({viewport:{width:2560,height:1440},deviceScaleFactor:1,acceptDownloads:true,...(!quick?{recordVideo:{dir:path.join(root,'.build','raw'),size:{width:2560,height:1440}}}:{})});
 const page=await context.newPage(),errors=[];page.setDefaultTimeout(8000);
 page.on('pageerror',e=>errors.push(e.message));
 page.on('response',r=>{if(r.status()>=400)errors.push('HTTP '+r.status()+' '+r.url());});
 await page.goto('http://127.0.0.1:5155/movie');await page.waitForFunction(()=>window.demoReady===true);
 const story=await page.evaluate(()=>window.demo.story),frame=()=>page.frameLocator('#app-frame');
 const mouse=async locator=>{await locator.scrollIntoViewIfNeeded();const b=await locator.boundingBox();if(!b)throw Error('Missing pointer target');await page.evaluate(({x,y})=>demo.cursor(x,y),{x:b.x+b.width/2,y:b.y+b.height/2});await sleep(quick?35:430);return b;};
 const click=async locator=>{const b=await mouse(locator);await page.evaluate(({x,y})=>demo.cursor(x,y,true),{x:b.x+b.width/2,y:b.y+b.height/2});await locator.click();};
 const select=async(locator,value)=>{await mouse(locator);await locator.selectOption(value);};
 const snap=async name=>{await page.screenshot({path:path.join(output,name+'.png')});};
 const chosen=selectedScene?story.scenes.filter(s=>s.id===selectedScene):story.scenes;const start=chosen[0].start,total=chosen.reduce((n,s)=>n+s.duration,0);const begin=Date.now();
 if(!quick)await page.evaluate(start=>{window.movieBegin=performance.now();window.movieClock=setInterval(()=>demo.tick(Math.min(300,start+(performance.now()-movieBegin)/1000)),100);},start);
 const waitUntil=async seconds=>{const remaining=begin+(seconds-start)*1000*scale-Date.now();if(remaining>0)await sleep(remaining);};
 for(let index=0;index<story.scenes.length;index++){
  const scene=story.scenes[index];if(selectedScene&&scene.id!==selectedScene)continue;await waitUntil(scene.start);await page.evaluate(i=>demo.setScene(i),index);await sleep(quick?160:750);
  const at=async(seconds,fn)=>{await waitUntil(scene.start+seconds);await fn();};
  const full=async()=>{await page.evaluate(()=>demo.expand());await sleep(quick?150:750);};
  if(scene.id==='configuration'){
   await frame().locator('#connections-table .row-selection').first().waitFor();
   await at(2,()=>click(frame().locator('#connections-table .row-selection').first()));
   await at(3,()=>click(frame().locator('#connections .shared-table-toolbar').getByRole('button',{name:'Edit',exact:true})));
   await frame().locator('#connection-dialog[open]').waitFor();
   await at(5,()=>click(frame().locator('[data-test-connection]')));
   await at(8,()=>snap('connection-test'));
   await at(10,()=>click(frame().locator('#connection-dialog').getByRole('button',{name:'Cancel',exact:true})));
   await at(11,()=>click(frame().locator('[data-tab=flows]')));
   await at(12,()=>click(frame().getByRole('button',{name:'Register table',exact:true})));
   const form=frame().locator('#flow-dialog');
   await form.locator('[name=name]').fill('demo-order-history');
   await select(form.locator('[name=source_id]'),'1');await select(form.locator('[name=target_id]'),'2');
   await select(form.locator('[name=source_schema]'),'demo_sales');await select(form.locator('[name=target_schema]'),'demo_audit');
   await select(form.locator('[name=source_table]'),'orders');await select(form.locator('[name=target_table]'),'__new__');
   await select(form.locator('[name=queue_connection_id]'),'1');await select(form.locator('[name=topic]'),'demo-orders');await select(form.locator('[name=connect_harness_id]'),'demo-harness');await select(form.locator('[name=capture_mode]'),'end');
  }else if(scene.id==='streaming'){
   await frame().locator('#resource-stream_pool .row-selection').first().waitFor();
   await at(3,()=>mouse(frame().locator('#resource-stream')));
   await at(5,()=>snap('streaming-resources'));
   await at(7,()=>click(frame().locator('[data-service-tab=queues]')));
   await at(9,()=>click(frame().getByRole('button',{name:'Add queue connection',exact:true})));
   const form=frame().locator('#queue-dialog');await form.locator('[name=name]').fill('Demo Streaming connection');await select(form.locator('[name=resource_ocid]'),'demo-pool');
   await at(14,()=>snap('principal-authentication'));
  }else if(scene.id==='control'){
   await frame().locator('#control-flows .row-selection').first().waitFor();
   await at(2,()=>click(frame().locator('#control-flows .row-selection').first()));
   await at(4,()=>mouse(frame().locator('[data-process-role=Debezium]')));
   await at(7,()=>mouse(frame().locator('[data-process-role="History writer"]')));
   await at(10,()=>snap('control-workloads'));
  }else if(scene.id==='insert'){
   await at(5,()=>snap('insert-three-panels'));
   await at(9,full);await at(10,()=>click(frame().locator('.message-row summary')));
   await at(14,()=>frame().locator('.message-row').evaluate(el=>el.scrollIntoView({block:'start'})));
  }else if(scene.id==='update'||scene.id==='delete'){
   await at(5,()=>snap(scene.id+'-three-panels'));
   await at(8,full);
   await at(10,()=>click(frame().locator('#history-table .row-selection').first()));
   await at(12,()=>click(frame().getByRole('button',{name:'Compare images',exact:true})));
   await at(15,()=>snap(scene.id+'-images'));
  }else if(scene.id==='lookup'){
   await frame().locator('[name=key]').waitFor();
   await at(2,async()=>{await mouse(frame().locator('[name=key]'));await frame().locator('[name=key]').fill('{"id":1042}');});
   await frame().locator('[name=from]').fill('2026-10-24T10:00');await frame().locator('[name=to]').fill('2026-10-24T11:00');
   await at(6,()=>click(frame().getByRole('button',{name:'Apply filters',exact:true})));
   await at(10,()=>frame().locator('#history-table').evaluate(el=>el.scrollIntoView({block:'center'})));
   await at(14,()=>snap('row-key-history'));
  }else if(scene.id==='global-chart'){
   await frame().locator('#global-chart-legend a').first().waitFor();
   await at(2,()=>frame().locator('#global-chart').evaluate(el=>el.scrollIntoView({block:'center'})));
   await at(5,()=>snap('all-databases-pie'));
   await at(7,()=>click(frame().locator('#global-chart-legend a').first()));
   await frame().locator('#global-chart-legend a').first().waitFor();
   await at(10,()=>frame().locator('#global-chart').evaluate(el=>el.scrollIntoView({block:'center'})));
   await at(12,()=>snap('table-bars'));
   await at(14,()=>click(frame().locator('#global-chart-legend a').first()));
   await frame().locator('#audit-chart').waitFor();
   await at(17,()=>frame().locator('#audit-chart').evaluate(el=>el.scrollIntoView({block:'center'})));
  }else if(scene.id==='trend-chart'){
   await frame().locator('#audit-chart').waitFor();
   await at(2,()=>frame().locator('#audit-chart').evaluate(el=>el.scrollIntoView({block:'center'})));
   await at(4,()=>snap('line-chart'));
   await at(6,()=>select(frame().locator('[name=chart_type]'),'bar'));
   await at(8,()=>frame().locator('#audit-chart').evaluate(el=>el.scrollIntoView({block:'center'})));
   await at(10,()=>snap('bar-chart'));
   await at(12,()=>select(frame().locator('[name=chart_type]'),'stacked'));
   await at(14,()=>frame().locator('#audit-chart').evaluate(el=>el.scrollIntoView({block:'center'})));
   await at(17,()=>mouse(frame().locator('[data-download-chart="audit-chart"]')));
  }
  await waitUntil(scene.start+scene.duration-.7);await snap(scene.id);console.log(JSON.stringify({scene:scene.id,elapsed:Math.round((Date.now()-begin)/1000),errors:errors.length}));
 }
 await waitUntil(start+total);if(!quick)await page.evaluate(()=>clearInterval(movieClock));
 const video=page.video();await context.close();const raw=video?await video.path():null;await browser.close();
 fs.writeFileSync(path.join(output,'recording.json'),JSON.stringify({raw,quick,errors,start,duration:total,width:2560,height:1440},null,2));
 if(errors.length)throw Error('Browser errors: '+errors.join('; '));
 console.log('Recording complete: '+(raw||'quick review'));
})().catch(error=>{console.error(error);process.exit(1);});
