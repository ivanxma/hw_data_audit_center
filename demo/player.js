'use strict';
(() => {
const video=document.getElementById('demo'),status=document.querySelector('[data-playback-status]'),button=document.querySelector('[data-play-demo]');if(!video)return;let playRequest=0;
async function play(time){
 const request=++playRequest;
 if(Number.isFinite(time))video.currentTime=time;
 try{await video.play();if(request!==playRequest)return;status.textContent='';video.scrollIntoView({behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'auto':'smooth',block:'center'});}
 catch(error){if(request!==playRequest||error.name==='AbortError')return;status.textContent='Use the video player controls to start playback. If the video is unavailable, refresh this page and try again.';}
}
button?.addEventListener('click',()=>{if(video.paused)play();else{playRequest++;video.pause();}});
document.querySelectorAll('[data-seek]').forEach(chapter=>chapter.addEventListener('click',()=>play(Number(chapter.dataset.seek))));
video.addEventListener('play',()=>{if(button)button.textContent='Pause demo';status.textContent='';});
video.addEventListener('pause',()=>{if(button)button.textContent='Play demo';});
video.addEventListener('ended',()=>{if(button)button.textContent='Replay demo';});
video.addEventListener('error',()=>{status.textContent='The video could not be loaded. Refresh this page and try again.';});
})();
