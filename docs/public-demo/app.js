
const $ = id => document.getElementById(id);
function node(tag,text,cls){const n=document.createElement(tag);n.textContent=String(text);if(cls)n.className=cls;return n;}
function rows(items){$('details').replaceChildren(...items.map(([title,body])=>{const r=node('div','','row');r.append(node('strong',title),node('small',body));return r;}));}
function error(e){$('result').textContent=e.message||String(e);$('result').className='status error';$('details').replaceChildren();}
function ready(text){$('result').textContent=text;$('result').className='status';}

const legal=[{id:'w',client_id:0,at:0,input:{Op:'put',Key:'x',Val:'A'}},{id:'w',client_id:0,at:1,output:{}},{id:'r',client_id:1,at:2,input:{Op:'get',Key:'x'}},{id:'r',client_id:1,at:3,output:{Val:'A',Found:true}}];
const stale=JSON.parse(JSON.stringify(legal));stale[3].output={Val:'old',Found:true};
const uncertain=[{id:'w',client_id:0,at:0,input:{Op:'put',Key:'x',Val:'A'}},{id:'w',client_id:0,at:1,output:{Unknown:true}},{id:'r',client_id:1,at:2,input:{Op:'get',Key:'x'}},{id:'r',client_id:1,at:3,output:{Val:'A',Found:true}}];
function example(value){$('events').value=JSON.stringify(value,null,2);}
function input(){if($('events').value.length>32768)throw new Error('History exceeds 32 KiB');return JSON.parse($('events').value);}
function render(d){ready(d.result==='Ok'?'A legal ordering exists':d.result==='Illegal'?'No legal ordering exists':'Search timed out: unknown');rows([['Observed operations',d.operations],['Indeterminate operations',d.indeterminate+' retained as uncertain, not failed'],['Search bound',d.budget_ms+' ms'],['Interpretation',d.result==='Ok'?'This finite history fits the KV specification. It is not a proof about a deployed cluster.':d.result==='Illegal'?'At least one observation contradicts the sequential KV specification.':'Unknown is not a passing result. Reduce the history and retry.']]);}
$('legal').onclick=()=>{example(legal);run();};$('stale').onclick=()=>{example(stale);run();};$('uncertain').onclick=()=>{example(uncertain);run();};$('reset').onclick=()=>{example(legal);run();};example(legal);

let worker, sequence=0, pending=new Map();
const boot=new Promise((resolve,reject)=>{
 worker=new Worker('./worker.js');
 const timer=setTimeout(()=>reject(new Error('Engine did not start. Reload to retry.')),20000);
 worker.onerror=()=>{clearTimeout(timer);reject(new Error('Engine failed to load. Reload to retry.'));for(const p of pending.values())p.reject(new Error('Engine stopped. Reload to retry.'));pending.clear();};
 worker.onmessage=({data})=>{if(data.ready){clearTimeout(timer);resolve();return;}if(data.bootError){clearTimeout(timer);reject(new Error(data.bootError));return;}const p=pending.get(data.id);if(p){pending.delete(data.id);data.error?p.reject(new Error(data.error)):p.resolve(data.value);}};
});
async function evaluate(input){await boot;const id=++sequence;return new Promise((resolve,reject)=>{pending.set(id,{resolve,reject});worker.postMessage({id,input});});}
async function run(){if($('run').disabled)return;$('run').disabled=true;try{render(await evaluate(input()));}catch(e){error(e);}finally{$('run').disabled=false;}}
document.querySelector('form').addEventListener('submit',e=>{e.preventDefault();run();});
boot.then(()=>{ $('run').disabled=false; run(); }).catch(error);
