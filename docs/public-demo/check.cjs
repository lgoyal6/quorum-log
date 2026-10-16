const assert = require('node:assert/strict');
const fs = require('node:fs');
globalThis.crypto ||= require('node:crypto').webcrypto;
require('./wasm_exec.js');
(async () => {
 const go = new Go();
 const {instance} = await WebAssembly.instantiate(fs.readFileSync(__dirname+'/engine.wasm'),go.importObject);
 go.run(instance).catch(error => { console.error(error); process.exit(1); });
 const evaluate = input => JSON.parse(publicDemo(JSON.stringify(input)));
 const legal=[{id:'w',client_id:0,at:0,input:{Op:'put',Key:'x',Val:'A'}},{id:'w',client_id:0,at:1,output:{}},{id:'r',client_id:1,at:2,input:{Op:'get',Key:'x'}},{id:'r',client_id:1,at:3,output:{Val:'A',Found:true}}];
 assert.equal(evaluate(legal).result,'Ok');
 const stale=structuredClone(legal);stale[3].output.Val='old';assert.equal(evaluate(stale).result,'Illegal');
 const unknown=structuredClone(legal);unknown[1].output={Unknown:true};assert.equal(evaluate(unknown).result,'Ok');assert.equal(evaluate(unknown).indeterminate,1);
 assert.ok(evaluate([...legal,...legal]).error);
 assert.ok(evaluate(Array(25).fill(legal[0])).error);
 assert.ok(evaluate([{...legal[0],at:-1}]).error);
 console.log('PASS: legal, stale, uncertain effect, duplicate identity, event bound, negative timestamp');
 process.exit(0);
})().catch(error=>{console.error(error);process.exit(1);});
