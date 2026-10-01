const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { EventEmitter } = require('node:events');
const { createUserStore } = require('../src/main/flows/store');
const { createFlowManager } = require('../src/main/flows/manager');

test('reiniciar etapa substitui contadores históricos pelos contadores do checkpoint atual', async t => {
    const directory=fs.mkdtempSync(path.join(os.tmpdir(),'flow-counts-test-'));
    t.after(()=>fs.rmSync(directory,{recursive:true,force:true}));
    const rootFile=path.join(directory,'root.json');
    fs.writeFileSync(rootFile,'{}');
    const store=createUserStore(directory,'Davi');
    store.saveJob({id:'synthetic',owner:'Davi',flowName:'Synthetic',status:'cancelled',logs:[],counts:{generated:1000,withoutPhones:500,ninthDigitAdded:200},rootFile,jobDir:directory,flowSnapshot:{cleaning:{blocklist:false}}});
    let complete;
    const finished=new Promise(resolve=>{complete=resolve;});
    const manager=createFlowManager({baseDirectory:directory,getUser:()=>({username:'Davi',role:'admin'}),resolveConnections:async()=>({}),onUpdate:job=>{if(job.status==='completed')complete(job);},workerFactory:()=>{
        const worker=new EventEmitter();
        worker.terminate=async()=>{};
        setImmediate(()=>{
            worker.emit('message',{type:'update',data:{stage:'generation',counts:{generated:2},replaceCounts:true}});
            worker.emit('message',{type:'result',data:{status:'completed',counts:{generated:2}}});
        });
        return worker;
    }});
    manager.resume('synthetic');
    const result=await finished;
    assert.deepEqual(result.counts,{generated:2});
    assert.deepEqual(store.getJob('synthetic').counts,{generated:2});
});
