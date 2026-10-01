'use strict';
const { app } = require('electron');
const { Worker } = require('node:worker_threads');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const root = process.argv[2] ? path.resolve(process.argv[2]) : path.resolve(__dirname, '..');
app.disableHardwareAcceleration();
app.on('window-all-closed', () => {});
app.whenReady().then(async () => {
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'flow-native-smoke-'));
    try {
        const result = await new Promise((resolve, reject) => {
            const worker = new Worker(`
                const { workerData, parentPort } = require('node:worker_threads');
                const { runFlow } = require(workerData.pipeline);
                const flow = { name: 'Synthetic', operation: 'c6', generation: {}, enrichment: {enabled:false}, cleaning: {enabled:true, rootSource:'none',blocklist:true,prohibitedCnaes:[]}, output:{formatId:'padrao',rowsPerFile:100000,directory:workerData.directory} };
                let calls = 0, fail = true;
                const providers = {
                    async *iterateReceita() { for (let batch=0;batch<50;batch++) yield {rows:Array.from({length:2000},(_,i)=>{const n=batch*2000+i;return {cnpj:String(n+1).padStart(14,'0'),phones:['119'+String(12340000+n)]};})}; },
                    async queryPhones() { if (++calls===3 && fail) throw new Error('Synthetic failure'); return []; },
                };
                (async()=>{
                    try { await runFlow({flow,user:{username:'Davi'},jobDir:workerData.directory,providers}); throw new Error('Failure not reached'); }
                    catch(error) { if(error.code!=='FLOW_FAILED')throw error; }
                    fail=false;
                    const result=await runFlow({flow,user:{username:'Davi'},jobDir:workerData.directory,providers});
                    if(result.counts.kept!==100000||result.counts.exported!==100000||calls!==51)throw new Error('Invalid resumed result');
                    parentPort.postMessage({kept:result.counts.kept,heap:Math.round(process.memoryUsage().heapUsed/1024/1024)});
                })().catch(error=>{throw error;});
            `, { eval: true, resourceLimits: { maxOldGenerationSizeMb: 64 }, workerData: { pipeline: path.join(root, 'src/main/flows/pipeline.js'), directory } });
            const timer = setTimeout(() => { void worker.terminate(); reject(new Error('Cache smoke timeout')); }, 45000);
            worker.once('message', result => { clearTimeout(timer); resolve(result); });
            worker.once('error', error => { clearTimeout(timer); reject(error); });
            worker.once('exit', code => { if (code) { clearTimeout(timer); reject(new Error(`Cache worker exited: ${code}`)); } });
        });
        console.log(`Cache Electron aprovado: ${result.kept} registros, falha e retomada, deduplicação em disco, heap de ${result.heap} MiB com limite de 64 MiB.`);
    } finally { await fs.rm(directory, { recursive: true, force: true }); }
}).then(() => app.exit(0), error => { console.error(error.message); app.exit(1); });
