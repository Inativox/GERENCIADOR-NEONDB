'use strict';
const { app } = require('electron');
const { Worker } = require('node:worker_threads');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const artifact = process.argv.slice(2).find(argument => !argument.startsWith('--'));
const root = artifact ? path.resolve(artifact) : path.resolve(__dirname, '..');
const lowMemory = process.argv.includes('--low-memory');
const totalRows = lowMemory ? 300001 : 100000;
const rowsPerFile = lowMemory ? 300000 : 100000;
app.disableHardwareAcceleration();
app.on('window-all-closed', () => {});
app.whenReady().then(async () => {
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'flow-native-smoke-'));
    try {
        const result = await new Promise((resolve, reject) => {
            const worker = new Worker(`
                const { workerData, parentPort } = require('node:worker_threads');
                const fs = require('node:fs');
                const { runFlow } = require(workerData.pipeline);
                const flow = { name: 'Synthetic', operation: 'c6', generation: {}, enrichment: {enabled:false}, cleaning: {enabled:true, rootSource:'none',blocklist:true,prohibitedCnaes:[]}, output:{formatId:'padrao',rowsPerFile:workerData.rowsPerFile,directory:workerData.directory} };
                let calls = 0, fail = true;
                let peakRss = 0, peakHeap = 0, maxPendingFiles = 0;
                function sample() {
                    const memory = process.memoryUsage();
                    peakRss = Math.max(peakRss, memory.rss);
                    peakHeap = Math.max(peakHeap, memory.heapUsed);
                    maxPendingFiles = Math.max(maxPendingFiles, fs.readdirSync(workerData.directory).filter(name => name.endsWith('.xlsx.tmp')).length);
                }
                const monitor = setInterval(sample, 100); monitor.unref();
                const extra = workerData.lowMemory ? {razao_social:'Empresa sintética para validação de memória '.repeat(3),email:'contato@example.test',atividade_principal:'Descrição sintética da atividade '.repeat(8),estado:'SP',cidade:'São Paulo',data_abertura:'2020-01-15'} : {};
                const providers = {
                    async *iterateReceita() { for (let offset=0;offset<workerData.totalRows;offset+=2000) yield {rows:Array.from({length:Math.min(2000,workerData.totalRows-offset)},(_,i)=>{const n=offset+i;return {...extra,cnpj:String(n+1).padStart(14,'0'),phones:['119'+String(12340000+n)]};})}; },
                    async queryPhones() { if (++calls===3 && fail) throw new Error('Synthetic failure'); return []; },
                };
                (async()=>{
                    try { await runFlow({flow,user:{username:'Davi'},jobDir:workerData.directory,providers,cachePolicy:{compressed:true,prune:true}}); throw new Error('Failure not reached'); }
                    catch(error) { if(error.code!=='FLOW_FAILED')throw error; }
                    fail=false;
                    const result=await runFlow({flow,user:{username:'Davi'},jobDir:workerData.directory,providers,cachePolicy:{compressed:true,prune:true},onUpdate:sample});
                    sample(); clearInterval(monitor);
                    const outputs=result.outputs.filter(output=>output.kind==='xlsx');
                    if(result.counts.kept!==workerData.totalRows||result.counts.exported!==workerData.totalRows||calls!==Math.ceil(workerData.totalRows/2000)+1)throw new Error('Invalid resumed result');
                    if(outputs.length!==Math.ceil(workerData.totalRows/workerData.rowsPerFile)||outputs.some((output,i)=>output.rows!==Math.min(workerData.rowsPerFile,workerData.totalRows-i*workerData.rowsPerFile)))throw new Error('Invalid sequential output parts');
                    if(maxPendingFiles!==1)throw new Error('More than one XLSX part was open at a time');
                    if(workerData.lowMemory&&peakRss>768*1024*1024)throw new Error('Low-memory RSS budget exceeded: '+Math.round(peakRss/1024/1024)+' MiB');
                    parentPort.postMessage({kept:result.counts.kept,files:outputs.length,maxPendingFiles,peakRss:Math.round(peakRss/1024/1024),peakHeap:Math.round(peakHeap/1024/1024),heap:Math.round(process.memoryUsage().heapUsed/1024/1024)});
                })().catch(error=>{throw error;});
            `, { eval: true, resourceLimits: { maxOldGenerationSizeMb: 64 }, workerData: { pipeline: path.join(root, 'src/main/flows/pipeline.js'), directory, totalRows, rowsPerFile, lowMemory } });
            const timer = setTimeout(() => { void worker.terminate(); reject(new Error('Cache smoke timeout')); }, lowMemory ? 180000 : 45000);
            worker.once('message', result => { clearTimeout(timer); resolve(result); });
            worker.once('error', error => { clearTimeout(timer); reject(error); });
            worker.once('exit', code => { if (code) { clearTimeout(timer); reject(new Error(`Cache worker exited: ${code}`)); } });
        });
        console.log(`Cache Electron aprovado: ${result.kept} registros, falha e retomada, deduplicação em disco, heap de ${result.heap} MiB com limite de 64 MiB.`);
        console.log(`Exportação: ${result.files} arquivo(s), no máximo ${result.maxPendingFiles} XLSX aberto, pico RSS ${result.peakRss} MiB, pico heap ${result.peakHeap} MiB.`);
    } finally { await fs.rm(directory, { recursive: true, force: true }); }
}).then(() => app.exit(0), error => { console.error(error.message); app.exit(1); });
