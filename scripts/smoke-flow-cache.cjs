'use strict';
const { app } = require('electron');
const { Worker } = require('node:worker_threads');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const artifact = process.argv.slice(2).find(argument => !argument.startsWith('--'));
const root = artifact ? path.resolve(artifact) : path.resolve(__dirname, '..');
const lowMemory = process.argv.includes('--low-memory');
const totalRows = lowMemory ? 300001 : 100001;
const rowsPerFile = lowMemory ? 300000 : 100000;
const batchSize = 100000;
// Match the production worker budget for 100k-row batches.
const heapLimit = 512;
app.disableHardwareAcceleration();
app.on('window-all-closed', () => {});
app.whenReady().then(async () => {
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'flow-native-smoke-'));
    try {
        const result = await new Promise((resolve, reject) => {
            const worker = new Worker(`
                const { workerData, parentPort } = require('node:worker_threads');
                const fs = require('node:fs');
                const path = require('node:path');
                const { runFlow } = require(workerData.pipeline);
                const flow = { name: 'Synthetic', operation: 'c6', generation: {}, enrichment: {enabled:workerData.lowMemory,strategy:'append'}, cleaning: {enabled:true, rootSource:'none',blocklist:true,prohibitedCnaes:[]}, output:{fileName:'lista rca',formatId:'padrao',rowsPerFile:workerData.rowsPerFile,directory:workerData.directory} };
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
                    async *iterateReceita({batchSize}) { if(batchSize!==workerData.batchSize)throw new Error('Unexpected Receita batch size'); for (let offset=0;offset<workerData.totalRows;offset+=batchSize) yield {rows:Array.from({length:Math.min(batchSize,workerData.totalRows-offset)},(_,i)=>{const n=offset+i;return {...extra,cnpj:String(n+1).padStart(14,'0'),phones:['119'+String(12340000+n)]};})}; },
                    async queryEnrichment(documents) { if(documents.length>workerData.batchSize)throw new Error('Oversized enrichment batch'); return documents.map(cnpj=>({cnpj,phones:['219'+String(12340000+Number(cnpj)),'319'+String(12340000+Number(cnpj))]})); },
                    async queryPhones() { if (++calls===2 && fail) throw new Error('Synthetic failure'); return []; },
                };
                (async()=>{
                    try { await runFlow({flow,user:{username:'Davi'},jobDir:workerData.directory,providers,cachePolicy:{compressed:true,prune:true},processingPolicy:{batchSize:workerData.batchSize}}); throw new Error('Failure not reached'); }
                    catch(error) { if(error.code!=='FLOW_FAILED')throw error; }
                    fail=false;
                    const result=await runFlow({flow,user:{username:'Davi'},jobDir:workerData.directory,providers,cachePolicy:{compressed:true,prune:true},processingPolicy:{batchSize:workerData.batchSize},onUpdate:sample});
                    sample(); clearInterval(monitor);
                    const outputs=result.outputs.filter(output=>output.kind==='xlsx');
                    if(result.counts.kept!==workerData.totalRows||result.counts.exported!==workerData.totalRows||calls!==Math.ceil(workerData.totalRows/workerData.batchSize)+1)throw new Error('Invalid resumed result');
                    if(outputs.length!==Math.ceil(workerData.totalRows/workerData.rowsPerFile)||outputs.some((output,i)=>output.rows!==Math.min(workerData.rowsPerFile,workerData.totalRows-i*workerData.rowsPerFile)))throw new Error('Invalid sequential output parts');
                    if(outputs.some((output,i)=>path.basename(output.path)!=='lista rca parte'+(i+1)+'.xlsx'))throw new Error('Invalid output names');
                    if(maxPendingFiles!==1)throw new Error('More than one XLSX part was open at a time');
                    if(workerData.lowMemory&&peakRss>768*1024*1024)throw new Error('Low-memory RSS budget exceeded: '+Math.round(peakRss/1024/1024)+' MiB');
                    parentPort.postMessage({kept:result.counts.kept,files:outputs.length,maxPendingFiles,peakRss:Math.round(peakRss/1024/1024),peakHeap:Math.round(peakHeap/1024/1024),heap:Math.round(process.memoryUsage().heapUsed/1024/1024)});
                })().catch(error=>{throw error;});
            `, { eval: true, resourceLimits: { maxOldGenerationSizeMb: heapLimit }, workerData: { pipeline: path.join(root, 'src/main/flows/pipeline.js'), directory, totalRows, rowsPerFile, lowMemory, batchSize } });
            let completed, failure;
            const timer = setTimeout(() => {
                failure = new Error('Cache smoke timeout');
                void worker.terminate();
            }, lowMemory ? 300000 : 180000);
            worker.once('message', result => { completed = result; });
            worker.once('error', error => { failure = error; });
            // On Windows, LMDB native handles may stay open until the worker exits.
            // Receiving its result is not enough to safely delete the test directory.
            worker.once('exit', code => {
                clearTimeout(timer);
                if (failure) reject(failure);
                else if (code || !completed) reject(new Error(`Cache worker exited without a confirmed result: ${code}`));
                else resolve(completed);
            });
        });
        console.log(`Cache Electron aprovado: ${result.kept} registros, lotes de ${batchSize}, falha e retomada, deduplicação em disco, heap de ${result.heap} MiB com limite de ${heapLimit} MiB.`);
        console.log(`Exportação: ${result.files} arquivo(s), no máximo ${result.maxPendingFiles} XLSX aberto, pico RSS ${result.peakRss} MiB, pico heap ${result.peakHeap} MiB.`);
    } finally { await fs.rm(directory, { recursive: true, force: true }); }
}).then(() => app.exit(0), error => { console.error(error.message); app.exit(1); });
