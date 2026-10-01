const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const {runFlow}=require('../src/main/flows/pipeline');

function fixture(t){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'flow-final-filters-test-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));return dir;}
function flow(directory){return {name:'Synthetic',operation:'c6',generation:{phone:'all'},enrichment:{enabled:true,strategy:'append'},api:{enabled:true},cleaning:{enabled:true,rootSource:'none',blocklist:true,invalidPhones:true,removeLandlines:true,prohibitedCnaes:['9999999']},output:{formatId:'padrao',rowsPerFile:100,directory}};}

test('API recebe os CNPJs enriquecidos antes de raiz, CNAE, blocklist, fixos e descarte sem contato; exportação recebe só o resultado final',async t=>{
    const directory=fixture(t);
    const ids=Array.from({length:7},(_,i)=>String(i+1).padStart(14,'0'));
    const phoneLists=[[],['1132345678'],[],['3198765432'],['4198765432'],['5198765432'],['6198765432']];
    const rows=ids.map((cnpj,i)=>({cnpj,razao_social:'Synthetic',phones:phoneLists[i],atividade_principal_cod:i===6?'9999999':''}));
    const rootFile=path.join(directory,'root.json');fs.writeFileSync(rootFile,JSON.stringify({documents:[ids[5]]}));
    const selected=flow(directory);selected.cleaning.rootSource='file';
    const consulted=[];let enriched=false,apiFinished=false,filterCalls=0,clock=1000000;
    const result=await runFlow({flow:selected,user:{username:'Davi'},jobDir:directory,rootFile,providers:{
        async *iterateReceita(){yield {rows};},
        async queryEnrichment(documents){assert.deepEqual(documents,ids);enriched=true;return [{cnpj:ids[0],phones:['2198765432']}];},
        async acquireApi(){return {
            async consult(documents){
                assert.equal(enriched,true);
                assert.equal(filterCalls,0);
                assert.equal(fs.existsSync(path.join(directory,'cleaning.jsonl.tmp')),false);
                consulted.push(...documents);
                return new Set(documents.filter(id=>id!==ids[4]));
            },
            async release(){apiFinished=true;},
        };},
        async queryPhones(kind,phones){assert.equal(apiFinished,true);filterCalls++;assert.ok(!phones.includes('41998765432'));return kind==='blocklist'?['3198765432']:[];},
        apiTiming:{now:()=>clock,sleep:async ms=>{clock+=ms;}},
    }});
    assert.deepEqual(consulted.sort(),ids);
    assert.ok(filterCalls>0);
    assert.equal(result.counts.apiConsulted,7);
    assert.equal(result.counts.apiAvailable,6);
    assert.equal(result.counts.apiClients,1);
    assert.equal(result.counts.removedRoot,1);
    assert.equal(result.counts.removedCnae,1);
    assert.equal(result.counts.removedBlocklist,1);
    assert.equal(result.counts.landlines,1);
    assert.equal(result.counts.withoutPhones,2);
    assert.equal(result.counts.kept,1);
    assert.equal(result.counts.exported,1);
    assert.equal(result.outputs[0].rows,1);
    const kept=fs.readFileSync(path.join(directory,'cleaning.jsonl'),'utf8').trim().split('\n').map(JSON.parse);
    assert.deepEqual(kept.map(r=>r.cnpj),[ids[0]]);
    assert.deepEqual(kept[0].phones,['21998765432']);
    const checkpointFile=path.join(directory,'checkpoint.json');
    const checkpoint=JSON.parse(fs.readFileSync(checkpointFile,'utf8'));
    assert.equal(checkpoint.stages.api.sourceStage,'enrichment');
    assert.equal(checkpoint.stages.cleaning.sourceStage,'api');
    checkpoint.stages.api.sourceStage='cleaning';fs.writeFileSync(checkpointFile,JSON.stringify(checkpoint));
    await assert.rejects(runFlow({flow:selected,user:{username:'Davi'},jobDir:directory,rootFile}),/filtros antes da API/);
});

test('disponíveis na API podem terminar em resultado vazio após os filtros finais',async t=>{
    const directory=fixture(t),selected=flow(directory);selected.enrichment.enabled=false;selected.cleaning.blocklist=false;
    let clock=1000000;
    const result=await runFlow({flow:selected,user:{username:'Davi'},jobDir:directory,providers:{
        async *iterateReceita(){yield {rows:[{cnpj:'00000000000001',phones:[]},{cnpj:'00000000000002',phones:[]}]};},
        async acquireApi(){return {async consult(documents){return new Set(documents);},async release(){}};},
        apiTiming:{now:()=>clock,sleep:async ms=>{clock+=ms;}},
    }});
    assert.equal(result.counts.apiAvailable,2);
    assert.equal(result.counts.cleaned,0);
    assert.equal(result.status,'empty');
    assert.deepEqual(result.outputs,[]);
});
