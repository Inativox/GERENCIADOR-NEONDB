const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { normalizarTelefoneFluxo, variantesTelefoneFluxo } = require('../src/main/flows/telefones');
const { runFlow } = require('../src/main/flows/pipeline');

test('fluxo aplica a mesma regra de nono dígito do exportador anterior e preserva fixos', () => {
    for (const first of ['6','7','8','9']) {
        const legacy = `21${first}2345678`;
        const r = normalizarTelefoneFluxo(legacy);
        assert.equal(r.phone, `219${first}2345678`);
        assert.equal(r.ninthDigitAdded, true);
        assert.equal(r.landline, false);
        assert.equal(normalizarTelefoneFluxo(r.phone).ninthDigitAdded, false);
    }
    assert.equal(normalizarTelefoneFluxo('1132345678').landline, true);
    assert.equal(normalizarTelefoneFluxo('1112345678').landline, false);
    assert.equal(normalizarTelefoneFluxo('1112345678').phone, '');
    assert.equal(normalizarTelefoneFluxo('551198765432').phone, '11998765432');
    assert.equal(normalizarTelefoneFluxo('5598765432').phone, '55998765432');
    assert.equal(normalizarTelefoneFluxo('98123456').phone, '');
    assert.equal(normalizarTelefoneFluxo('2199999999').phone, '');
    assert.equal(normalizarTelefoneFluxo('2198765432',{ajustarNonoDigito:false}).phone,'2198765432');
    assert.equal(normalizarTelefoneFluxo('2198765432',{ajustarNonoDigito:false}).ninthDigitAdded,false);
});

test('arquivo da geração está normalizado antes da primeira consulta de enriquecimento', async t => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-generation-phone-test-'));
    t.after(() => fs.rmSync(directory,{recursive:true,force:true}));
    const flow = {name:'Synthetic',operation:'c6',generation:{},enrichment:{enabled:true,strategy:'append'},cleaning:{enabled:false,rootSource:'none',blocklist:false},output:{formatId:'padrao',rowsPerFile:100,directory}};
    let enrichmentCalls=0;
    const providers = {
        async *iterateReceita(){yield {rows:[{cnpj:'00000000000001',razao_social:'Synthetic',telefone_principal:'2198765432',telefone_secundario:'2181234567'}]};},
        async queryEnrichment(){
            enrichmentCalls++;
            const first=JSON.parse(fs.readFileSync(path.join(directory,'generation.jsonl'),'utf8').trim());
            assert.equal(first.telefone_principal,'21998765432');
            assert.equal(first.telefone_secundario,'21981234567');
            assert.deepEqual(first.phones,['21998765432','21981234567']);
            return [];
        },
    };
    const result = await runFlow({flow,user:{username:'Davi'},jobDir:directory,providers});
    assert.equal(result.status,'completed');
    assert.equal(enrichmentCalls,1);
    // Resuming a historical unnormalized generation must not reach later stages.
    const checkpointFile=path.join(directory,'checkpoint.json');
    const checkpoint=JSON.parse(fs.readFileSync(checkpointFile,'utf8'));
    delete checkpoint.stages.generation.phoneFormatVersion;
    fs.writeFileSync(checkpointFile,JSON.stringify(checkpoint));
    await assert.rejects(runFlow({flow,user:{username:'Davi'},jobDir:directory,providers}), /Reinicie a geração/);
    assert.equal(enrichmentCalls,1);
});

test('variantes consultam o número antigo e o normalizado com e sem DDI', () => {
    assert.deepEqual(variantesTelefoneFluxo('2198765432'), ['21998765432','5521998765432','2198765432','552198765432']);
    assert.deepEqual(variantesTelefoneFluxo('21912345678'), ['21912345678','5521912345678']);
});

test('empresa sem telefone na Receita chega ao enriquecimento e só é descartada após todos os contatos serem avaliados', async t => {
    const directory=fs.mkdtempSync(path.join(os.tmpdir(),'flow-enrich-before-clean-test-'));
    t.after(()=>fs.rmSync(directory,{recursive:true,force:true}));
    const cnpjs=['00000000000001','00000000000002','00000000000003'];
    const rows=[
        {cnpj:cnpjs[0],razao_social:'Synthetic A',phones:[],telefone_principal:'',telefone_secundario:''},
        {cnpj:cnpjs[1],razao_social:'Synthetic B',phones:['1132345678']},
        {cnpj:cnpjs[2],razao_social:'Synthetic C',phones:[]},
    ];
    let enrichmentCompleted=false, filterCalls=0;
    const flow={name:'Synthetic',operation:'c6',generation:{phone:'all'},enrichment:{enabled:true,strategy:'append'},cleaning:{enabled:true,rootSource:'none',blocklist:true,invalidPhones:true,removeLandlines:true,prohibitedCnaes:[]},output:{formatId:'padrao',rowsPerFile:100,directory}};
    const result=await runFlow({flow,user:{username:'Davi'},jobDir:directory,providers:{
        async *iterateReceita({filters}){assert.equal(filters.phone,'all');yield {rows};},
        async queryEnrichment(documents){
            assert.deepEqual(documents,cnpjs);
            const generated=fs.readFileSync(path.join(directory,'generation.jsonl'),'utf8').trim().split('\n').map(JSON.parse);
            assert.equal(generated.length,3);
            assert.deepEqual(generated[0].phones,[]);
            assert.equal(filterCalls,0);
            enrichmentCompleted=true;
            return [{cnpj:cnpjs[0],phones:['2198765432']},{cnpj:cnpjs[1],phones:['2181234567']}];
        },
        async queryPhones(){assert.equal(enrichmentCompleted,true);filterCalls++;return [];},
    }});
    assert.equal(result.status,'completed');
    assert.ok(filterCalls>0);
    const enriched=fs.readFileSync(path.join(directory,'enrichment.jsonl'),'utf8').trim().split('\n').map(JSON.parse);
    assert.equal(enriched.length,3);
    assert.deepEqual(enriched[0].phones,['21998765432']);
    const cleaned=fs.readFileSync(path.join(directory,'cleaning.jsonl'),'utf8').trim().split('\n').map(JSON.parse);
    assert.deepEqual(cleaned.map(r=>r.cnpj),cnpjs.slice(0,2));
    assert.deepEqual(cleaned.map(r=>r.phones),[['21998765432'],['21981234567']]);
    assert.equal(result.counts.withoutPhones,1);
    assert.equal(result.counts.withoutPhonesBeforeFilters,1);
    assert.equal(result.counts.withoutPhonesAfterFilters,0);
    assert.equal(result.counts.landlines,1);
});

test('limpeza preserva celular antigo, bloqueia grafia antiga e separa motivos de linhas sem telefone', async t => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-phone-test-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const phoneLists = [ ['2198765432'], ['1132345678'], [], ['2181234567'], ['2162345678'], ['21998765432'], ['21981230000'], ['98123456'] ];
    const rows = phoneLists.map((phones,i) => ({cnpj:String(i+1).padStart(14,'0'),razao_social:'Synthetic',phones}));
    const calls = [];
    const flow = {name:'Synthetic',operation:'c6',generation:{},enrichment:{enabled:false},cleaning:{enabled:true,rootSource:'none',blocklist:true,invalidPhones:true,removeLandlines:true,prohibitedCnaes:[]},output:{formatId:'padrao',rowsPerFile:100,includeSituacao:false,directory}};
    const result = await runFlow({flow,user:{username:'Davi'},jobDir:directory,providers:{
        async *iterateReceita() { yield {rows}; },
        async queryPhones(kind, phones) { calls.push({kind,phones}); return kind === 'blocklist' ? ['552181234567'] : ['2162345678']; },
    }});
    assert.equal(result.status,'completed');
    assert.equal(result.counts.kept,2);
    assert.equal(result.counts.landlines,1);
    assert.equal(result.counts.ninthDigitAdded,undefined);
    assert.equal(result.counts.removedBlocklist,1);
    assert.equal(result.counts.invalidPhones,1);
    assert.equal(result.counts.withoutPhonesBeforeFilters,2);
    assert.equal(result.counts.withoutPhonesAfterFilters,2);
    assert.equal(result.counts.withoutPhonesRepeatedOnly,1);
    assert.equal(result.counts.withoutPhones,5);
    assert.equal(result.counts.withoutPhones, result.counts.withoutPhonesBeforeFilters + result.counts.withoutPhonesAfterFilters + result.counts.withoutPhonesRepeatedOnly);
    assert.ok(calls.every(c=>c.phones.includes('552181234567') && c.phones.includes('21981234567')));
    const kept = fs.readFileSync(path.join(directory,'cleaning.jsonl'),'utf8').trim().split('\n').map(JSON.parse);
    assert.deepEqual(kept.map(r=>r.phones), [['21998765432'], ['21981230000']]);
    const generated = fs.readFileSync(path.join(directory,'generation.jsonl'),'utf8').trim().split('\n').map(JSON.parse);
    assert.equal(generated[0].phones[0], '21998765432');
    assert.equal(generated[3].phones[0], '21981234567');
    assert.equal(JSON.parse(fs.readFileSync(path.join(directory,'checkpoint.json'),'utf8')).stages.generation.phoneFormatVersion,1);
});
