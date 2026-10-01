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
});

test('variantes consultam o número antigo e o normalizado com e sem DDI', () => {
    assert.deepEqual(variantesTelefoneFluxo('2198765432'), ['21998765432','5521998765432','2198765432','552198765432']);
    assert.deepEqual(variantesTelefoneFluxo('21912345678'), ['21912345678','5521912345678']);
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
    assert.equal(result.counts.ninthDigitAdded,3);
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
});
