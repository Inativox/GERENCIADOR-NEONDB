'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { iterateReceita } = require('../src/main/flows/receita');
const code = index => String(index).padStart(14, '0');
const company = (index, matches = false) => ({ cnpj: code(index), razao_social: 'Sintetica ' + index, situacao_cadastral_cod: '02', atividade_principal_cod: matches ? '1091102' : '9999999', opcao_mei: 'N', data_abertura: '2022-01-01', telefone_principal: '11987654321' });

// Evaluate filters before LIMIT over synthetic companies, including the old
// source-window query so these tests expose small candidate-page regressions.
function fixturePool(source, { failAt, onRead } = {}) {
    const calls = [];
    return { calls, async query(sql, values) {
        if (sql.includes('information_schema.columns')) return { rows: ['cnpj','razao_social','situacao_cadastral_cod','atividade_principal_cod','opcao_mei','data_abertura','telefone_principal'].map(column_name => ({ column_name, data_type: column_name === 'cnpj' ? 'character' : column_name === 'data_abertura' ? 'date' : 'text' })) };
        const candidates = source.filter(row => row.cnpj > values[0]);
        if (sql.includes('__scan_end')) {
            const selected = candidates.slice(0, values.at(-1));
            return { rows: [{ __scan_end: selected.at(-1)?.cnpj || null, __scan_count: selected.length }] };
        }
        calls.push({ after: values[0], size: values.at(-1) });
        if (failAt === values[0]) throw Object.assign(new Error('Synthetic interruption'), { code: '57014' });
        const parameter = expression => { const match = sql.match(expression); return match && values[Number(match[1]) - 1]; };
        const end = parameter(/e\."cnpj" <= \$(\d+)::bpchar/);
        const cnaes = parameter(/e\."atividade_principal_cod"::text = ANY\(\$(\d+)::text\[\]\)/);
        const mei = parameter(/upper\(e\."opcao_mei"::text\) = \$(\d+)/);
        const from = parameter(/e\."data_abertura"::date >= \$(\d+)::date/);
        const through = parameter(/e\."data_abertura"::date <= \$(\d+)::date/);
        const rows = candidates.filter(row => (!end || row.cnpj <= end) && values[1].includes(row.situacao_cadastral_cod)
            && (!cnaes || cnaes.includes(row.atividade_principal_cod)) && (!mei || row.opcao_mei === mei || row.opcao_mei == null)
            && (!from || row.data_abertura >= from) && (!through || row.data_abertura <= through)
            && (!sql.includes('btrim(e."telefone_principal"') || Boolean(row.telefone_principal?.trim()))).slice(0, values.at(-1));
        onRead?.();
        return { rows };
    }};
}
async function pages(options) { const result = []; for await (const page of iterateReceita(options)) result.push(page); return result; }

test('a 50000-row CNAE page counts eligible companies beyond discarded candidate ranges', async () => {
    const source = Array.from({length:150002}, (_, index) => company(index + 1, index >= 100000));
    const result = await pages({pool:fixturePool(source), filters:{cnaes:['1091102']}, batchSize:50000});
    assert.deepEqual(result.map(page=>page.rows.length), [50000,2]);
    assert.deepEqual(result.map(page=>page.cursor), [code(150000),code(150002)]);
    assert.deepEqual(result.flatMap(page=>page.rows.map(row=>row.cnpj)), source.slice(100000).map(row=>row.cnpj));
});

test('a partial total limit resumes after the last eligible company without skipping later matches', async () => {
    const source = Array.from({length:52004}, (_, index) => company(index + 1, index % 2 === 0));
    const first = await pages({pool:fixturePool(source), filters:{cnaes:['1091102'],limit:3}, batchSize:50000});
    assert.deepEqual(first.flatMap(page=>page.rows.map(row=>row.cnpj)), [code(1),code(3),code(5)]);
    const resumed = await pages({pool:fixturePool(source), filters:{cnaes:['1091102'],limit:3}, batchSize:50000, afterCnpj:first.at(-1).cursor});
    assert.deepEqual(resumed.flatMap(page=>page.rows.map(row=>row.cnpj)), [code(7),code(9),code(11)]);
});

test('sparse filters return a final partial batch without emitting empty candidate batches', async () => {
    const expected = [2,3,50000,50001,52004];
    const source = Array.from({length:52004}, (_,index)=>company(index+1,expected.includes(index+1)));
    const result = await pages({pool:fixturePool(source), filters:{cnaes:['1091102']}, batchSize:50000});
    assert.deepEqual(result.map(page=>page.rows.length), [5]);
    assert.deepEqual(result[0].rows.map(row=>row.cnpj), expected.map(code));
});

test('a legacy cursor confirmed at an empty candidate range resumes eligible companies safely', async () => {
    const source = Array.from({length:52004}, (_,index)=>company(index+1,index>=50000));
    const result = await pages({pool:fixturePool(source), filters:{cnaes:['1091102']}, batchSize:50000, afterCnpj:code(50000)});
    assert.deepEqual(result.flatMap(page=>page.rows.map(row=>row.cnpj)), source.slice(50000).map(row=>row.cnpj));
});

test('interruption after a full eligible batch resumes only the remaining companies', async () => {
    const source = Array.from({length:50002}, (_,index)=>company(index+1,true));
    const iterator = iterateReceita({pool:fixturePool(source,{failAt:code(50000)}), filters:{cnaes:['1091102']}, batchSize:50000});
    const first = (await iterator.next()).value;
    assert.equal(first.rows.length,50000);
    await assert.rejects(iterator.next(),/interrompida pelo banco/);
    const resumed = await pages({pool:fixturePool(source), filters:{cnaes:['1091102']}, batchSize:50000, afterCnpj:first.cursor});
    assert.deepEqual(resumed.flatMap(page=>page.rows.map(row=>row.cnpj)), [code(50001),code(50002)]);
});

test('cancellation discards a returned eligible batch before yielding or advancing saved progress', async () => {
    const controller = new AbortController();
    const pool = fixturePool([company(1,true)],{onRead:()=>controller.abort()});
    await assert.rejects(pages({pool,filters:{cnaes:['1091102']},batchSize:50000,signal:controller.signal}),{name:'AbortError'});
    assert.equal(pool.calls.length,1);
});

test('active status, selected CNAEs, dates, phone and non-MEI apply before the eligible limit', async () => {
    const source = [
        {...company(1,true),situacao_cadastral_cod:'08'}, company(2,true),
        {...company(3,true),opcao_mei:'S'}, company(4,false),
        {...company(5,true),data_abertura:'2010-01-01'}, {...company(6,true),opcao_mei:null},
        {...company(7,true),telefone_principal:'   '}, {...company(8,true),data_abertura:'2027-01-01'}
    ];
    const result = await pages({pool:fixturePool(source), filters:{cnaes:['1091102'],situacoes:['02'],mei:'no',phone:'with',dateFrom:'2020-01-01',dateTo:'2026-12-31',limit:2}, batchSize:50000});
    assert.deepEqual(result.flatMap(page=>page.rows.map(row=>row.cnpj)), [code(2),code(6)]);
});
