'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { iterateReceita } = require('../src/main/flows/receita');
const code = index => String(index).padStart(14, '0');
const company = (index, matches = false) => ({ cnpj: code(index), razao_social: `Sintetica ${index}`, situacao_cadastral_cod: '02', atividade_principal_cod: matches ? '1091102' : '9999999', opcao_mei: 'N' });

// This fixture makes the original unbounded query time out. It evaluates the
// requested CNPJ range and CNAE selection over synthetic rows, without a DB.
function windowPool(source, { failAt, cancel, maxRange = Infinity } = {}) {
    const calls = [];
    return { calls, async query(sql, values) {
        if (sql.includes('information_schema.columns')) return { rows: ['cnpj','razao_social','situacao_cadastral_cod','atividade_principal_cod','opcao_mei'].map(column_name => ({ column_name, data_type: column_name === 'cnpj' ? 'character' : 'text' })) };
        const candidates = source.filter(row => row.cnpj > values[0]);
        if (sql.includes('__scan_end')) {
            const selected = candidates.slice(0, values.at(-1));
            calls.push({ kind: 'boundary', after: values[0], size: values.at(-1) });
            return { rows: [{ __scan_end: selected.at(-1)?.cnpj || null, __scan_count: selected.length }] };
        }
        const parameter = sql.match(/e\."cnpj" <= \$(\d+)::bpchar/);
        const end = parameter && values[Number(parameter[1]) - 1];
        calls.push({ kind: 'read', after: values[0], end, size: values.at(-1) });
        if (!end || (failAt && values[0] === failAt)) throw Object.assign(new Error('Synthetic database timeout'), { code: '57014' });
        const range = candidates.filter(row => row.cnpj <= end);
        if (range.length > maxRange) { cancel?.(); throw Object.assign(new Error('Synthetic database timeout'), { code: '57014' }); }
        const mei = sql.match(/upper\(e\."opcao_mei"::text\) = \$(\d+)/);
        return { rows: range.filter(row => values[1].includes(row.situacao_cadastral_cod) && values[2].includes(row.atividade_principal_cod) && (!mei || row.opcao_mei === values[Number(mei[1]) - 1] || row.opcao_mei == null)).slice(0, values.at(-1)) };
    }};
}
async function pages(options) { const result = []; for await (const page of iterateReceita(options)) result.push(page); return result; }

test('sparse CNAE generation crosses an empty source window and returns all later matches', async () => {
    // Removing the source bound would restore the original timeout.
    const source = Array.from({length:12004}, (_, index) => company(index + 1, [10001,10002,12004].includes(index + 1)));
    const pool = windowPool(source);
    const result = await pages({pool, filters:{cnaes:['1091102'],limit:300000},batchSize:100000});
    assert.deepEqual(result.flatMap(page=>page.rows.map(row=>row.cnpj)), [code(10001),code(10002),code(12004)]);
    assert.deepEqual(result[0], {rows:[],cursor:code(10000)});
    assert.equal(result.at(-1).cursor, code(12004));
    assert.ok(pool.calls.filter(call=>call.kind==='boundary').every(call=>call.size<=10000));
});

test('a partially consumed source window resumes after the last emitted company without skipping matches', async () => {
    const source = Array.from({length:12004}, (_, index) => company(index + 1, true));
    const first = await pages({pool:windowPool(source),filters:{cnaes:['1091102'],limit:3},batchSize:100000});
    assert.deepEqual(first.flatMap(page=>page.rows.map(row=>row.cnpj)), [code(1),code(2),code(3)]);
    assert.equal(first.at(-1).cursor, code(3));
    const resumed = await pages({pool:windowPool(source),filters:{cnaes:['1091102'],limit:3},batchSize:100000,afterCnpj:first.at(-1).cursor});
    assert.deepEqual(resumed.flatMap(page=>page.rows.map(row=>row.cnpj)), [code(4),code(5),code(6)]);
});

test('small output pages preserve every match across source boundaries', async () => {
    const expected = [2,3,10000,10001,12004];
    const source = Array.from({length:12004}, (_,index)=>company(index+1,expected.includes(index+1)));
    const result = await pages({pool:windowPool(source),filters:{cnaes:['1091102']},batchSize:2});
    assert.deepEqual(result.flatMap(page=>page.rows.map(row=>row.cnpj)),expected.map(code));
    assert.ok(result.every(page=>page.rows.length<=2));
});

test('failure after an empty window keeps a resumable cursor before unread matches', async () => {
    const source = Array.from({length:12004}, (_,index)=>company(index+1,index>=10000));
    const pool = windowPool(source,{failAt:code(10000)});
    const iterator = iterateReceita({pool,filters:{cnaes:['1091102']},batchSize:100000});
    const first = (await iterator.next()).value;
    assert.deepEqual(first,{rows:[],cursor:code(10000)});
    await assert.rejects(iterator.next(),/tempo limite/);
    const resumed = await pages({pool:windowPool(source),filters:{cnaes:['1091102']},batchSize:100000,afterCnpj:first.cursor});
    assert.deepEqual(resumed.flatMap(page=>page.rows.map(row=>row.cnpj)),source.slice(10000).map(row=>row.cnpj));
});

test('a bounded timeout retries a smaller source window from the same cursor', async () => {
    const source = Array.from({length:7000}, (_,index)=>company(index+1,index%3===0));
    const pool = windowPool(source,{maxRange:5000});
    const result = await pages({pool,filters:{cnaes:['1091102']},batchSize:100000});
    assert.deepEqual(result.flatMap(page=>page.rows.map(row=>row.cnpj)),source.filter(row=>row.atividade_principal_cod==='1091102').map(row=>row.cnpj));
    const reads = pool.calls.filter(call=>call.kind==='read');
    assert.equal(reads[0].after,reads[1].after);
    assert.ok(reads[1].end<reads[0].end);
});

test('cancellation during a bounded timeout stops before retrying or advancing the cursor', async () => {
    const controller = new AbortController();
    const source = Array.from({length:7000},(_,index)=>company(index+1,true));
    const pool = windowPool(source,{maxRange:5000,cancel:()=>controller.abort()});
    await assert.rejects(pages({pool,filters:{cnaes:['1091102']},batchSize:100000,signal:controller.signal}),{name:'AbortError'});
    assert.equal(pool.calls.filter(call=>call.kind==='read').length,1);
});

test('MP SQL keeps active status, selected CNAEs and non-MEI filters inside every source range', async () => {
    const source = [
        {...company(1,true),situacao_cadastral_cod:'08'},
        company(2,true),
        {...company(3,true),opcao_mei:'S'},
        company(4,false),
        {...company(5,true),situacao_cadastral_cod:'04'},
        {...company(6,true),opcao_mei:null},
    ];
    const result = await pages({pool:windowPool(source),filters:{cnaes:['1091102'],situacoes:['02'],mei:'no'},batchSize:100000});
    assert.deepEqual(result.flatMap(page=>page.rows.map(row=>row.cnpj)),[code(2),code(6)]);
});
