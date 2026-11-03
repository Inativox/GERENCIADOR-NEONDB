const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { EventEmitter } = require('node:events');
const { validateFlow, effectiveFlow } = require('../src/main/flows/config');
const { createFlowManager } = require('../src/main/flows/manager');
const { listPresets, getPreset, localDate, PROHIBITED_CNAES, MAQUININHA_CNAES } = require('../src/main/flows/presets');
const { getFormat } = require('../src/main/flows/formats');
const { PROHIBITED_CNAES: APP_PROHIBITED_CNAES } = require('../src/main/database/connection');
const tick = () => new Promise(resolve => setTimeout(resolve, 5));
const NOW = new Date(2026, 10, 3, 23, 30);

function fixture(t) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-presets-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const workers = [];
    const manager = createFlowManager({ baseDirectory: directory, getUser: () => ({ username: 'Outro', role: 'admin' }),
        resolveFastwayCredentials: () => ({ login: 'synthetic', password: 'synthetic' }),
        resolveConnections: async () => ({ enrichment: 'synthetic' }),
        resolveRoot: async () => ({ documents: [], info: { source: 'bq', count: 0, queriedAt: 'synthetic' } }),
        workerFactory: data => { const worker = new EventEmitter(); worker.data = data; worker.postMessage = () => {}; worker.terminate = async () => { worker.emit('exit', 1); }; workers.push(worker); return worker; } });
    return { manager, directory, workers };
}

test('quatro presets fixos, um por operação', () => {
    assert.deepEqual(listPresets(NOW).map(item => [item.id, item.name, item.operation, item.pipelines[0], item.fastway.campaignId]), [
        ['preset-c6-bank', 'C6 BANK', 'c6', 90, '120'], ['preset-mercado-pago', 'MERCADO PAGO', 'mercadopago', 58, '18'],
        ['preset-pagbank', 'PAGBANK', 'pagbank', 34, '87'], ['preset-santander', 'SANTANDER', 'santander', 119, '63']]);
    for (const item of listPresets(NOW)) {
        assert.equal(item.preset, true);
        assert.deepEqual(item.enrichment, { enabled: true, strategy: 'append', fillCpf: false }, item.name);
        assert.equal(item.api.enabled, false);
        assert.equal(item.cleaning.blocklist, true, item.name);
        assert.deepEqual(item.cleaning.prohibitedCnaes, [...PROHIBITED_CNAES]);
        assert.equal(item.output.csv, true);
    }
});

test('preset C6 BANK traz a especificação padrão da listagem do C6', () => {
    const preset = getPreset('preset-c6-bank', NOW);
    assert.deepEqual(preset.generation, { limit: null, uf: [], cidade: [], bairro: [], cnaes: [], machineCnaes: false, naturezas: [], dateFrom: '2000-01-01', dateTo: '2026-11-03',
        mei: 'no', phone: 'all', email: 'all', situacoes: ['02'], availability: 'available', source: 'bq' });
    assert.deepEqual(preset.cleaning, { enabled: true, rootSource: 'bq', rootFile: '', blocklist: true, invalidPhones: true, removeLandlines: true, fillLivre5: true, prohibitedCnaes: [...PROHIBITED_CNAES] });
    assert.deepEqual(preset.output, { fileName: 'RCAN', formatId: 'padrao_8_fones', csv: true, rowsPerFile: 250000, includeSituacao: false });
    // Os 65 CNAEs proibidos são os mesmos da lista do aplicativo, com 7 dígitos.
    assert.equal(PROHIBITED_CNAES.length, 65);
    assert.deepEqual([...PROHIBITED_CNAES].sort(), [...APP_PROHIBITED_CNAES].map(value => value.padStart(7, '0')).sort());
});

test('Mercado Pago, PagBank e Santander seguem os fluxos salvos', () => {
    const mp = getPreset('preset-mercado-pago', NOW), pagbank = getPreset('preset-pagbank', NOW), santander = getPreset('preset-santander', NOW);
    assert.equal(MAQUININHA_CNAES.length, 48);
    assert.deepEqual([mp.generation.limit, mp.generation.dateTo, mp.generation.mei, mp.cleaning.enabled, mp.output.fileName, mp.output.formatId, mp.output.rowsPerFile], [150000, '2023-12-31', 'no', true, 'MP PADRÃO', 'padrao_8_fones', 150000]);
    assert.deepEqual(mp.generation.cnaes, [...MAQUININHA_CNAES]);
    assert.deepEqual([pagbank.generation.limit, pagbank.generation.dateTo, pagbank.generation.mei, pagbank.cleaning.enabled, pagbank.output.fileName, pagbank.output.formatId, pagbank.output.rowsPerFile], [150000, '2026-11-03', 'yes', true, 'PAGBANK PADRAO', 'olos', 150000]);
    assert.deepEqual(pagbank.generation.cnaes, [...MAQUININHA_CNAES]);
    assert.deepEqual([santander.generation.limit, santander.generation.dateTo, santander.generation.cnaes, santander.cleaning.enabled, santander.output.fileName, santander.output.formatId, santander.output.rowsPerFile], [200000, '2026-11-03', [], true, 'SANTANDER PADRAO', 'padrao_8_fones', 250000]);
});

test('a abertura vai até o dia corrente local', () => {
    assert.equal(localDate(new Date(2026, 0, 5, 0, 1)), '2026-01-05');
    assert.equal(getPreset('preset-santander', new Date(2026, 11, 31, 23, 59)).generation.dateTo, '2026-12-31');
});

test('todos os presets passam na validação de fluxo e resolvem o layout', () => {
    for (const preset of listPresets()) {
        const validated = validateFlow(preset);
        for (const key of ['generation', 'enrichment', 'cleaning', 'fastway', 'output']) assert.deepEqual(validated[key], preset[key], `${preset.name} ${key}`);
        assert.ok(effectiveFlow(preset, { username: 'Outro' }).output.formatSnapshot.colunas.length);
    }
    const snapshot = effectiveFlow(getPreset('preset-c6-bank'), { username: 'Outro' });
    assert.deepEqual(snapshot.output.formatSnapshot.colunas.map(column => column.header),
        ['nome', 'cpf', 'livre1', 'chave', 'livre3', 'livre4', 'livre5', 'livre7', 'fone1', 'fone2', 'fone3', 'fone4', 'fone5', 'fone6', 'fone7', 'fone8']);
    assert.ok(!snapshot.output.formatSnapshot.colunas.some(column => column.header === 'livre6'));
    assert.equal(getFormat('padrao_8_fones').nome, 'LAYOUT PADRÃO · 8 telefones');
});

test('presets aparecem no bootstrap, não podem ser salvos nem excluídos e executam com a data do dia', async t => {
    const f = fixture(t);
    const bootstrap = f.manager.bootstrap();
    assert.deepEqual(bootstrap.presets.map(preset => preset.id), ['preset-c6-bank', 'preset-mercado-pago', 'preset-pagbank', 'preset-santander']);
    assert.deepEqual(bootstrap.flows, []);
    assert.throws(() => f.manager.save({ ...getPreset('preset-c6-bank'), name: 'Alterado' }), /Presets não podem ser editados/);
    assert.throws(() => f.manager.delete('preset-santander'), /Presets não podem ser excluídos/);
    // A cópia (Duplicar) vira um fluxo do modo livre, editável.
    const copy = f.manager.save({ ...getPreset('preset-c6-bank'), id: '', revision: 0, name: 'C6 BANK · cópia' });
    assert.equal(copy.preset, undefined);
    assert.deepEqual(f.manager.bootstrap().flows.map(flow => flow.name), ['C6 BANK · cópia']);
    const job = f.manager.start({ flowId: 'preset-c6-bank', outputDirectory: f.directory });
    assert.equal(job.flowName, 'C6 BANK');
    assert.equal(job.flowSnapshot.generation.dateTo, localDate());
    assert.equal(job.flowSnapshot.output.directory, f.directory);
    for (let n = 0; n < 100 && !f.workers.length; n++) await tick();
    f.workers[0].emit('message', { type: 'result', data: { status: 'empty', outputs: [] } });
    for (let n = 0; n < 100 && f.manager.isBusy(); n++) await tick();
});
