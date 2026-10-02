const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const XLSX = require('xlsx');
const loadModule = require('./helpers/loadModule');

function fixture(t, query) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'limpeza-queue-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const files = ['primeira.xlsx', 'segunda.xlsx'].map((name, index) => {
        const file = path.join(directory, name);
        const workbook = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet([
            ['cnpj', 'nome'], [String(12345678000199 + index), `123 Empresa ${index}`]
        ]), 'Base');
        XLSX.writeFile(workbook, file);
        return { path: file, id: String(index) };
    });
    const listeners = new Map();
    const handlers = new Map();
    const messages = [];
    const reads = [];
    let pendingReads = 0;
    let peakReads = 0;
    let failRead = false;
    let queries = 0;
    const state = { currentUser: { username: 'Davi', role: 'admin' }, pool: null };
    const sender = { isDestroyed: () => false, send: (channel, payload) => messages.push({ channel, payload }) };
    state.mainWindow = { webContents: sender };
    const cleaning = loadModule('src/main/handlers/limpeza.js', {
        electron: { ipcMain: { on: (channel, callback) => listeners.set(channel, callback), handle: (channel, callback) => handlers.set(channel, callback) }, dialog: {}, shell: {} },
        path, fs, xlsx: XLSX, exceljs: require('exceljs'), axios: {},
        '../state': state,
        '../limpezaTelefones': require('../src/main/limpezaTelefones'),
        '../flows/config': require('../src/main/flows/config'),
        '../database/connection': { PROHIBITED_CNAES: new Set(), queryWithRetry: async (...args) => { queries++; if (query) return query(...args); throw new Error('Banco não deve ser acessado'); }, logSystemAction() {} },
        './files': {
            readSpreadsheet: async file => {
                reads.push(path.basename(file));
                peakReads = Math.max(peakReads, ++pendingReads);
                await new Promise(resolve => setTimeout(resolve, 15));
                pendingReads--;
                if (failRead) throw new Error('Falha de leitura');
                return XLSX.readFile(file);
            },
            writeSpreadsheet: (workbook, file) => XLSX.writeFile(workbook, file),
            letterToIndex: value => value.toUpperCase().charCodeAt(0) - 65
        },
        '../database/cache': { getStoredCnpjs: () => new Set(['12345678000199']) },
        '../keyfile': {}
    }, { setImmediate, AbortController });
    cleaning.register();
    return {
        files, messages, reads, state,
        start: (args = {}, selectedSender = sender) => listeners.get('start-cleaning')({ sender: selectedSender }, { cleanFiles: files, ...args }),
        peak: () => peakReads,
        queries: () => queries,
        fail: value => { failRead = value; },
        rootOptions: (from = sender) => handlers.get('local-cleaning-root-options')({ sender: from }),
        logout: () => listeners.get('logout')()
    };
}

test('catálogo local oferece pipelines oficiais somente para o administrador da janela', t => {
    const f = fixture(t);
    const options = f.rootOptions();
    assert.equal(options.success, true);
    assert.deepEqual(Array.from(options.operations, item => [item.id, Array.from(item.pipelines)]), [['c6', [90]], ['santander', [119]], ['pagbank', [34]], ['mercadopago', [58]]]);
    assert.equal(f.rootOptions({}).success, false);
    f.state.currentUser.role = 'limited';
    assert.equal(f.rootOptions().success, false);
});

for (const [operation, pipeline] of [['c6', 90], ['santander', 119], ['pagbank', 34], ['mercadopago', 58]]) {
    test(`Auto Raiz BQ ${operation} carrega só o pipeline ${pipeline}, uma vez por lote, antes das planilhas`, async t => {
        const f = fixture(t);
        let loads = 0;
        f.state.bqRootService = { async loadRoot(pipelines, options) {
            loads++;
            assert.deepEqual(Array.from(pipelines), [pipeline]);
            assert.equal(f.reads.length, 0);
            options.onProgress({ documents: 1 });
            return { documents: ['12345678000199'] };
        } };
        await f.start({ isAutoRoot: true, autoRootSource: 'bq', autoRootOperation: operation, pipelines: [999] });
        assert.equal(loads, 1);
        assert.equal(f.queries(), 0);
        assert.equal(f.peak(), 1);
        const first = XLSX.readFile(f.files[0].path), second = XLSX.readFile(f.files[1].path);
        assert.deepEqual(XLSX.utils.sheet_to_json(first.Sheets.Base, { header: 1 }), [['cnpj', 'nome']]);
        assert.deepEqual(XLSX.utils.sheet_to_json(second.Sheets.Base, { header: 1 }), [['cnpj', 'nome'], ['12345678000200', 'Empresa']]);
        assert.equal(f.messages.at(-1).payload.success, true);
        const logs = f.messages.filter(message => message.channel === 'log').map(message => message.payload);
        assert.ok(logs.findIndex(log => log.includes('Raiz BQ carregada')) < logs.findIndex(log => log.includes('PROCESSANDO')));
    });
}

test('Auto Raiz antigo continua usando o banco sem exigir operação BQ', async t => {
    const f = fixture(t, async sql => { assert.equal(sql, 'SELECT cnpj FROM raiz_cnpjs'); return { rows: [{ cnpj: '12345678000199' }] }; });
    f.state.pool = {};
    await f.start({ isAutoRoot: true });
    assert.equal(f.queries(), 1);
    assert.equal(f.messages.at(-1).payload.success, true);
    const first = XLSX.readFile(f.files[0].path);
    assert.equal(XLSX.utils.sheet_to_json(first.Sheets.Base, { header: 1 }).length, 1);
});

test('raiz BQ vazia, indisponível ou falha preserva todos os originais e libera a fila, sem fallback para o banco', async t => {
    for (const load of [null, async () => ({ documents: [] }), async () => { throw Object.assign(new Error('Renove o login Google.'), { code: 'BQ_AUTH_REQUIRED' }); }]) {
        const f = fixture(t);
        f.state.pool = {};
        f.state.bqRootService = load ? { loadRoot: load } : null;
        const originals = f.files.map(file => fs.readFileSync(file.path));
        await f.start({ isAutoRoot: true, autoRootSource: 'bq', autoRootOperation: 'c6', backup: true });
        assert.equal(f.messages.at(-1).payload.success, false);
        assert.equal(f.reads.length, 0);
        assert.equal(f.queries(), 0);
        f.files.forEach((file, index) => assert.deepEqual(fs.readFileSync(file.path), originals[index]));
        await f.start();
        assert.equal(f.messages.at(-1).payload.success, true);
    }
});

test('fonte ou pipeline inválido é rejeitado antes de qualquer consulta ou alteração', async t => {
    for (const selection of [{ autoRootSource: 'outro' }, { autoRootSource: 'bq' }, { autoRootSource: 'bq', autoRootOperation: 'other' }]) {
        const f = fixture(t);
        await f.start({ isAutoRoot: true, ...selection });
        assert.equal(f.messages.at(-1).payload.success, false);
        assert.equal(f.reads.length, 0);
        assert.equal(f.queries(), 0);
    }
});

test('BQ não dispensa a conexão para a blocklist obrigatória de outros usuários', async t => {
    const f = fixture(t);
    f.state.currentUser.username = 'Outro';
    let loads = 0;
    f.state.bqRootService = { async loadRoot() { loads++; return { documents: ['12345678000199'] }; } };
    await f.start({ isAutoRoot: true, autoRootSource: 'bq', autoRootOperation: 'c6', checkBlocklist: false });
    assert.equal(loads, 0);
    assert.equal(f.messages.at(-1).payload.success, false);
    assert.equal(f.reads.length, 0);
});

test('logout durante a consulta BQ cancela a raiz antes de alterar qualquer arquivo e impede lote concorrente', async t => {
    const f = fixture(t);
    let release, signal;
    f.state.bqRootService = { loadRoot(_pipelines, options) { signal = options.signal; return new Promise(resolve => { release = resolve; }); } };
    const active = f.start({ isAutoRoot: true, autoRootSource: 'bq', autoRootOperation: 'santander' });
    assert.equal(signal.aborted, false);
    await f.start();
    assert.equal(f.messages.filter(message => message.channel === 'cleaning-finished').length, 0);
    f.logout();
    assert.equal(signal.aborted, true);
    release({ documents: ['12345678000199'] });
    await active;
    assert.equal(f.reads.length, 0);
    assert.equal(f.messages.at(-1).payload.success, false);
});

test('raiz BQ cruza CNPJ com zero inicial perdido no Excel e CNPJ alfanumérico', async t => {
    const f = fixture(t);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet([
        ['cnpj', 'nome'], [4252011000110, 'Já na raiz'], ['12ABC34501DE35', 'Já na raiz alfa'], ['22345678000199', 'Manter']
    ]), 'Base');
    XLSX.writeFile(workbook, f.files[0].path);
    f.state.bqRootService = { async loadRoot() { return { documents: ['04252011000110', '12ABC34501DE35'] }; } };
    await f.start({ isAutoRoot: true, autoRootSource: 'bq', autoRootOperation: 'c6', cleanFiles: [f.files[0]] });
    const result = XLSX.readFile(f.files[0].path);
    assert.deepEqual(XLSX.utils.sheet_to_json(result.Sheets.Base, { header: 1 }), [['cnpj', 'nome'], ['22345678000199', 'Manter']]);
    assert.equal(f.messages.at(-1).payload.success, true);
});

test('limpa uma lista por vez e conclui seus logs antes de iniciar a seguinte', async t => {
    const f = fixture(t);
    await f.start();
    assert.equal(f.peak(), 1);
    assert.deepEqual(f.reads, ['primeira.xlsx', 'segunda.xlsx']);
    const logs = f.messages.filter(m => m.channel === 'log').map(m => m.payload);
    const firstFinished = logs.findIndex(log => log.includes('primeira.xlsx') && log.includes('Total final:'));
    const secondStarted = logs.findIndex(log => log.includes('PROCESSANDO') && log.includes('segunda.xlsx'));
    assert.ok(firstFinished >= 0 && secondStarted > firstFinished);
    assert.equal(f.messages.at(-1).channel, 'cleaning-finished');
    assert.equal(f.messages.at(-1).payload.success, true);
});

test('recusa um segundo lote enquanto a limpeza está em andamento', async t => {
    const f = fixture(t);
    const first = f.start();
    await f.start();
    assert.equal(f.messages.filter(m => m.channel === 'cleaning-finished').length, 0);
    await first;
    assert.equal(f.reads.length, 2);
    assert.equal(f.messages.filter(m => m.channel === 'cleaning-finished').length, 1);
});

test('opções antigas de consultar/salvar CNPJs não acessam o banco nem removem históricos', async t => {
    const f = fixture(t);
    await f.start({ checkDb: true, saveToDb: true });
    assert.equal(f.reads.length, 2);
    assert.equal(f.queries(), 0);
    const workbook = XLSX.readFile(f.files[0].path);
    const rows = XLSX.utils.sheet_to_json(workbook.Sheets.Base, { header: 1 });
    assert.equal(rows.length, 2);
    assert.equal(f.messages.at(-1).payload.success, true);
});

test('libera a fila depois de erro e devolve conclusão com falha', async t => {
    const f = fixture(t);
    f.fail(true);
    await f.start();
    assert.equal(f.messages.at(-1).channel, 'cleaning-finished');
    assert.equal(f.messages.at(-1).payload.success, false);
    f.fail(false);
    await f.start();
    assert.equal(f.messages.at(-1).payload.success, true);
});

test('outra janela recebe falha de lote ocupado sem encerrar o proprietário', async t => {
    const f = fixture(t);
    const first = f.start();
    const otherMessages = [];
    await f.start({}, { isDestroyed: () => false, send: (channel, payload) => otherMessages.push({ channel, payload }) });
    assert.equal(otherMessages.at(-1).channel, 'cleaning-finished');
    assert.equal(otherMessages.at(-1).payload.success, false);
    assert.equal(f.messages.some(m => m.channel === 'cleaning-finished'), false);
    await first;
    assert.equal(f.reads.length, 2);
    assert.equal(f.messages.at(-1).payload.success, true);
});

test('janela destruída não derruba a limpeza nem prende o próximo lote', async t => {
    const f = fixture(t);
    let destroyed = false;
    const sender = { isDestroyed: () => destroyed, send() { if (destroyed) throw new Error('Janela destruída'); } };
    const first = f.start({}, sender);
    destroyed = true;
    await first;
    await f.start();
    assert.equal(f.reads.length, 4);
    assert.equal(f.messages.at(-1).payload.success, true);
});

test('arquivo pulado aparece no resumo e não é anunciado como sucesso total', async t => {
    const f = fixture(t);
    fs.unlinkSync(f.files[0].path);
    await f.start();
    const result = f.messages.at(-1).payload;
    assert.equal(result.success, false);
    assert.equal(result.processados, 1);
    assert.equal(result.pulados, 1);
    assert.ok(f.messages.some(m => m.channel === 'log' && m.payload.includes('Arquivo não encontrado')));
});

test('recusa o mesmo arquivo duas vezes antes de alterar qualquer planilha', async t => {
    const f = fixture(t);
    await f.start({ cleanFiles: [f.files[0], { ...f.files[0], id: 'repetido' }] });
    assert.equal(f.reads.length, 0);
    assert.equal(f.messages.at(-1).payload.success, false);
    assert.ok(f.messages.some(m => m.channel === 'log' && m.payload.includes('repetido')));
});

test('outros usuários não limpam sem conexão mesmo enviando blocklist desmarcada', async t => {
    const f = fixture(t);
    f.state.currentUser.username = 'Outro';
    await f.start({ checkBlocklist: false, username: 'Davi' });
    assert.equal(f.messages.at(-1).payload.success, false);
    assert.equal(f.reads.length, 0);
});

for (const username of ['Outro', 'Davi']) {
    test(`blocklist consulta e remove contatos bloqueados para ${username}`, async t => {
        const f = fixture(t, async sql => {
            assert.match(sql, /FROM blocklist/);
            return { rows: [{ telefone: '21998364849' }] };
        });
        f.state.currentUser.username = username;
        f.state.pool = {};
        const workbook = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet([
            ['cnpj', 'fone1'], ['12345678000199', '21998364849'], ['22345678000199', '11987654321']
        ]), 'Base');
        XLSX.writeFile(workbook, f.files[0].path);
        await f.start({ cleanFiles: [f.files[0]], checkBlocklist: username === 'Davi' });
        assert.equal(f.queries(), 1);
        assert.equal(f.messages.at(-1).payload.success, true);
        const result = XLSX.readFile(f.files[0].path);
        assert.deepEqual(XLSX.utils.sheet_to_json(result.Sheets.Base, { header: 1 }), [
            ['cnpj', 'fone1'], ['22345678000199', 11987654321]
        ]);
    });
}
