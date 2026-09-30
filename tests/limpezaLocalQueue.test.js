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
    const messages = [];
    const reads = [];
    let pendingReads = 0;
    let peakReads = 0;
    let failRead = false;
    let queries = 0;
    const state = { currentUser: { username: 'Davi', role: 'admin' }, pool: null };
    const sender = { isDestroyed: () => false, send: (channel, payload) => messages.push({ channel, payload }) };
    const cleaning = loadModule('src/main/handlers/limpeza.js', {
        electron: { ipcMain: { on: (channel, callback) => listeners.set(channel, callback), handle() {} }, dialog: {}, shell: {} },
        path, fs, xlsx: XLSX, exceljs: require('exceljs'), axios: {},
        '../state': state,
        '../limpezaTelefones': require('../src/main/limpezaTelefones'),
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
    }, { setImmediate });
    cleaning.register();
    return {
        files, messages, reads, state,
        start: (args = {}, selectedSender = sender) => listeners.get('start-cleaning')({ sender: selectedSender }, { cleanFiles: files, ...args }),
        peak: () => peakReads,
        queries: () => queries,
        fail: value => { failRead = value; }
    };
}

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
