const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');

// Objetos criados dentro do vm têm outro protótipo; o JSON compara só o conteúdo.
const plain = valor => JSON.parse(JSON.stringify(valor));
const safeStorage = { isEncryptionAvailable: () => true, encryptString: texto => Buffer.from(`sealed:${Buffer.from(texto).toString('hex')}`), decryptString: buffer => Buffer.from(buffer.toString().slice(7), 'hex').toString() };

// Fastway sintética: um serviço com a lista em 99,5% (5 de 1.000 restantes).
function fastwayFalsa(chamadas) {
    return {
        async get(url, opcoes) {
            const { api_logon: logon, api_comando: comando } = opcoes.params;
            chamadas.push(comando);
            if (logon === 'errado') return { status: 401, data: '{"status":"00"}' };
            if (comando === 'ListarAtivos') return { status: 200, data: JSON.stringify({ Status: 'Sucesso', SERVICOS: [{ id: 87, nome: 'PAG', mailing_ativo_id: 100 }] }) };
            if (comando === 'ListarMailing') return { status: 200, data: JSON.stringify({ Status: 'Sucesso', Mailing: [{ id: 100, arquivo: 'pag.csv', registros: 1000, total_registros_disponiveis: 5, total_registros_aguardando_tentativa: 0, quantidade_reciclagem: 0, campanha_id: 87 }] }) };
            return { status: 200, data: '{"Status":"Sucesso"}' };
        },
    };
}

function fixture(t) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'alerta-ipc-')); t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const handlers = new Map(), listeners = new Map(), enviados = [], notificacoes = [], bandejas = [], chamadas = [], loginItems = [];
    const webContents = { send: (canal, dados) => enviados.push([canal, dados]) };
    const janela = { webContents, shown: 0, isDestroyed: () => false, isMinimized: () => false, restore() {}, show() { this.shown++; }, focus() {} };
    const state = { currentUser: { username: 'Ana', role: 'admin' }, mainWindow: janela };
    const stored = {};
    class Store { get(key, fallback) { return Object.hasOwn(stored, key) ? structuredClone(stored[key]) : fallback; } set(key, value) { stored[key] = structuredClone(value); } }
    class Notification {
        static isSupported() { return true; }
        constructor(opcoes) { this.opcoes = opcoes; this.eventos = {}; notificacoes.push(this); }
        on(nome, funcao) { this.eventos[nome] = funcao; }
        show() {} close() { this.fechada = true; }
    }
    class Tray {
        constructor() { this.destruida = false; bandejas.push(this); }
        setImage() {} setToolTip(texto) { this.dica = texto; } setContextMenu(menu) { this.menu = menu; } on() {}
        isDestroyed() { return this.destruida; } destroy() { this.destruida = true; }
    }
    const app = { isPackaged: true, getPath: () => dir, setAppUserModelId() {}, setLoginItemSettings: opcoes => loginItems.push(opcoes), on: (nome, funcao) => listeners.set(`app:${nome}`, funcao), quit() {} };
    const electron = { app, ipcMain: { handle: (nome, funcao) => handlers.set(nome, funcao), on: (nome, funcao) => listeners.set(nome, funcao) }, Notification, Tray,
        Menu: { buildFromTemplate: modelo => modelo }, nativeImage: { createEmpty: () => ({ addRepresentation() {} }) } };
    const filename = path.resolve('src/main/handlers/alertaMailing.js'), actual = createRequire(filename), module = { exports: {} };
    const fmi = actual('../alertaMailing/fastwayFmi');
    const logins = actual('../fastwayLogins').createFastwayLogins({ store: new Store(), safeStorage });
    const mocks = { electron, 'electron-store': Store, '../settingsWriteRetry': Base => Base, '../state': state, '../runtimeFastwayLogins': logins,
        '../alertaMailing/fastwayFmi': { ...fmi, createFastwayFmi: opcoes => fmi.createFastwayFmi({ ...opcoes, http: fastwayFalsa(chamadas), intervaloMinMs: 0, sleep: async () => {} }) } };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { require: nome => mocks[nome] || actual(nome), module, __dirname: path.dirname(filename), console: { log() {}, warn() {}, error() {} },
        process: { platform: 'win32', env: {}, execPath: 'electron.exe' }, setTimeout, clearTimeout, Buffer }, { filename });
    module.exports.register();
    t.after(() => module.exports.parar());
    const invocar = (nome, input, sender = webContents) => handlers.get(nome)({ sender }, input);
    const ate = async condicao => { for (let i = 0; i < 200 && !condicao(); i++) await new Promise(setImmediate); assert.ok(condicao(), 'estado esperado não chegou'); };
    return { alerta: module.exports, handlers, listeners, enviados, notificacoes, bandejas, chamadas, loginItems, state, stored, logins, janela, invocar, ate };
}

test('o painel só responde ao admin logado na janela principal', async t => {
    const f = fixture(t);
    for (const [nome] of f.handlers) assert.equal((await f.invocar(nome, {}, {})).success, false, nome);
    f.state.currentUser = { username: 'Bia', role: 'limited' };
    for (const [nome] of f.handlers) assert.equal((await f.invocar(nome, {})).success, false, nome);
});

test('sem login salvo o alerta não monitora e a janela fecha normalmente', async t => {
    const f = fixture(t);
    const { painel } = await f.invocar('alerta-mailing-state');
    assert.equal(painel.configurado, false);
    assert.deepEqual(painel.login, { saved: false, login: null, canSave: true });
    assert.equal(f.alerta.manterNaBandeja(), false);
    assert.equal(f.bandejas.length, 0);
    assert.match((await f.invocar('alerta-mailing-verificar')).message, /login Fastway/);
});

test('login recusado pela Fastway não é salvo; aceito liga o monitor, a bandeja e o início com o Windows', async t => {
    const f = fixture(t);
    const recusado = await f.invocar('alerta-mailing-login', { login: 'errado', password: 'x' });
    assert.equal(recusado.success, false);
    assert.match(recusado.message, /recusou/);
    assert.equal(f.logins.get('Ana'), null);
    assert.match((await f.invocar('alerta-mailing-login', { login: ' ', password: 'x' })).message, /login e a senha/);

    const aceito = await f.invocar('alerta-mailing-login', { login: 'ana.fastway', password: 'segredo' });
    assert.equal(aceito.success, true);
    assert.equal(aceito.painel.configurado, true);
    assert.deepEqual(f.logins.get('Ana'), { login: 'ana.fastway', password: 'segredo' });
    assert.equal(f.bandejas.length, 1);
    assert.ok(f.alerta.manterNaBandeja());
    assert.deepEqual(plain(f.loginItems), [{ openAtLogin: true, args: ['--bandeja'] }]);
    assert.ok(!JSON.stringify(aceito).includes('segredo'), 'a senha nunca volta para a tela');
});

test('o ciclo chega ao painel, à bandeja e às notificações; o clique abre o serviço', async t => {
    const f = fixture(t);
    f.logins.save('Ana', { login: 'ana.fastway', password: 'segredo' });
    f.alerta.sincronizar();
    await f.ate(() => f.enviados.some(([canal, dados]) => canal === 'alerta-mailing-update' && dados.linhas.length));
    const painel = f.enviados.filter(([canal]) => canal === 'alerta-mailing-update').at(-1)[1];
    assert.deepEqual(painel.linhas.map(l => [l.servicoId, l.situacao]), [[87, 'QUASE']]);
    assert.equal(f.bandejas[0].dica, 'Alerta de Mailing: 1 serviço(s) em alerta');
    assert.equal(f.notificacoes[0].opcoes.title, 'PAG: mailing em 99%');
    f.notificacoes[0].eventos.click();
    assert.equal(f.janela.shown, 1);
    assert.deepEqual(plain(f.enviados.at(-1)), ['alerta-mailing-foco', { servicoId: 87 }]);
});

test('a chave automática fica salva neste PC e o logout desliga tudo', async t => {
    const f = fixture(t);
    f.logins.save('Ana', { login: 'ana.fastway', password: 'segredo' });
    f.alerta.sincronizar();
    const ligado = await f.invocar('alerta-mailing-reciclador', { ligar: true });
    assert.equal(ligado.painel.reciclador, true);
    assert.equal(f.stored.alerta_mailing_reciclador, true);
    f.state.currentUser = null;
    f.listeners.get('logout')();
    assert.equal(f.bandejas[0].destruida, true);
    assert.equal(f.alerta.manterNaBandeja(), false);
});

test('ao sair pelo app o alerta para e a janela não fica presa na bandeja', async t => {
    const f = fixture(t);
    f.logins.save('Ana', { login: 'ana.fastway', password: 'segredo' });
    f.alerta.sincronizar();
    assert.ok(f.alerta.manterNaBandeja());
    f.listeners.get('app:before-quit')();
    assert.equal(f.alerta.manterNaBandeja(), false);
    assert.equal(f.bandejas[0].destruida, true);
});
