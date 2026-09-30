const test = require('node:test');
const assert = require('node:assert/strict');
const loadModule = require('./helpers/loadModule');

test('novo renderer recebe rejeição explícita quando o lote pertence à janela anterior', async () => {
    let listener;
    let busy = false;
    let finish;
    const handler = loadModule('src/main/handlers/limpezaColunas.js', {
        electron: { ipcMain: { on: (_channel, callback) => { listener = callback; } } },
        '../state': { currentUser: { role: 'admin', username: 'Teste' } },
        '../database/connection': { logSystemAction() {} },
        '../limpezaColunasJob': {
            isLimpezaColunasRunning: () => busy,
            runLimpezaColunas: () => { busy = true; return new Promise(resolve => { finish = resolve; }); },
        },
    });
    handler.register();
    function sender() {
        return { messages: [], destroyed: false, isDestroyed() { return this.destroyed; }, send(channel, value) { this.messages.push({ channel, value }); } };
    }
    const previous = sender();
    const next = sender();
    const pending = listener({ sender: previous }, ['original.xlsx']);
    await listener({ sender: previous }, ['duplicate.xlsx']);
    assert.equal(previous.messages.filter(m => m.channel === 'limpeza-colunas-finished').length, 0);
    previous.destroyed = true;
    await listener({ sender: next }, ['new.xlsx']);
    const rejection = next.messages.find(m => m.channel === 'limpeza-colunas-finished');
    assert.ok(rejection, 'A nova janela precisa liberar os controles de execução.');
    assert.equal(rejection.value.success, false);
    assert.match(rejection.value.message, /andamento/i);
    finish({ processados: 1, pulados: 0 });
    await pending;
});
