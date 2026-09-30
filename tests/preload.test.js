const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const loadModule = require('./helpers/loadModule');

test('assinatura de eventos de colunas pode ser removida sem afetar outro consumidor', () => {
    const ipcRenderer = new EventEmitter();
    let api;
    loadModule('preload.js', { electron: { ipcRenderer, contextBridge: { exposeInMainWorld: (_, value) => { api = value; } } } });
    let first = 0;
    let second = 0;
    const unsubscribe = api.onLimpezaColunasLog(() => { first++; });
    api.onLimpezaColunasLog(() => { second++; });
    ipcRenderer.emit('limpeza-colunas-log', {}, 'mensagem');
    assert.equal(first, 1);
    unsubscribe();
    ipcRenderer.emit('limpeza-colunas-log', {}, 'outra');
    assert.equal(first, 1);
    assert.equal(second, 2);
    assert.equal(ipcRenderer.listenerCount('limpeza-colunas-log'), 1);
});
