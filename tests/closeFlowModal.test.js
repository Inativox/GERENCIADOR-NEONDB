const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');

const IDS = ['close-flow-modal', 'close-flow-title', 'close-flow-detail', 'close-flow-keep', 'close-flow-confirm'];

function fakeDocument() {
    const document = { activeElement: null, elements: {} };
    for (const id of IDS) {
        const events = new EventEmitter(), classes = new Set();
        const node = {
            id, textContent: '', inert: false,
            classList: { add: name => classes.add(name), remove: name => classes.delete(name), contains: name => classes.has(name) },
            addEventListener: (type, listener) => events.on(type, listener),
            dispatch: (type, extra = {}) => { const event = { target: node, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, ...extra }; events.emit(type, event); return event; },
            focus: () => { document.activeElement = node; },
        };
        document.elements[id] = node;
    }
    document.getElementById = id => document.elements[id];
    const modal = document.elements['close-flow-modal'];
    modal.classList.add('hidden'); modal.inert = true;
    return document;
}
async function setup() {
    const { setupCloseFlowModal } = await import('../src/renderer/closeFlowModal.mjs');
    const document = fakeDocument(), answers = [];
    let request;
    setupCloseFlowModal({ onConfirmCloseDuringFlow: callback => { request = callback; }, answerCloseDuringFlow: close => answers.push(close) }, document);
    return { document, answers, request: data => request(data), $: id => document.getElementById(id) };
}

test('modal de fechamento avisa sobre a Fastway, foca "Manter aberto" e devolve o foco ao responder', async () => {
    const f = await setup();
    const previous = { focus() { f.document.activeElement = previous; } };
    previous.focus();
    f.request({ uploading: true });
    assert.equal(f.$('close-flow-modal').classList.contains('hidden'), false);
    assert.equal(f.$('close-flow-modal').inert, false);
    assert.match(f.$('close-flow-title').textContent, /Fastway/);
    assert.equal(f.document.activeElement, f.$('close-flow-keep'));
    const escape = f.$('close-flow-modal').dispatch('keydown', { key: 'Escape' });
    assert.equal(escape.defaultPrevented, true);
    assert.deepEqual(f.answers, [false]);
    assert.equal(f.$('close-flow-modal').classList.contains('hidden'), true);
    assert.equal(f.$('close-flow-modal').inert, true);
    assert.equal(f.document.activeElement, previous);
    f.request({ uploading: false });
    assert.doesNotMatch(f.$('close-flow-title').textContent, /Fastway/);
    f.$('close-flow-confirm').dispatch('click');
    assert.deepEqual(f.answers, [false, true]);
});

test('modal de fechamento prende o Tab nos dois botões, clicar fora mantém aberto e não responde duas vezes', async () => {
    const f = await setup();
    f.request({ uploading: true });
    f.$('close-flow-modal').dispatch('keydown', { key: 'Tab' });
    assert.equal(f.document.activeElement, f.$('close-flow-confirm'));
    f.$('close-flow-modal').dispatch('keydown', { key: 'Tab', shiftKey: true });
    assert.equal(f.document.activeElement, f.$('close-flow-keep'));
    f.$('close-flow-modal').dispatch('click');
    assert.deepEqual(f.answers, [false]);
    f.$('close-flow-confirm').dispatch('click');
    assert.deepEqual(f.answers, [false]);
});
