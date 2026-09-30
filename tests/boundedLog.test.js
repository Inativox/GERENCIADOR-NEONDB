const test = require('node:test');
const assert = require('node:assert/strict');

class Element {
    constructor(document) { this.ownerDocument = document; this.children = []; this._text = ''; this.scrollTop = 0; }
    get textContent() { return this._text + this.children.map(c => c.textContent).join(''); }
    set textContent(text) { this._text = text; this.children = []; }
    get childElementCount() { return this.children.length; }
    get firstElementChild() { return this.children[0]; }
    get scrollHeight() { return this.children.length; }
    appendChild(child) { if (child.fragment) this.children.push(...child.children); else this.children.push(child); }
    removeChild(child) { this.children.splice(this.children.indexOf(child), 1); }
}

function fixture() {
    const document = { createElement: () => new Element(document), createDocumentFragment: () => Object.assign(new Element(document), { fragment: true }) };
    const element = new Element(document);
    const scheduled = [];
    return { element, scheduled, schedule: fn => scheduled.push(fn), flush: () => { while (scheduled.length) scheduled.shift()(); } };
}

test('burst de mensagens mantém limite e preserva as últimas linhas', async () => {
    const { createBoundedLog } = await import('../src/renderer/boundedLog.mjs');
    const f = fixture();
    const append = createBoundedLog(f.element, { maxLines: 20, schedule: f.schedule });
    for (let i = 0; i < 10000; i++) append(`linha ${i}`);
    assert.equal(f.scheduled.length, 1);
    f.flush();
    assert.equal(f.element.childElementCount, 20);
    assert.equal(f.element.firstElementChild.textContent, '> linha 9980');
    assert.equal(f.element.children.at(-1).textContent, '> linha 9999');
    append('final');
    f.flush();
    assert.equal(f.element.childElementCount, 20);
    assert.equal(f.element.children.at(-1).textContent, '> final');
});

test('logs HTML e multiline entram como texto e placeholder é removido', async () => {
    const { createBoundedLog } = await import('../src/renderer/boundedLog.mjs');
    const f = fixture();
    f.element.textContent = 'Aguardando...';
    const append = createBoundedLog(f.element, { placeholder: 'Aguardando...', schedule: f.schedule });
    append('<img src=x onerror=alert(1)>\nsegunda linha');
    f.flush();
    assert.equal(f.element.childElementCount, 2);
    assert.equal(f.element.firstElementChild.textContent, '> <img src=x onerror=alert(1)>');
    assert.equal(f.element.children.at(-1).textContent, '> segunda linha');
});

test('limite continua funcionando depois de limpar painel externamente', async () => {
    const { createBoundedLog } = await import('../src/renderer/boundedLog.mjs');
    const f = fixture();
    const append = createBoundedLog(f.element, { maxLines: 2, schedule: f.schedule });
    append('antigo');
    f.flush();
    f.element.textContent = '';
    append('novo\nmais novo\núltimo');
    f.flush();
    assert.equal(f.element.childElementCount, 2);
    assert.equal(f.element.firstElementChild.textContent, '> mais novo');
    assert.equal(f.element.children.at(-1).textContent, '> último');
});
