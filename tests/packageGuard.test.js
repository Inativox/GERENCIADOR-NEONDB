const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const asar = require('@electron/asar');
const { checkPackage } = require('../scripts/check-package.cjs');

test('barreira de publicação recusa arquivos privados e aceita pacote sem configuração', async t => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'package-guard-'));
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const input = path.join(root, 'input');
    fs.mkdirSync(input);
    fs.writeFileSync(path.join(input, 'main.js'), 'console.log("app");');
    const clean = path.join(root, 'clean.asar');
    await asar.createPackage(input, clean);
    assert.doesNotThrow(() => checkPackage(clean));
    fs.writeFileSync(path.join(input, 'users.json'), '{}');
    fs.writeFileSync(path.join(input, '.env'), 'TEST=ficticio');
    fs.writeFileSync(path.join(input, 'access.mbconfig'), '{}');
    const unsafe = path.join(root, 'unsafe.asar');
    await asar.createPackage(input, unsafe);
    assert.throws(() => checkPackage(unsafe), /Pacote contém arquivos privados/);
});
