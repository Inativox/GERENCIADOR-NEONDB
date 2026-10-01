const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { credentialLocations, localSecrets } = require('../scripts/credential-guard.cjs');

test('guard rejects a complete Neon connection and reports only the line number', () => {
    const url = new URL('postgresql://ep-synthetic.neon.tech/fixture');
    url.username = 'fixture'; url.password = 'synthetic';
    assert.deepEqual(credentialLocations(`first line\n${url}`), [2]);
    assert.deepEqual(credentialLocations('postgresql://ep-synthetic.neon.tech/fixture'), []);
    assert.deepEqual(credentialLocations('const token = ' + 'npg_' + 'SyntheticOnly123'), [1]);
});

test('guard checks password values from database URLs even without SECRET in the variable name', t => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'credential-guard-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const url = new URL('postgresql://localhost/fixture');
    url.username = 'fixture'; url.password = 'synthetic@password';
    fs.writeFileSync(path.join(directory, '.env'), `DATABASE_URL=${url}\n`);
    const secrets = localSecrets(directory);
    assert.ok(secrets.includes('synthetic@password'));
    assert.deepEqual(credentialLocations('first\nsynthetic@password', secrets), [2]);
});
