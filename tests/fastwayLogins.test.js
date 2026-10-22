const test = require('node:test');
const assert = require('node:assert/strict');

const { createFastwayLogins } = require('../src/main/fastwayLogins');

function fixture({ available = true } = {}) {
    const data = {};
    const store = { get: (key, fallback) => Object.hasOwn(data, key) ? structuredClone(data[key]) : fallback, set: (key, value) => { data[key] = structuredClone(value); } };
    // Cifra sintética: reversível só por este fake, como o DPAPI só abre na mesma conta do Windows.
    const safeStorage = {
        isEncryptionAvailable: () => available,
        encryptString: text => Buffer.from(`sealed:${Buffer.from(text).toString('hex')}`),
        decryptString: buffer => {
            const text = buffer.toString();
            if (!text.startsWith('sealed:')) throw new Error('Não foi possível abrir.');
            return Buffer.from(text.slice(7), 'hex').toString();
        }
    };
    return { data, logins: createFastwayLogins({ store, safeStorage }) };
}

const ANA = { login: 'ana.fastway', password: 'senha-sintetica' };

test('salva cifrado e devolve o login de cada usuário', () => {
    const { data, logins } = fixture();
    logins.save('Ana', ANA);

    assert.deepEqual(logins.get('Ana'), ANA);
    assert.equal(logins.get('Bia'), null);
    assert.equal(JSON.stringify(data).includes(ANA.password), false, 'senha não pode ficar em texto puro');
    assert.equal(JSON.stringify(data).includes(ANA.login), false);
});

test('resumo para a tela mostra o login salvo, nunca a senha', () => {
    const { logins } = fixture();
    assert.deepEqual(logins.summary('Ana'), { saved: false, login: null, canSave: true });

    logins.save('Ana', ANA);

    assert.deepEqual(logins.summary('Ana'), { saved: true, login: 'ana.fastway', canSave: true });
});

test('esquecer apaga só o login daquele usuário', () => {
    const { logins } = fixture();
    logins.save('Ana', ANA);
    logins.save('Bia', { login: 'bia.fastway', password: 'outra' });

    logins.forget('Ana');

    assert.equal(logins.get('Ana'), null);
    assert.deepEqual(logins.get('Bia'), { login: 'bia.fastway', password: 'outra' });
});

test('sem a proteção do Windows não salva e não lê', () => {
    const { logins } = fixture({ available: false });

    assert.throws(() => logins.save('Ana', ANA), /proteção do Windows/);
    assert.deepEqual(logins.summary('Ana'), { saved: false, login: null, canSave: false });
});

test('login que não abre mais (outra conta do Windows) conta como não salvo', () => {
    const { data, logins } = fixture();
    data.fastway_logins = { Ana: Buffer.from('cifrado-em-outra-maquina').toString('base64') };

    assert.equal(logins.get('Ana'), null);
    assert.deepEqual(logins.summary('Ana'), { saved: false, login: null, canSave: true });
});
