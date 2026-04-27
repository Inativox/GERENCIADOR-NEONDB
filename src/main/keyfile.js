const crypto = require('crypto');
const fs = require('fs');

const FILE_VERSION = 1;

function _getMasterKey() {
    const _a = Buffer.from([0x47,0x65,0x72,0x42,0x61,0x73,0x65,0x73]);
    const _b = Buffer.from([0x43,0x36,0x41,0x50,0x49,0x4b,0x65,0x79]);
    const _c = Buffer.from([0x32,0x30,0x32,0x34,0x4d,0x42,0x46,0x69]);
    const passphrase = Buffer.concat([_a, _b, _c]);
    const salt = Buffer.from([
        0x9f,0x2e,0x1a,0x83,0xc4,0xd7,0xb9,0x20,
        0x3f,0x8a,0x91,0xe2,0x74,0x5c,0x0b,0xd6
    ]);
    return crypto.scryptSync(passphrase, salt, 32, { N: 16384 });
}

let _cachedCredentials = null;

function loadKeyFile(filePath) {
    const raw = fs.readFileSync(filePath, 'utf8');
    const envelope = JSON.parse(raw);

    if (envelope.v !== FILE_VERSION) {
        throw new Error(`Versão de arquivo-chave inválida: ${envelope.v}`);
    }

    const key = _getMasterKey();
    const iv = Buffer.from(envelope.iv, 'hex');
    const tag = Buffer.from(envelope.tag, 'hex');
    const ciphertext = Buffer.from(envelope.payload, 'hex');

    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(tag);

    let decrypted;
    try {
        decrypted = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    } catch {
        throw new Error('Arquivo-chave inválido ou corrompido. Verifique o arquivo e tente novamente.');
    }

    _cachedCredentials = JSON.parse(decrypted.toString('utf8'));
}

function getApiCredentials() {
    return _cachedCredentials;
}

function clearCredentials() {
    _cachedCredentials = null;
}

function hasCredentials() {
    return _cachedCredentials !== null;
}

module.exports = { loadKeyFile, getApiCredentials, clearCredentials, hasCredentials };
