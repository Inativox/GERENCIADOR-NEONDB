#!/usr/bin/env node
/**
 * Gerador de arquivo-chave (.mbkey) para o Gerenciador de Bases.
 *
 * Uso:
 *   C6_CLIENT_ID=xxx C6_CLIENT_SECRET=xxx IM_CLIENT_ID=xxx IM_CLIENT_SECRET=xxx node scripts/generate-key.js
 *
 * Ou defina as variáveis no .env antes de executar.
 * O arquivo gerado (chave-api.mbkey) deve ser distribuído para usuários autorizados.
 * Nunca compartilhe as credenciais em texto claro.
 */
require('dotenv').config();
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const CREDENTIALS = {
    c6: {
        clientId: process.env.C6_CLIENT_ID || '',
        clientSecret: process.env.C6_CLIENT_SECRET || '',
    },
    im: {
        clientId: process.env.IM_CLIENT_ID || '',
        clientSecret: process.env.IM_CLIENT_SECRET || '',
    },
};

const FILE_VERSION = 1;

// Deve ser idêntico ao src/main/keyfile.js
function getMasterKey() {
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

function generateKeyFile(credentials, outputPath) {
    const plaintext = Buffer.from(JSON.stringify(credentials), 'utf8');
    const key = getMasterKey();
    const iv = crypto.randomBytes(12);

    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
    const tag = cipher.getAuthTag();

    const envelope = {
        v: FILE_VERSION,
        iv: iv.toString('hex'),
        tag: tag.toString('hex'),
        payload: encrypted.toString('hex'),
    };

    fs.writeFileSync(outputPath, JSON.stringify(envelope), 'utf8');
    console.log(`\n✅ Arquivo-chave gerado: ${outputPath}`);
    console.log(`   Distribua este arquivo para usuários autorizados via canal seguro.`);
    console.log(`   NÃO inclua este arquivo no repositório git.\n`);
}

const missing = [];
if (!CREDENTIALS.c6.clientId) missing.push('C6_CLIENT_ID');
if (!CREDENTIALS.c6.clientSecret) missing.push('C6_CLIENT_SECRET');
if (!CREDENTIALS.im.clientId) missing.push('IM_CLIENT_ID');
if (!CREDENTIALS.im.clientSecret) missing.push('IM_CLIENT_SECRET');

if (missing.length > 0) {
    console.error(`\n❌ Variáveis de ambiente ausentes: ${missing.join(', ')}`);
    console.error(`   Certifique-se que o .env está configurado com todas as credenciais.\n`);
    process.exit(1);
}

const outputPath = path.join(__dirname, '..', 'chave-api.mbkey');
generateKeyFile(CREDENTIALS, outputPath);
