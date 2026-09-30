// Export only when requested; the output must be delivered privately to its user.
const fs = require('fs');
const path = require('path');
const { validateBundle } = require('../src/main/privateConfig');
const username = process.argv[2];
if (!username) throw new Error('Uso: npm run config:export -- NomeDoUsuario [caminho.mbconfig]');
const users = JSON.parse(fs.readFileSync(path.resolve('users.json'), 'utf8'));
if (!Object.hasOwn(users, username)) throw new Error('Usuário não encontrado.');
const bundle = { version: 1, users: { [username]: users[username] }, env: {} };
if (fs.existsSync('.env')) bundle.env = require('dotenv').parse(fs.readFileSync('.env'));
if (fs.existsSync('chave-api.mbkey')) bundle.keyFile = fs.readFileSync('chave-api.mbkey', 'utf8');
validateBundle(bundle);
const safeName = username.replace(/[^a-z0-9_-]/gi, '_');
const output = path.resolve(process.argv[3] || path.join('private-exports', `${safeName}.mbconfig`));
fs.mkdirSync(path.dirname(output), { recursive: true });
fs.writeFileSync(output, JSON.stringify(bundle), { flag: 'wx', mode: 0o600 });
console.log(`Acesso exportado para ${output}. Entregue por canal privado; não publique esse arquivo.`);
