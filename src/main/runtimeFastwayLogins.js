// Login Fastway salvo por usuário neste computador, compartilhado pelo Importar Mailing e pelo Alerta de Mailing.
const { safeStorage } = require('electron');
const Store = require('electron-store');
const { createFastwayLogins } = require('./fastwayLogins');

module.exports = createFastwayLogins({ store: new (require('./settingsWriteRetry')(Store))(), safeStorage });
