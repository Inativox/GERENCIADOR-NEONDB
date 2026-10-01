'use strict';
const state = require('./state');
const { getApiCredentials } = require('./keyfile');
const { createApiLocks } = require('./apiLocks');
const locks = createApiLocks({ getPool: () => state.pool });

async function acquireFlowApiSession(username) {
    if (!state.currentUser || state.currentUser.username !== username || state.currentUser.role !== 'admin') {
        throw Object.assign(new Error('A sessão mudou. Retome com o usuário que iniciou o fluxo.'), { code: 'FLOW_VALIDATION' });
    }
    const credentials = getApiCredentials();
    if (!credentials?.c6?.clientId || !credentials.c6.clientSecret || !credentials?.im?.clientId || !credentials.im.clientSecret) {
        throw Object.assign(new Error('Importe uma licença de API válida com as duas chaves C6/IM na tela de login.'), { code: 'FLOW_VALIDATION' });
    }
    const lease = await locks.acquire(['c6', 'im'], username, 'dupla');
    return { ...lease, credentials: { c6: { clientId: credentials.c6.clientId, clientSecret: credentials.c6.clientSecret },
        im: { clientId: credentials.im.clientId, clientSecret: credentials.im.clientSecret } } };
}
module.exports = { locks, acquireFlowApiSession };
