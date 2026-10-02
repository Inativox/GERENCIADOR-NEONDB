/**
 * Estado compartilhado do processo principal.
 * Todos os handlers importam este módulo e mutam suas propriedades diretamente.
 */
const state = {
    pool: null,
    mainWindow: null,
    loginWindow: null,
    currentUser: null,
    bqRootService: null,
};

module.exports = state;
