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
    receitaCnaeService: null, // CNAE principal por CNPJ, criado pelo handler da Situação Receita
    mailingUpload: null, // envio manual da aba Importar Mailing; o fechamento do app pede confirmação enquanto existir
};

module.exports = state;
