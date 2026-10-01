const { contextBridge, ipcRenderer } = require("electron");

function subscribe(channel, callback) {
    const listener = (_event, ...args) => callback(...args);
    ipcRenderer.on(channel, listener);
    return () => ipcRenderer.removeListener(channel, listener);
}

contextBridge.exposeInMainWorld("electronAPI", {
    // --- NOVO: Funções de Controle da Janela ---
    minimizeWindow: () => ipcRenderer.send('minimize-window'),
    maximizeWindow: () => ipcRenderer.send('maximize-window'),
    closeWindow: () => ipcRenderer.send('close-window'),

    // --- Funções de Login e Sessão ---
    loginAttempt: (username, password, rememberMe) => ipcRenderer.invoke('login-attempt', username, password, rememberMe),
    logout: () => ipcRenderer.send('logout'),
    onUserInfo: (callback) => ipcRenderer.on('user-info', (event, ...args) => callback(...args)),
    getAccessStatus: () => ipcRenderer.invoke('get-access-status'),
    importPrivateAccess: () => ipcRenderer.invoke('import-private-access'),

    // --- NOVO: Funções de Configuração do BD ---
    getDbConnectionString: () => ipcRenderer.invoke('get-db-connection-string'),
    saveAndTestDbConnection: (connectionString) => ipcRenderer.invoke('save-and-test-db-connection', connectionString),

    // --- Licença de API (arquivo-chave .mbkey) ---
    selectAndLoadKeyFile: () => ipcRenderer.invoke('load-key-file'),
    getKeyFileStatus: () => ipcRenderer.invoke('get-key-file-status'),

    // --- Funções de Configuração da UI ---
    getUiSettings: () => ipcRenderer.invoke('get-ui-settings'),
    saveUiSettings: (settings) => ipcRenderer.send('save-ui-settings', settings),
    // --- FIM DA MODIFICAÇÃO ---

    // --- Funções da Aba de Monitoramento ---
    fetchMonitoringReport: (payload) => ipcRenderer.invoke('fetch-monitoring-report', payload),
    fetchBitrixReport: (payload) => ipcRenderer.invoke('fetch-bitrix-report', payload), // ADICIONE ESTA LINHA
    downloadRecording: (url, fileName) => ipcRenderer.invoke('download-recording', url, fileName),

    // --- Funções da Limpeza Local ---
    flowsBootstrap: () => ipcRenderer.invoke('flows-bootstrap'),
    flowsSave: (flow) => ipcRenderer.invoke('flows-save', flow),
      flowsReceitaOptions: (input) => ipcRenderer.invoke('flows-receita-options', input),
      receitaSituacaoState: () => ipcRenderer.invoke('receita-situacao-state'),
      receitaSituacaoFile: () => ipcRenderer.invoke('receita-situacao-file'),
      receitaSituacaoOne: (cnpj) => ipcRenderer.invoke('receita-situacao-one', { cnpj }),
      receitaSituacaoStart: (fileId) => ipcRenderer.invoke('receita-situacao-start', { fileId }),
      receitaSituacaoCancel: () => ipcRenderer.invoke('receita-situacao-cancel'),
      receitaSituacaoOpen: () => ipcRenderer.invoke('receita-situacao-open'),
      onReceitaSituacaoUpdate: (callback) => {
          const listener = (_event, update) => callback(update);
          ipcRenderer.on('receita-situacao-update', listener);
          return () => ipcRenderer.removeListener('receita-situacao-update', listener);
      },
    flowsSaveLayout: (layout) => ipcRenderer.invoke('flows-save-layout', layout),
    flowsDeleteLayout: (id) => ipcRenderer.invoke('flows-delete-layout', id),
    flowsPreviewLayout: (input) => ipcRenderer.invoke('flows-preview-layout', input),
    flowsDelete: (id) => ipcRenderer.invoke('flows-delete', id),
    flowsSelectFolder: () => ipcRenderer.invoke('flows-select-folder'),
    flowsStart: (input) => ipcRenderer.invoke('flows-start', input),
    flowsCancel: (id) => ipcRenderer.invoke('flows-cancel', id),
    flowsResume: (id) => ipcRenderer.invoke('flows-resume', id),
    flowsOpenOutput: (input) => ipcRenderer.invoke('flows-open-output', input),
    flowsConfigureReceita: (input) => ipcRenderer.invoke('flows-configure-receita', input),
    flowsConfigureBq: () => ipcRenderer.invoke('flows-configure-bq'),
    flowsTestBq: () => ipcRenderer.invoke('flows-test-bq'),
    flowsRenewBq: () => ipcRenderer.invoke('flows-renew-bq'),
    flowsBqAutoLogin: (enabled) => ipcRenderer.invoke('flows-bq-auto-login', { enabled }),
    onFlowBqAuthUpdate: (callback) => subscribe('flow-bq-auth-update', callback),
    onFlowUpdate: (callback) => subscribe('flow-update', callback),
    selectFile: (options) => ipcRenderer.invoke("select-file", options),
    showSaveDialog: (options) => ipcRenderer.invoke("show-save-dialog", options), // NOVO
    openPath: (path) => ipcRenderer.send("open-path", path),
    startCleaning: (args) => ipcRenderer.send("start-cleaning", args),
    feedRootDatabase: (filePaths) => ipcRenderer.send("feed-root-database", filePaths),
    feedBlocklist: (filePaths) => ipcRenderer.send("feed-blocklist", filePaths), // NOVO
    getBlocklistStats: () => ipcRenderer.invoke("get-blocklist-stats"), // NOVO
    checkBlocklistNumbers: (numbers) => ipcRenderer.invoke("check-blocklist-numbers", numbers), // NOVO
    addNumbersToBlocklist: (numbers) => ipcRenderer.invoke("add-numbers-to-blocklist", numbers),
    refreshBlocklistCache: () => ipcRenderer.invoke("refresh-blocklist-cache"),
    splitLargeCsv: (args) => ipcRenderer.send("split-large-csv", args), // NOVO

    // --- Funções da API de Consulta (C6) ---
    addFilesToApiQueue: (files) => ipcRenderer.send("add-files-to-api-queue", files),
    pauseApiQueue: () => ipcRenderer.send("pause-api-queue"),
    resumeApiQueue: () => ipcRenderer.send("resume-api-queue"),
    startApiQueue: (args) => ipcRenderer.send("start-api-queue", args),
    resetApiQueue: () => ipcRenderer.send("reset-api-queue"),
    removeFromApiQueue: (filePath) => ipcRenderer.send("remove-from-api-queue", filePath),
    prioritizeInApiQueue: (filePath) => ipcRenderer.send("prioritize-in-api-queue", filePath),
    cancelCurrentApiTask: () => ipcRenderer.send("cancel-current-api-task"),
    updateApiTimingSettings: (settings) => ipcRenderer.send('set-api-delays', settings), // CORRIGIDO
    updateApiKeyMode: (keyMode) => ipcRenderer.send('set-api-key-mode', keyMode),
    showConfirmDialog: (options) => ipcRenderer.invoke('show-confirm-dialog', options), // JÁ EXISTENTE E CORRETO
    // Novas funções de agendamento
    scheduleFishCleanup: (options) => ipcRenderer.send('schedule-fish-cleanup', options),
    cancelFishSchedule: () => ipcRenderer.send('cancel-fish-schedule'),

    // --- Funções de Enriquecimento ---
    getEnrichedCnpjCount: () => ipcRenderer.invoke("get-enriched-cnpj-count"),
    downloadEnrichedData: () => ipcRenderer.invoke("download-enriched-data"),
    prepareEnrichmentFiles: (files) => ipcRenderer.send("prepare-enrichment-files", files),
    startDbLoad: (args) => ipcRenderer.send("start-db-load", args),
    startEnrichment: (args) => ipcRenderer.send("start-enrichment", args),

    // --- INÍCIO DA MODIFICAÇÃO: Funções de Relacionamento ---
    runRelacionamentoPipeline: (filePaths, modo) => ipcRenderer.send('run-relacionamento-pipeline', filePaths, modo),
    splitByResponsible: (filePath) => ipcRenderer.send('split-by-responsible', filePath), // NOVO
    // --- FIM DA MODIFICAÇÃO ---

    // --- Funções de Limpeza de Colunas ---
    startLimpezaColunas: (caminhos) => ipcRenderer.send('start-limpeza-colunas', caminhos),

    // --- Listeners de Eventos (Renderer "escuta" o Main) ---
    onLog: (callback) => ipcRenderer.on("log", (event, ...args) => callback(...args)),
    onProgress: (callback) => ipcRenderer.on("progress", (event, ...args) => callback(...args)),
    onCleaningFinished: callback => subscribe('cleaning-finished', callback),
    onApiQueueUpdate: (callback) => ipcRenderer.on("api-queue-update", (event, ...args) => callback(...args)),
    onApiLog: (callback) => ipcRenderer.on("api-log", (event, ...args) => callback(...args)),
    onApiProgress: (callback) => ipcRenderer.on("api-progress", (event, ...args) => callback(...args)),
    onApiLockError: (callback) => ipcRenderer.on("api-lock-error", (event, ...args) => callback(...args)), // NOVO: Erro de Lock
    onEnrichmentLog: (callback) => ipcRenderer.on("enrichment-log", (event, ...args) => callback(...args)),
    onEnrichmentProgress: (callback) => ipcRenderer.on("enrichment-progress", (event, ...args) => callback(...args)),
    onDbLoadProgress: (callback) => ipcRenderer.on("db-load-progress", (event, ...args) => callback(...args)),
    onDbLoadFinished: (callback) => ipcRenderer.on("db-load-finished", (event, ...args) => callback(...args)),
    onBlocklistLog: (callback) => ipcRenderer.on("blocklist-log", (event, ...args) => callback(...args)), // NOVO
    onEnrichmentFinished: (callback) => ipcRenderer.on("enrichment-finished", (event, ...args) => callback(...args)),
    onFishScheduleUpdate: (callback) => ipcRenderer.on('fish-schedule-update', (event, ...args) => callback(...args)),
    onUpdateDownloading: (callback) => ipcRenderer.on("update-downloading", (event, ...args) => callback(...args)),
    onUpdateProgress: (callback) => ipcRenderer.on("update-progress", (event, ...args) => callback(...args)),
    onUpdateReady: (callback) => ipcRenderer.on("update-ready", (event, ...args) => callback(...args)),
    onRootFeedFinished: (callback) => ipcRenderer.on('root-feed-finished', (event, ...args) => callback(...args)),

    // --- INÍCIO DA MODIFICAÇÃO: Listeners de Relacionamento ---
    onRelacionamentoLog: (callback) => ipcRenderer.on("relacionamento-log", (event, ...args) => callback(...args)),
    onRelacionamentoFinished: (callback) => ipcRenderer.on("relacionamento-finished", (event, ...args) => callback(...args)),
    onSplitByResponsibleLog: (callback) => ipcRenderer.on('split-by-responsible-log', (event, ...args) => callback(...args)), // NOVO
    onSplitByResponsibleFinished: (callback) => ipcRenderer.on('split-by-responsible-finished', (event, ...args) => callback(...args)), // NOVO
    // --- FIM DA MODIFICAÇÃO ---

    // --- Listeners de Limpeza de Colunas ---
    onLimpezaColunasLog: (callback) => subscribe('limpeza-colunas-log', callback),
    onLimpezaColunasProgress: (callback) => subscribe('limpeza-colunas-progress', callback),
    onLimpezaColunasFinished: (callback) => subscribe('limpeza-colunas-finished', callback),

    // Função para remover todos os listeners para evitar memory leaks ao recarregar
    removeAllListeners: (channel) => ipcRenderer.removeAllListeners(channel),
    
});
