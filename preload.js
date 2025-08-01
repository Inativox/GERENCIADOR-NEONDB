const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("electronAPI", {
    // --- Funções de Login e Sessão ---
    loginAttempt: (username, password, rememberMe) => ipcRenderer.invoke('login-attempt', username, password, rememberMe), // MODIFICADO: Adicionado 'rememberMe'
    logout: () => ipcRenderer.send('logout'), // NOVO: Função para o botão de sair
    onUserInfo: (callback) => ipcRenderer.on('user-info', (event, ...args) => callback(...args)),

    // --- Funções da Aba de Monitoramento ---
    fetchMonitoringReport: (payload) => ipcRenderer.invoke('fetch-monitoring-report', payload), // NOVO: Função que faltava para a 2ª API
    downloadRecording: (url, fileName) => ipcRenderer.invoke('download-recording', url, fileName), // NOVO: Função para baixar gravações

    // --- Funções da Limpeza Local ---
    selectFile: (options) => ipcRenderer.invoke("select-file", options),
    openPath: (path) => ipcRenderer.send("open-path", path),
    updateBlocklist: (backup) => ipcRenderer.invoke("update-blocklist", backup),
    startCleaning: (args) => ipcRenderer.send("start-cleaning", args),
    startAdjustPhones: (args) => ipcRenderer.send("start-adjust-phones", args),
    startMerge: (files) => ipcRenderer.send("start-merge", files),
    startDbOnlyCleaning: (args) => ipcRenderer.send("start-db-only-cleaning", args),
    feedRootDatabase: (filePaths) => ipcRenderer.send("feed-root-database", filePaths),
    saveStoredCnpjsToExcel: () => ipcRenderer.invoke("save-stored-cnpjs-to-excel"),
    deleteBatch: (batchId) => ipcRenderer.invoke("delete-batch", batchId),

    // --- Funções da API de Consulta (C6) ---
    addFilesToApiQueue: (files) => ipcRenderer.send("add-files-to-api-queue", files),
    startApiQueue: (args) => ipcRenderer.send("start-api-queue", args),
    resetApiQueue: () => ipcRenderer.send("reset-api-queue"),

    // --- Funções de Enriquecimento ---
    getEnrichedCnpjCount: () => ipcRenderer.invoke("get-enriched-cnpj-count"),
    downloadEnrichedData: () => ipcRenderer.invoke("download-enriched-data"),
    prepareEnrichmentFiles: (files) => ipcRenderer.send("prepare-enrichment-files", files),
    startDbLoad: (args) => ipcRenderer.send("start-db-load", args),
    startEnrichment: (args) => ipcRenderer.send("start-enrichment", args),

    // --- Listeners de Eventos (Renderer "escuta" o Main) ---
    onLog: (callback) => ipcRenderer.on("log", (event, ...args) => callback(...args)),
    onProgress: (callback) => ipcRenderer.on("progress", (event, ...args) => callback(...args)),
    onUploadProgress: (callback) => ipcRenderer.on("upload-progress", (event, ...args) => callback(...args)),
    onApiQueueUpdate: (callback) => ipcRenderer.on("api-queue-update", (event, ...args) => callback(...args)),
    onApiLog: (callback) => ipcRenderer.on("api-log", (event, ...args) => callback(...args)),
    onApiProgress: (callback) => ipcRenderer.on("api-progress", (event, ...args) => callback(...args)),
    onEnrichmentLog: (callback) => ipcRenderer.on("enrichment-log", (event, ...args) => callback(...args)),
    onEnrichmentProgress: (callback) => ipcRenderer.on("enrichment-progress", (event, ...args) => callback(...args)),
    onDbLoadProgress: (callback) => ipcRenderer.on("db-load-progress", (event, ...args) => callback(...args)),
    onDbLoadFinished: (callback) => ipcRenderer.on("db-load-finished", (event, ...args) => callback(...args)),
    onEnrichmentFinished: (callback) => ipcRenderer.on("enrichment-finished", (event, ...args) => callback(...args)),
    onUpdateMessage: (callback) => ipcRenderer.on("update-message", (event, ...args) => callback(...args)),
    onRootFeedFinished: (callback) => ipcRenderer.on('root-feed-finished', (event, ...args) => callback(...args)),

    // Função para remover todos os listeners para evitar memory leaks ao recarregar
    removeAllListeners: (channel) => ipcRenderer.removeAllListeners(channel)
});