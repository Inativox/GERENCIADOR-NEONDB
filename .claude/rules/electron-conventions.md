# Convenções Electron deste projeto

- Todo acesso ao banco de dados fica em `main.js`, nunca no renderer.
- O renderer só pode chamar funções expostas pelo `preload.js` via `window.api.*`.
- Para adicionar uma nova funcionalidade de banco: 1) adiciona handler em `ipcMain` no main.js, 2) expõe via `contextBridge` no preload.js, 3) chama via `window.api` no renderer.js.
- Janelas e diálogos nativos (abrir arquivo, salvar) sempre no processo main.
