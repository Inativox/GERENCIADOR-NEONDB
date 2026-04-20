# Gerenciador de Bases — Instruções para o Claude

## O que é este projeto
App desktop Electron para gerenciamento de bases de dados comerciais da MB Finance.
Stack: Electron + Node.js + PostgreSQL (NeonDB).

## Arquitetura
- `main.js` — processo principal do Electron. Toda lógica de banco, arquivos e IPC fica aqui.
- `renderer.js` — lógica do frontend (roda no browser context do Electron).
- `preload.js` — bridge segura entre main e renderer via `contextBridge`. Nunca exponha Node diretamente no renderer.
- `index.html` — interface principal com sistema de abas.
- `login.html` — tela de login com validação de usuários via `users.json`.

## Regras importantes
- Variáveis de ambiente sempre via `process.env` (carregadas com `dotenv`). Nunca hardcode credenciais.
- Comunicação entre processos sempre via `ipcMain`/`ipcRenderer` com handlers nomeados.
- Manipulação de planilhas: usar `ExcelJS` para leitura/escrita, `xlsx` apenas quando necessário.
- Erros de banco devem ser capturados e enviados de volta ao renderer com mensagem amigável.

## Design system
- Dark theme como padrão. Variáveis CSS definidas em `:root` no `index.html`.
- Fonte display: Orbitron. Interface: Inter. Código: JetBrains Mono.
- Cor de marca: `#6366f1` (indigo). Nunca usar cores hardcoded no JS — usar variáveis CSS.

## O que NÃO fazer
- Não commitar `.env`, `users.json`, `*.py`, `node_modules/` ou a pasta `dist/`.
- Não instalar dependências desnecessárias — o bundle já está pesado.
- Não criar novos arquivos HTML para cada funcionalidade — usar o sistema de abas existente.
