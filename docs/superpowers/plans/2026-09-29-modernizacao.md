# Modernização — primeira entrega

> Execução nativa nesta sessão com `superpowers:executing-plans`, conforme autorização do usuário. Não publicar nem alterar o banco de produção.

**Goal:** estabilizar logs e conexão e entregar a primeira aba React com trabalho de planilhas fora do processo principal.

**Architecture:** React ocupa apenas o conteúdo de Limpeza de Colunas dentro da navegação existente; Vite gera recursos locais. O preload fornece eventos com unsubscribe. Worker limitado a um lote separa CPU do Electron. O pool reaproveita conexões durante retry.

**Tech Stack:** Electron, Node.js, pg, ExcelJS, React, TypeScript e Vite.

**Spec:** `docs/superpowers/specs/2026-09-29-modernizacao-design.md`

## Restrições globais

- Tema escuro, indigo e fontes do projeto; cores via CSS.
- IPC nomeado, contextIsolation habilitado e Node desabilitado no renderer.
- Nenhum HTML adicional por funcionalidade; usar abas existentes.
- Dependências somente para compilação e React; testes usam Node e Electron existentes.
- Manter formato e regras de Limpeza de Colunas e nunca modificar arquivos de entrada.

## Review Focus

- Burst de logs e conteúdo parecido com HTML: preservar texto recente sem crescimento ilimitado.
- Falha de inicialização de BD: liberar o pool criado e conservar uma conexão anterior válida.
- Retry concorrente: nunca encerrar o pool compartilhado.
- Duplo início e janela fechada durante limpeza: uma execução, sem envio para webContents destruído.
- Arquivos com zeros iniciais e números científicos: saída equivalente aos testes existentes.

## Task 1 — pool, janelas e atualização

Arquivos: `src/main/database/connection.js`, `src/main/handlers/auth.js`, `src/main/index.js`, `tests/connection.test.js`, `tests/lifecycle.test.js`.

Interfaces: manter `initializePool(connectionString, windowToLog)` e `queryWithRetry(sql, params, maxRetries, logFn)`.

- [x] Reproduzir evento de erro ocioso com EventEmitter, falha de inicialização, retry simultâneo e tentativas limitadas em módulo real carregado via VM; transporte fake sem acesso ao Neon.
- [x] Executar `node --test tests/connection.test.js` e confirmar falhas antes da correção.
- [x] Criar pool candidato local; tratar `error`; publicar apenas após inicialização; liberar candidato na falha; preservar pool anterior. Retry faz nova query no mesmo pool, sem ler configuração ou reconectar globalmente.
- [x] Mover listeners de janela para register e proteger janela ausente/destruída. Retirar timer quitAndInstall, mantendo autoInstallOnAppQuit.
- [x] Executar regressões de conexão e ciclo de vida.

## Task 2 — logs limitados

Arquivos: `src/renderer/boundedLog.js`, `src/renderer/index.js`, `tests/boundedLog.test.js`.

Interface: `createBoundedLog(element, { maxLines, schedule, placeholder, prefix })` devolve callback de append. Usa texto, fila limitada, document fragment e um flush por frame.

- [x] Escrever regressões para milhares de mensagens, limite de pendências, mensagens multiline, reset externo e texto HTML.
- [x] Executar testes e confirmar falha antes de criar o módulo.
- [x] Implementar helper e integrar todos os sete painéis legados; remover concatenação innerHTML de logs.
- [x] Executar teste e validar no renderer real em smoke posterior.

## Task 3 — worker de colunas

Arquivos: `src/main/workers/limpezaColunasWorker.js`, `src/main/limpezaColunasJob.js`, `src/main/handlers/limpezaColunas.js`, `tests/limpezaColunasJob.test.js`.

Interface: `runLimpezaColunas(caminhos, onEvent)` processa lote em worker e retorna `{ processados, pulados, primeiraSaida }`. Mensagens discriminadas transportam apenas progresso e resumo, nunca workbook.

- [x] Criar fixture XLSX e testar saída, manutenção do original, erro de arquivo e progresso; testar limite de concorrência e loop principal livre.
- [x] Executar teste falhando antes de implementar.
- [x] Implementar worker com saída e erro tratados; processar arquivos sequencialmente. Handler valida entrada e identidade admin, trata duplo início e sender destruído.
- [x] Executar testes existentes da limpeza e regressões do worker.

## Task 4 — aba React e distribuição

Arquivos: `src/renderer/react/main.tsx`, `src/renderer/react/LimpezaColunas.tsx`, `src/renderer/react/limpezaColunas.css`, `src/renderer/react/types.ts`, `preload.js`, `index.html`, `package.json`, `package-lock.json`, `tsconfig.json`, `vite.config.mjs`, `.gitignore`, `scripts/smoke-renderer.cjs`, `README.md`.

Interface: bridge de colunas fornece unsubscribe para logs, progresso e conclusão, mantendo APIs legadas. HTML mantém containers das abas e inclui mount React dentro de limpezaColunas.

- [x] Adicionar teste de unsubscribe do preload antes da alteração e confirmar falha.
- [x] Instalar React/react-dom e ferramentas TS/Vite compatíveis com Node local; definir engines e lockfile.
- [x] Configurar Vite em modo library para bundle IIFE local, sem servidor em produção. `prestart`, `predist` e `prepublish` compilam recursos para `out/renderer`.
- [x] Remover markup e eventos legados de colunas, montar componente React com seleção, status, progresso, resumo e atividade limitada. Adicionar CSS limitado à aba e tipos da bridge.
- [x] Typecheck, build e smoke Electron escondido com bridge real e IPC sintético: selecionar arquivos, iniciar, receber logs/progresso/conclusão e renderizar HTML como texto.
- [x] Documentar scripts e limites da migração. Rodar `npm test`, `npm run typecheck`, `npm run build:renderer`, `npm run test:renderer`, `git diff --check`.

## Entrega

Não fazer commit de arquivos de configuração pessoais, credenciais, node_modules ou assets gerados. Registrar evidências e limites dos testes; outras abas continuam com seus fluxos existentes e precisam de etapas próprias de migração.
