# Modernização do Gerenciador de Bases

Status: primeira entrega aprovada e concluída em 29/09/2026. Evidências em `docs/superpowers/plans/2026-09-29-modernizacao-evidencias.md`. As etapas seguintes continuam como roteiro da migração.

## Problema e evidências

O usuário relata interface travada ou aplicativo fechado durante processamento intenso. A investigação combina leitura do código e reproduções isoladas; ainda não reproduzimos o fechamento com uma base real.

- `src/main/handlers/limpeza.js:627`: duas planilhas são processadas simultaneamente. A leitura SheetJS gera workbook, arrays e estruturas de deduplicação na memória do processo principal.
- `src/main/handlers/files.js:23`: `XLSX.read` e `XLSX.writeFile` executam trabalho síncrono no processo principal. As rotinas ExcelJS também carregam workbooks completos em diversos fluxos.
- `src/renderer/index.js:1583`: `innerHTML +=` reinterpreta todo o painel de logs e aceita HTML contido em mensagens. Os demais painéis acumulam nós sem limite e recalculam o scroll a cada mensagem.
- `src/main/database/connection.js:43`: o pool não trata o evento `error` de clientes ociosos. Esse evento pode resultar em erro não capturado.
- `src/main/database/connection.js:121`: retry lê `dbConnectionString`, mas autenticação salva `db_connection_string`. Além disso, retry tenta encerrar o pool compartilhado, afetando outras consultas.
- `src/main/handlers/auth.js:67`: controles de janela registram listeners IPC toda vez que a janela principal é criada. Logout/login acumula listeners.
- `src/main/index.js:54`: atualização baixada executa `quitAndInstall` após três segundos, inclusive durante processamento. Essa é outra possível causa de fechamento, independente de memória.
- `src/main/handlers/enriquecimento.js:389`: carga de sócios faz uma consulta por sócio. O rollback de um lote é logado, mas o erro é absorvido e a execução continua; precisa de correção específica posterior.
- Frontend atual: aproximadamente 173 KB em um único arquivo JS e 81 KB de CSS; mistura estado, montagem de HTML, eventos e regras de apresentação.

Há uma boa base para evolução: backend já dividido por funcionalidades, `contextIsolation: true`, `nodeIntegration: false`, bridge nomeada no preload, operações SQL parametrizadas e 37 testes locais passando.

### Reproduções locais

- Em Node 22.20.0, uma planilha sintética de 50 mil linhas (aproximadamente 7,9 MB), lida com `XLSX.read` e convertida com `sheet_to_json`, atrasou um timer agendado para execução imediata em 541 ms. É evidência de bloqueio síncrono, não um benchmark completo do Electron nem estimativa de uso máximo de memória.
- Carregando o módulo real de conexão em ambiente isolado, com transporte PostgreSQL substituído por um `EventEmitter`, emitir `error` no pool propagou uma exceção sem tratamento. O teste de retry confirmou a leitura de `dbConnectionString`, divergente da chave realmente salva.
- `npm test`: 37 testes passaram antes de alterações no código.

## Alternativas

1. **Migrar por abas, recomendado:** React + TypeScript + Vite convivem com a interface existente, com limites claros de propriedade do DOM. Menor risco operacional e validação por funcionalidade.
2. **Reescrever tudo de uma vez:** maior liberdade visual, mas exige validar simultaneamente todos os fluxos de negócio e amplia o risco de regressão.
3. **Extrair serviços antes do frontend:** melhora organização, porém adia o avanço visual solicitado. Extrações serão feitas conforme cada aba migrar.

## Arquitetura de destino

- Electron continua responsável pela aplicação desktop e janelas.
- React + TypeScript cuidam apenas da interface. Vite compila o frontend; não há novo servidor em produção.
- IPC nomeado continua sendo a fronteira entre interface e backend. Nenhum acesso Node ou credencial de banco no React.
- Trabalho de CPU de planilhas é movido para workers no backend, com concorrência limitada. Para volumes grandes, leitura/escrita por streaming e respeito à pressão do stream evitam armazenar múltiplas cópias da base.
- PostgreSQL/Neon e `pg` permanecem. Correção do ciclo de vida do pool precede qualquer aumento de concorrência.
- CSS existente fornece tokens, tema escuro, indigo, Orbitron, Inter e JetBrains Mono. Componentes novos usam CSS próprio limitado à área migrada.

## Primeira entrega delimitada

1. Reproduzir falhas de log sob milhares de mensagens e eventos de erro/retry do pool em testes locais, sem banco de produção.
2. Corrigir logs legados: inserir texto, agrupar atualizações e manter quantidade limitada de linhas e mensagens pendentes. Evitar interpretar mensagens como HTML.
3. Corrigir retry para reaproveitar o pool, tratar erros de clientes ociosos e evitar vazamento de pool se inicialização falhar. Registrar os controles de janela uma única vez.
4. Remover a instalação forçada da atualização em três segundos. Avisar que a atualização está pronta e instalar no encerramento normal do aplicativo.
5. Introduzir React + TypeScript + Vite e migrar **Limpeza de Colunas**, mantendo `index.html` e a navegação existente. Remover os listeners e controles legados dessa aba para não existir dupla execução.
6. Executar a limpeza de colunas em worker, uma tarefa por vez, mantendo o contrato de saída, regras numéricas e arquivos originais. Validar o comportamento sob carga sintética. Outros fluxos ainda exigirão migração específica.
7. Criar scripts de build e typecheck; verificar que iniciar e empacotar incluem os assets compilados. Dependências somente para React, tipos e compilação.

## Interface da primeira aba

Área operacional sóbria, com título e explicação curta, seleção de arquivos, lista de nomes e estado explícito (pronto/processando/concluído/com erro). Ação principal desabilitada durante execução, resumo de processados/pulados e painel de atividade limitado. Aviso sobre números truncados permanece disponível. Estados vazios e erros orientam a próxima ação. A aba continua dentro da navegação existente.

## Validação

- Testes atuais de normalização e saída continuam passando.
- Regressão para logs: volume elevado mantém limite, preserva mensagens recentes e renderiza conteúdo como texto.
- Regressão de conexão: erro de cliente ocioso tratado; retries não encerram pool em uso; inicialização fracassada libera o pool criado.
- Worker: saída equivalente, erro de arquivo propagado, concorrência limitada e loop principal responsivo durante trabalho sintético.
- Typecheck e build do React passam; verificação do carregamento da aba e dos assets quando possível no ambiente.
- Sem testes contra o Neon de produção, modificação de dados comerciais ou publicação automática.

## Etapas seguintes

1. Limpeza Local: leitura/escrita incremental, fila, cancelamento e deduplicação sem cópias integrais; tratar cada opção de negócio em regressões.
2. Enriquecimento e carga: SQL em lotes para sócios, erros transacionais explícitos, staging/migrations e gravação atômica de arquivos. Retirar `DROP TABLE` do fluxo operacional somente após análise de compatibilidade.
3. API CNPJ: locks atômicos, heartbeat independente do trabalho de CPU, cancelamento e retomada persistente.
4. Migrar outras abas e login; padronizar componentes de formulário, tabela, progresso e feedback.
5. Atualizar Electron para uma versão suportada com testes de compatibilidade, retirar bibliotecas duplicadas de planilha onde possível, empacotar recursos locais e revisar autenticação/autorização IPC.

## Fontes oficiais consultadas

- Electron, desempenho: https://www.electronjs.org/docs/latest/tutorial/performance
- Electron, versões suportadas: https://www.electronjs.org/docs/latest/tutorial/electron-timelines
- React, integração em projeto existente: https://react.dev/learn/add-react-to-an-existing-project
- React, TypeScript: https://react.dev/learn/typescript
- node-postgres, ciclo de vida e eventos do pool: https://node-postgres.com/apis/pool

## Limites da evidência

As causas de risco acima são verificáveis no código. O vínculo exato entre elas e cada fechamento relatado depende de logs e reprodução com o fluxo real. A primeira entrega estabiliza pontos delimitados e começa a migração; não equivale à modernização de todas as abas nem à comprovação de desempenho com bases de produção.
