# Workspace claro e simplificação da Limpeza Local

Solicitação do usuário: retirar as quatro seções da imagem (Excluir Lote, Dividir Lista, Organizar Planilha Diária e Mesclar), consultar/salvar CNPJs no histórico e atualizar a raiz com blocklist. Voltar a uma lista por vez e renovar o visual. Direção escolhida explicitamente: claro, com aparência de ferramenta de trabalho.

## Entrega

- Tema claro inicial; superfícies brancas, fundo cinza frio, navegação compacta, indigo nas ações e melhor hierarquia de texto. Inter e Orbitron da identidade existente preservadas. Tema escuro continua selecionável; a nova preferência usa `workspace-theme`.
- Limpeza Local com preparação e atividade lado a lado. Opções restantes disponíveis na tela e ações auxiliares em disclosure. Os layouts antigos dessa aba não são restaurados sobre a nova organização fixa.
- Controles, listeners, preferências e métodos do preload das funcionalidades retiradas foram removidos. Também foram removidos handlers e helpers usados exclusivamente nessas funcionalidades, incluindo as duas definições duplicadas de organização no preload. Nenhum dado histórico ou tabela foi apagado.
- Mantidas consulta CNPJ via API, raiz automática, alimentação da raiz, filtros de telefone, exportação do histórico e divisão de CSV na aba Blocklist.
- Removidos efeitos de reconstrução, luzes ambientais e embaralhamento de títulos. O texto permanece estável durante navegação, sem animações contínuas de decoração.
- `start-cleaning` usa loop sequencial e bloqueio de lote concorrente. Cada arquivo termina seus logs e o ajuste opcional de fones antes do seguinte. Eventos para janela destruída são descartados; um novo renderer recebe erro de lote ocupado sem encerrar o proprietário.
- `cleaning-finished` libera controles mantendo estados anteriores, inclusive opções obrigatórias/desabilitadas do perfil. O resumo diferencia arquivos processados e pulados, evitando anunciar sucesso completo quando houve arquivos ignorados.
- Opções antigas `checkDb` e `saveToDb` não reativam consultas ou escrita do histórico. Corrigido o listener de backup, que procurava um input dentro do próprio input disparador.

## Validação

- Quatro regressões iniciais falharam no código anterior: duas listas em paralelo, lote duplicado, flags antigas de histórico e ausência de liberação explícita após erro. Passaram depois da correção.
- Regressões adicionais cobrem nova janela, janela destruída e arquivo pulado.
- `npm test`: 60 testes, zero falhas.
- `npm run typecheck`: passou.
- `npm run test:renderer`: passou com arquivos temporários, React/worker real e Limpeza Local real, sem conexão ao Neon ou carregamento de usuários reais.
- O smoke verifica remoção de controles/bridge, compatibilidade das preferências antigas, backup, Livre5, conclusão de duas listas, início duplicado, restrição desabilitada preservada, troca de tema e ausência de overflow horizontal no workspace a 960×800.
- `npm run dist -- --dir`: pacote local recompilado.
- `npm run test:packaged`: passou com os arquivos do ASAR final. Inspeção dos arquivos do projeto confirma CSS do workspace e backend presentes, sem testes, scripts, documentação, config de design, sourcemaps ou capturas no pacote.
- Capturas em `out/workspace-light.png`, `out/workspace-compact.png` e `out/renderer-smoke.png`, não versionadas.
- Detector de design: zero apontamentos nos arquivos alterados; sua instalação usa fallback regex, portanto não certifica contraste computado. Exceções anteriores de Inter e da alça de resize continuam documentadas e limitadas aos arquivos pertinentes.

## Limite técnico

A Limpeza Local continua usando planilhas completas no processo principal. Reduzir a concorrência evita duas bases simultâneas e organiza o log, mas não equivale à migração desse fluxo para worker/streaming. Só a Limpeza de Colunas usa worker nesta etapa. Não foram adicionadas dependências nem realizada publicação automática.
