# Evidências da primeira entrega

Branch: `feat/modernizacao-react`. Primeira entrega concluída e validada localmente, sem publicação e sem alterar o Neon de produção.

## Execução

- Task 1: cinco regressões de conexão/ciclo de vida falharam antes da correção; as sete passaram após a alteração.
- Task 2: regressões de volume, multiline/HTML e reset de logs passaram. Fila circular limita mensagens pendentes mesmo com a janela minimizada.
- Task 3: fixture real em worker mantém original e saída numérica; informa progresso e recusa lote concorrente. Regra numérica original continua coberta pelos testes existentes.
- Task 4: unsubscribe reproduzido falhando no preload original e passando após a correção. Typecheck e build passam. O smoke executa o renderer real no Electron 28, com bridge e worker de produção, e valida a planilha gerada. Banco e autenticação reais não são inicializados.
- Primeira suíte completa: 50/50 testes passando.
- Benchmark local em Node 22.20.0: 50.000 linhas, 1.842 ms de duração; 116 heartbeats do loop principal; atraso p99 20,1 ms, máximo 21,5 ms. É uma carga sintética e não uma garantia de desempenho com qualquer base.

## Decisões durante a execução

- A autorização "pode fazer" cobre a implementação do desenho aprovado; não foi criada uma segunda etapa de autorização para o plano técnico.
- Trabalho em branch própria no workspace do usuário, preservando alterações pessoais já existentes e sem criar commits de `.agents/`, `AGENTS.md` ou credenciais.
- Vite em modo library/IIFE evita servidor em produção e convive com módulos legados. `npm run publish` compila explicitamente; evitamos um hook `prepublish` que também seria executado em instalações npm.
- O helper de logs usa extensão `.mjs` para ser importado nativamente pelo browser e pelos testes Node sem alterar o formato CommonJS do backend.
- O smoke offline revelou `Sortable is not defined`, que interrompia a configuração das abas legadas. O acesso agora é opcional: abas, logs e controles continuam funcionando; arrastar seções exige que o CDN carregue.
- O worker conserva o processador ExcelJS existente e limita seu heap. Streaming completo das abas pesadas continua na etapa seguinte; um worker não representa isolamento total de memória nativa.

## Limites

Nenhuma base comercial ou conexão Neon foi usada. Electron e bibliotecas antigas mantêm pendências de atualização fora dessa primeira entrega. A instalação npm reportou 31 avisos de vulnerabilidade no conjunto de dependências; não foi executado `npm audit fix --force` nem realizada uma atualização indiscriminada.

## Revisão final e correções

Reviewer com contexto independente identificou dois problemas importantes. Ambos foram reproduzidos em regressões antes da correção:

- Logout durante a inicialização: `closePool` agora invalida candidatos em andamento e chamadas na fila. Testes interrompem tanto o primeiro SQL quanto o último SQL da inicialização; o candidato é liberado e não reaparece na nova sessão.
- Lote da janela anterior: uma solicitação de outro renderer recebe conclusão com erro explícito, liberando os controles; início duplicado do mesmo renderer não encerra a execução original.
- A revisão local ampliou o teste de janela para o evento `closed` de uma janela anterior, preservando a referência à janela atual.

Não houve outros findings Critical ou Minor concretos. As áreas que o reviewer não julgou são etapas explicitamente posteriores (outras abas, atualização Electron, autenticação geral), regras numéricas reutilizadas e garantias de produção sem bases reais. ASAR e desempenho sintético são validados localmente nesta entrega; não se afirma isolamento absoluto de memória.

O primeiro pacote ASAR carregou React, IPC, worker e geração XLSX corretamente. A inspeção também detectou que um padrão redundante de inclusão reintroduzia sourcemap e captura de teste; o padrão foi retirado para manter apenas os recursos de execução.

## Verificação final

- `npm test`: 53 testes passando, zero falhas.
- `npm run typecheck`: passou.
- `npm run dist -- --dir`: passou; aplicativo local em `dist/win-unpacked/`.
- `npm run test:packaged`: passou, usando HTML, CSS, React, preload, handler e worker dentro do pacote ASAR.
- Inspeção do ASAR: seis recursos necessários presentes; backup, testes, fontes TSX, sourcemap e captura excluídos.
- `git diff --check`: passou, apenas avisos de normalização LF/CRLF do Git no Windows.
- Captura do renderer real: `out/renderer-smoke.png` (artefato local ignorado pelo Git).

As duas observações importantes da revisão independente foram corrigidas com regressões RED→GREEN. Trabalho mantido no workspace e branch próprios, sem integração ou publicação automática.

## Triagem do hook de design

Os dez apontamentos recebidos foram revisados no contexto dos seletores. Linhas abaixo correspondem ao relatório original, antes dos ajustes.

| Arquivo e linha | Apontamento | Resultado |
| --- | --- | --- |
| CSS 762 | Borda lateral na alça de redimensionamento | Falso positivo: o traço de 10px sinaliza uma interação de redimensionamento. Mantido; exceção `side-tab` restrita ao arquivo, depois de corrigir as quatro faixas decorativas. |
| CSS 950 | Faixa em `.file-item` | Removida; borda neutra existente preservada. |
| CSS 1753 | Faixa em `.callout` | Removida; ícone distingue informação e aviso com tokens de cor. |
| CSS 1908 | Faixa em `.summary-card` | Substituída por borda neutra uniforme de 1px. |
| CSS 2056 | Faixa em `.suspicious-summary` | Substituída por texto na cor de alerta existente. |
| CSS 526 | Elasticidade em `badgePop` | Curva de desaceleração sem ultrapassar o estado final. |
| CSS 638 | Elasticidade em `sectionMaterialize` | Mesma curva de desaceleração sem elasticidade. |
| CSS 1183 | Transição de largura do progresso | Removida; valor real continua atualizando via IPC, sem interpolar layout. |
| CSS 1421 | Transição de largura do atualizador | Removida; progresso continua disponível. |
| HTML 9 | Fonte Inter | Exceção específica `inter` restrita ao HTML: fonte exigida nas instruções AGENTS.md do usuário. |

Exceções registradas exclusivamente por `hook-admin.mjs ignore-value`, com justificativas em `.impeccable/config.json`; nenhuma regra global ou arquivo inteiro foi ignorado. A configuração de desenvolvimento foi excluída do pacote. Nenhum dos dez apontamentos ficou sem decisão.

Verificação mecânica: zero apontamentos restantes com as duas exceções aplicadas. O detector rodou em modo regex degradado por falta dos módulos de análise HTML em sua instalação; esse resultado não certifica contraste ou estilos computados. O pacote foi recompilado após os ajustes visuais.

`npm run test:packaged` passou novamente com o ASAR atualizado; a inspeção confirmou que `.impeccable/` não foi incluída no pacote.
