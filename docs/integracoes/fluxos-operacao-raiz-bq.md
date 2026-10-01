# Fluxos por operação e raiz Bitrix no BigQuery

Investigação somente leitura do BQ em 30/09/2026, a partir do computador, sem SSH. O fluxo nativo foi implementado no aplicativo; veja [operação e validação](gerar-listas.md). A conexão Receita foi posteriormente configurada nesta máquina usando a mesma fonte do PortalDados; acesso PostgreSQL e login local BQ foram validados.

## Decisões do usuário

- Permitir múltiplos fluxos salvos, com configurações independentes por operação e pela finalidade do fluxo.
- Configurar as etapas e as opções de limpeza, deixando explícito o que estará ativo em cada execução.
- Usar raiz Bitrix/BQ da operação em lugar da raiz genérica do Neon nos fluxos configurados dessa forma.
- A raiz inclui qualquer CNPJ que já passou pelo pipeline, mesmo se o negócio foi perdido, arquivado ou movido para outro pipeline. Não filtrar por fase atual, sucesso ou situação aberta.

## Evidência atual do BQ

Na rodada iniciada às 13h11 BRT, Fastway tinha ingestão às 13h10 e o histórico Bitrix às 13h11. As quatro categorias tinham linhas ingeridas no dia. Isso comprova ingestões recentes observadas, não completude histórica ou avanço contínuo de todos os jobs.

| Operação | Categoria | Nome no catálogo de categorias | Documentos distintos de 14 dígitos no histórico disponível |
| --- | --- | --- | ---: |
| C6 | 90 | C6 - Abertura | 239.442 |
| Santander | 119 | Santander | 34.236 |
| PagBank | 34 | PagBank | 16.637 |
| Mercado Pago | 58 | MP - Hunter | 51.100 |

Os nomes foram consultados no BQ, mas o catálogo de categorias foi ingerido em 20/08/2026. Os volumes são fotografias dinâmicas; a contagem exige apenas 14 dígitos após retirar pontuação, não valida dígitos verificadores. Há valores de 13 dígitos, ausentes e malformados que exigem tratamento específico antes de considerar a raiz pronta.

Fontes confirmadas por API de metadados:

- `mbtech-bronze.bitrix.deals`: versões, `id`, `date_modify`, `ingested_at`, `payload`. CNPJ no campo `UF_CRM_1637254536351`; pipeline em `CATEGORY_ID`.
- `mbtech-bronze.bitrix.stage_history`: passagens, `deal_id`, `category_id`, `stage_id`, `ocorreu_em`, `ingerido_em`.
- `mbtech-bronze.bitrix.deal_categories`: identificadores e nomes.
- `mbtech-silver.bitrix.deals_latest`: escolhe uma versão por negócio ordenando `ingested_at DESC`. Sozinha não representa qualquer passagem pelo pipeline.

As duas tabelas bronze não tinham expiração de tabela ou partição configurada na consulta. Isso não garante que todos os negócios antigos, apagados ou anteriores à coleta estejam disponíveis. Não usar as tabelas silver com retenção curta como única memória da raiz histórica.

Jobs de evidência: categorias `8b45ab45-18cc-43d7-b7c0-1b3f6c88b4c8`, agregação histórica `afe5ea08-5938-4f34-bfa9-22aaf43c3d25`, cobertura dos documentos `bd67d4ad-4978-422d-a584-15ad2443ae69`. Saídas locais agregadas em `out/`, ignoradas pelo Git; nenhum CNPJ individual foi exportado nesta investigação.

## Composição da raiz

Unir as relações pipeline/negócio observadas nas versões de `deals` com as passagens de `stage_history`. Relacionar os negócios com seus documentos históricos e normalizar/deduplicar por operação. Se um negócio passou por duas operações, pertence às duas raízes. A lista C6 não deve excluir a carteira Santander apenas por estar em Santander.

Não limitar a raiz à data de geração, às fases atuais ou à janela recente de ingestão. Preservar zeros à esquerda. Recuperação de zeros perdidos precisa distinguir CNPJ de outros documentos e erros; não preencher números arbitrariamente. Informar negócios sem documento recuperável e cobertura do histórico.

Para uso recorrente, recomenda-se uma raiz histórica cumulativa com operação, documento, origem e datas de observação, evitando perder uma exclusão quando a fonte mudar. Criar essa estrutura no BQ e estabelecer sua atualização é uma etapa separada que exige autorização de escrita. A primeira implementação pode consultar o histórico disponível diretamente e manter um snapshot por execução, com cache separado por operação.

## Configuração de cada fluxo

Salvar identificação, nome, operação, pipelines associados, filtros de geração, sequência das etapas, opções de enriquecimento, fonte da raiz, opções de limpeza e formato de saída. Permitir duplicar um fluxo para criar variações, como C6 Prospecção e C6 Cadência.

Exibir antes de iniciar um resumo das configurações efetivas, já aplicadas as regras do perfil. Registrar uma cópia da configuração e da versão do fluxo no job; editar um fluxo salvo não deve alterar uma execução ou retomada anterior.

| Regra | Tratamento inicial |
| --- | --- |
| Raiz | Fonte por fluxo: Bitrix/BQ da operação, Neon, arquivo ou sem raiz. Exibir origem, pipelines, volume, horário da consulta e estado de cobertura. |
| Blocklist | Salvar preferência por fluxo para Davi; para os demais, manter obrigatória no backend e mostrar bloqueada na interface. Continua na fonte atual até pedido de mudança. |
| Telefone inválido do banco | Opção configurável por fluxo; distinta de número sujo detectado localmente. |
| Telefone sujo, normalização e compactação | Permanecem obrigatórios, conforme decisão anterior. Mostrar como ativos fixos. |
| Linha sem telefone | Permanece descartada após os filtros, conforme regra atual. |
| CNPJ/telefone repetido no lote | Preservar cruzamento e prioridade da primeira ocorrência válida. Índice separado por execução. |
| Telefone fixo | Opção por fluxo. |
| Backup e Livre5 | Opções por fluxo. |
| CNAE proibido | A implementação atual aplica uma lista fixa. Expor a configuração efetiva e definir listas por operação antes de ampliar essa regra; não desligar silenciosamente o filtro existente. |

## Execução proposta

Gerar → enriquecer, quando escolhido → limpar com a raiz e os filtros daquele fluxo → aplicar o formato final → disponibilizar saída. Cada etapa registra resultado explícito, contadores, arquivos e progresso. A fila de listas permanece sequencial.

Carregar a raiz uma vez por execução, usando consulta em lote/cache; não consultar o BQ para cada linha da planilha. Indisponibilidade de uma raiz BQ obrigatória deve interromper a etapa com erro claro. Não continuar com raiz vazia nem substituir automaticamente pela raiz genérica do Neon. Informar quando um cache é usado e de quando é o snapshot.

O acesso BQ dos usuários do aplicativo precisa de autenticação própria/configuração externa com permissões adequadas. O login Google deste computador, usado na investigação, não resolve a distribuição para todos os usuários. Não incluir credenciais compartilhadas no instalador.

## Estado do código

Hoje `src/main/handlers/limpeza.js` consulta `SELECT cnpj FROM raiz_cnpjs` quando Auto Raiz está ativo. Não há fonte BQ nem cadastro de múltiplos fluxos. O ajuste de fones e o cruzamento já são fixos; filtros de números inválidos, fixos, backup e Livre5 já têm opções reutilizáveis. A seleção da raiz e as configurações de execução precisam ser extraídas para serviços reutilizáveis pelo novo fluxo.
