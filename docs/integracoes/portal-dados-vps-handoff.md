# PortalDados no Gerenciador — decisões e referência

Decisões confirmadas no terminal VPS pelo Maestri em 30/09/2026. Davi autorizou implementar a integração nativa. A versão 1.7.0 contém a modernização do Gerenciador; esta integração ainda não faz parte do aplicativo publicado.

## Direção aprovada

- Adaptar o motor do PortalDados para Electron/Node e interface React, dentro do sistema de abas existente.
- Consultar a mesma base CNPJ/Receita utilizada pelo PortalDados. O código e a geração dos arquivos executam no computador do usuário.
- Usar autenticação, permissões, formatos e limites do Gerenciador.
- Excluir `custo` e `custo_total` da integração.
- Permitir definir o fluxo após gerar uma listagem, incluindo enriquecimento e limpeza locais.
- Compartilhar histórico, progresso, arquivos e retomada entre as etapas. Aplicar o formato final somente após o processamento.
- Preservar alterações locais e manter credenciais fora do código e do instalador público.

A proposta inicial de chamar a API de exportação da VPS foi substituída pela geração local. Publicar a integração é uma etapa separada; a autorização de implementação não solicita um novo release.

## Referência preparada na VPS

O terminal VPS informou que preparou e conferiu um pacote sem credenciais:

`/root/portaldados-handoff-20260930/portaldados-referencia.zip`

A confirmação foi lida no terminal conectado chamado `VPS`, a partir do terminal `GERENCIADOR DE BASES APP` no workspace `AMBIENTE DAVI`. O pacote ainda precisa ser transferido e inspecionado antes da implementação. Caminhos Linux pertencem à VPS; caminhos Windows pertencem ao checkout desktop.

O clone local do Hub contém referências em `bases-cnpj/exportar_leads.py`, `bases-cnpj/formatos_exportacao.json`, `novo-front/src/pages/PortalDados.tsx` e `novo-front/src/lib/portal-dados.ts`. O motor e as regras de exportação precisam ser adaptados para Node; o fluxo não deve depender de automação de navegador.

## Regras do app que o fluxo deve preservar

- Limpeza sequencial: uma lista por vez, com cruzamento de CNPJ e telefone no mesmo lote. A primeira ocorrência válida gravada tem prioridade; CNPJ repetido exclui a linha e telefone repetido exclui apenas o contato.
- Ajuste de telefones obrigatório: normalizar, compactar, remover números sujos e eliminar linhas sem contato restante.
- Blocklist obrigatória para todos, exceto o username autenticado exato `Davi`. O backend determina a regra pela sessão.
- Auto Raiz desligado indica Modo cadência, mantendo ajuste e cruzamento obrigatórios.
- Banco, arquivos e processamento permanecem no main/worker, acessados por IPC nomeado e preload seguro. Não expor Node ao renderer.
- Reutilizar o enriquecimento do aplicativo, não o do Hub. Hoje `src/main/handlers/enriquecimento.js` emite conclusão também após erros; extrair um serviço com resultado explícito antes de encadear etapas. Um evento de conclusão não significa sucesso.

## Pontos a resolver na implementação

Usar dados intermediários com documento, CNAE e contatos enquanto as etapas processam, deixando o layout de saída para o final. Persistir um job com etapas, arquivos, contadores e resultados explícitos; falhas devem interromper as etapas seguintes. Validar retomada e cancelamento para evitar processamento duplicado.

Confirmar no pacote o esquema Receita, os filtros, a semântica dos formatos, divisão de arquivos e modo dual. Adaptar consultas com parâmetros, limites e permissões do app. Tratar conexão perdida e base Receita em atualização com mensagens amigáveis. Testes devem usar dados sintéticos, sem exportar listas reais, alterar tabelas ou reiniciar serviços na VPS.
