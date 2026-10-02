# Gerar listas no aplicativo

A aba **Gerar listas** consulta a mesma base PostgreSQL/Receita do PortalDados diretamente do computador. A geração, o enriquecimento e a limpeza rodam em um worker do Electron. Os jobs HTTP do Hub não são utilizados.

## Configurar e executar

1. Na tela de login, configure **Neon · Banco do Gerenciador** e **Receita · Banco do PortalDados** com os botões **Salvar e testar**. São bancos diferentes: Neon fornece enriquecimento/filtros/raiz comercial; Receita fornece geração e situação cadastral. A Receita valida `public.empresas` antes de salvar. Cada conexão fica no armazenamento local privado e não vai para o Git ou instalador. Falhas preservam a configuração anterior.
2. Entre com um perfil administrativo e abra **Gerar listas**. Os acessos salvos são reutilizados automaticamente; conexões antigas continuam válidas, sem preencher novamente as credenciais.
3. Para raiz Bitrix, use uma credencial Google local/ADC/login gcloud, ou **Importar chave BQ**. **Testar BQ** valida metadados e faz dry run da consulta, sem exportar documentos.
4. Crie um fluxo, escolha a operação e salve seus filtros, etapas e layout.
5. Escolha a pasta de saída e clique **Gerar lista**. O resultado aparece no **Histórico**.

Os acessos existentes do banco do Gerenciador continuam sendo usados para enriquecimento, blocklist e números inválidos. Cada computador precisa ter seus próprios acessos configurados. Não há credenciais compartilhadas dentro do pacote público.

## Regras por fluxo

- Operações iniciais: C6/90, Santander/119, PagBank/34, Mercado Pago Hunter/58. Pipelines podem ser ajustados no fluxo.
- Geração: limite, UF, cidade, bairro, período de abertura, CNAE, natureza jurídica, MEI, presença de telefone/e-mail e situação cadastral.
- **Disponibilidade salva**: **Todos** mantém a geração sem filtro de disponibilidade; **Somente disponíveis** exige o status `disponivel` em `public.limpeza_api`, no banco da Receita, por CNPJ. Clientes, resultados diferentes e empresas sem consulta são excluídos antes de aplicar o limite. O filtro usa o resultado histórico, sem chamar a API online. A seleção é salva no fluxo e congelada na execução; fluxos antigos continuam em Todos. Tabela ausente ou incompatível interrompe a geração filtrada, sem retornar todos silenciosamente.
- UF, cidades, bairros, CNAEs e naturezas jurídicas têm dropdown com busca e multiseleção. As opções vêm por SQL de `public.empresas`, sem catálogos externos. A cidade acompanha as UFs escolhidas e o bairro exige cidades selecionadas; alterar UF limpa cidades/bairros, e alterar cidades limpa bairros. Código e descrição aparecem em CNAEs/naturezas.
- Os catálogos são consultados uma vez e guardados no armazenamento local por até 24 horas, inclusive entre reinícios. Busca e paginação reutilizam esse cache. A primeira consulta de cidades/naturezas/bairros pode demorar, pois esses campos não têm índice na fonte atual. O carregamento é assíncrono, com tempo limite de três minutos e mensagens de erro; nunca retorna uma amostra como catálogo completo.
- Situação **02 · Ativa** vem selecionada como filtro de geração. A situação não é acrescentada automaticamente nos novos fluxos. Layouts personalizados podem selecionar explicitamente esses campos. Execuções antigas mantêm o snapshot de saída que já estava congelado.
- Enriquecimento: adicionar contatos, substituir os contatos ou preencher somente quando vazio; CPF do sócio opcional.
- Raiz: histórico Bitrix/BQ da operação, raiz Neon, arquivo XLSX/CSV ou sem raiz.
- A raiz BQ considera qualquer passagem disponível no pipeline, incluindo negócios perdidos ou movidos. Não usa apenas o estado atual. O histórico apresenta origem, pipelines, horário, volume e cobertura.
- Limpeza opcional: raiz, CNAEs proibidos, números inválidos do banco, telefones fixos e LIVRE5. Desligar os filtros opcionais também desliga a raiz.
- Normalização, remoção de telefones sujos, compactação, exclusão de linhas sem contato e cruzamento de CNPJ/telefone continuam obrigatórios.
- CNPJ repetido exclui a linha; telefone repetido remove somente aquele contato. A primeira ocorrência válida fica com o contato. Telefones que não cabem no layout não reservam chaves para as próximas linhas.
- Blocklist obrigatória para todos, com exceção somente do username autenticado exatamente **Davi**. O backend aplica essa política também na retomada.
- Nos fluxos **C6**, a seção **Disponibilidade na API** oferece **Validar na Limpeza API antes de exportar**. Marque ou desmarque e salve o fluxo. Quando ativa, consulta após o enriquecimento e antes dos filtros finais, com chave dupla C6/IM e intervalo de um minuto entre lotes/retentativas; somente disponíveis que sobrevivem à limpeza são exportados. A licença com as duas chaves deve estar importada no login. A opção não aparece nas outras operações.
- Nos fluxos de geração, a blocklist remove o contato bloqueado e compacta os contatos restantes. A linha é descartada quando fica sem telefone utilizável. A primeira empresa que sobreviver aos filtros mantém prioridade na deduplicação.
- Sem raiz, o resumo do fluxo exibe **Modo cadência**.

## Saídas e retomada

O leitor, a limpeza, o cruzamento da raiz e a exportação do fluxo preservam CNPJ alfanumérico. O cursor de paginação também aceita letras nas primeiras 12 posições; os dois dígitos finais permanecem numéricos. Letras nunca são removidas para fabricar um CNPJ numérico.

## Consulta independente da situação

A aba **Situação Receita** usa o mesmo acesso PostgreSQL da Receita configurado na tela de login. Consulte um CNPJ avulso ou selecione uma lista XLSX/CSV própria. As consultas por lote usam `CNPJ = ANY(...)` e não aplicam filtros de geração, raiz, telefones, enriquecimento ou blocklist.

O resultado preserva todas as linhas e as colunas originais; acrescenta código, descrição, data e motivo da situação, atualização da base, resultado da consulta e horário da consulta. Documentos inválidos e CNPJs não encontrados são sinalizados, sem excluir linhas. Uma nova cópia XLSX é gravada na mesma pasta; o original é preservado. No XLSX de entrada, a primeira aba é usada. CPF de pessoa física não é consultado na tabela de empresas.

O processamento usa um worker com lotes de 1.000 linhas, uma consulta ativa por vez e cancelamento. Saídas parciais são removidas em caso de erro/cancelamento. A consulta mostra o que consta na versão da base carregada, sem consultar o site da Receita em tempo real. O acesso segue o perfil administrativo do aplicativo.

XLSX é sempre gerado. CSV adicional pode ser ativado por fluxo, com os mesmos registros e colunas finais. CSV usa UTF-8 com BOM, separador `;` e escape de aspas/quebras de linha. Identificadores ficam como texto no XLSX, e seus dígitos são preservados no conteúdo CSV. CPF mascarado da Receita não é transformado em CPF completo.

A configuração e o layout são congelados por execução. Alterar um fluxo salvo não muda um job anterior. A geração, o enriquecimento, a limpeza e a exportação só são confirmados após gravação da etapa. Erro/cancelamento interrompe o encadeamento; **Retomar execução** reutiliza as etapas confirmadas e refaz a etapa interrompida.

Na etapa API opcional, cada metade do lote tem seu resultado confirmado separadamente. Se uma chave falhar, a retomada reutiliza a metade que respondeu, preservando o intervalo. Falha de autenticação, resposta inválida ou CNPJs inesperados interrompem o fluxo sem classificar o lote como clientes e sem gerar arquivos finais. O histórico mostra consultados, disponíveis e clientes removidos. As chaves são reservadas apenas durante essa etapa; aguarde a fila manual terminar se estiverem em uso.

Os arquivos intermediários e o snapshot da raiz ficam no armazenamento privado da execução, para permitir retomada. Não há checkbox de backup redundante para essas etapas. Os arquivos finais recebem nomes únicos, sem sobrescrever as bases do usuário. Arquivos parciais não são apresentados como resultado final.

O histórico pertence ao usuário autenticado. A interface mostra as últimas 200 execuções e até 300 mensagens/64 KiB de logs por job. Os registros completos e os arquivos permanecem no disco; esta versão não faz limpeza automática de histórico.

## Login Google e layouts

Em **Gerar listas → Acessos às fontes → BigQuery**, use **Renovar login Google** para entrar pelo navegador padrão. É necessário ter o Google Cloud CLI instalado nesta máquina. O acesso é verificado após o login, sem gerar listas. A opção **Abrir login Google automaticamente quando expirar** vem ativa e fica salva nesta máquina.

O token curto é renovado silenciosamente. Um HTTP 401 provoca uma nova tentativa com token atualizado; se o login não puder ser renovado, o navegador é aberto uma vez, com intervalo mínimo de cinco minutos entre tentativas automáticas. A execução interrompida mantém seu histórico e checkpoint: após entrar, use **Histórico → Retomar execução**. Erro de conexão ou HTTP 403/permissão não abre o login. Uma chave de serviço ou credencial importada revogada precisa ser substituída por **Importar chave BQ**; o app não troca essa credencial por uma conta pessoal.

A sessão de login é independente do processamento, com espera de até cinco minutos. A interface informa o estado e impede iniciar/retomar execuções ou importar outra chave durante a renovação. Tokens, URLs de autorização e conteúdo do CLI não são enviados ao renderer nem registrados no histórico.

Para escolher o formato, vá a **Gerar listas → Arquivos de saída → Layout**. Use **Personalizar layout** para criar uma cópia de um modelo padrão, **Novo layout** para começar do zero, ou **Editar layout** para alterar um formato próprio. Você pode renomear, adicionar, remover e reordenar colunas; selecionar um campo de dados; definir texto fixo; ou combinar de 2 a 8 campos com um separador. A prévia usa um registro fictício e considera as opções de situação cadastral, CPF e LIVRE5 do fluxo.

Salve o layout e depois o fluxo. O layout salvo já fica selecionado. As alterações funcionam imediatamente, sem editar arquivos ou reinstalar o aplicativo. Formatos próprios ficam isolados por usuário no armazenamento privado desta máquina, com até 100 layouts e 60 colunas por layout. Modelos padrão permanecem intactos. Para excluir um layout próprio, nenhum fluxo salvo pode usá-lo; o editor explica como liberar a exclusão.

Telefones podem ter qualquer nome de coluna, como “Celular principal”: selecione **Telefone 1**, **Telefone 2**, etc., sem lacunas, até Telefone 10. A quantidade de contatos reservados na limpeza considera esses campos, mesmo com cabeçalhos personalizados. Nomes de colunas duplicados e origens inválidas são rejeitados. O editor confirma antes de descartar alterações e recusa sobrescrever uma revisão desatualizada.

Novas execuções adotam a definição atual; execuções já criadas e retomadas preservam o layout congelado no snapshot, mesmo que o formato próprio seja posteriormente excluído. A ordem e os valores são os mesmos no XLSX e CSV. `src/main/flows/formats.json` contém somente os modelos distribuídos com o app; layouts próprios não alteram esse arquivo.

## Limites e validação

- Limite de empresas vazio significa todas as empresas que atendem aos filtros. Limite preenchido deve ser inteiro positivo; não há mais teto de 500 mil. A leitura ocorre em lotes de 100 mil registros, com paginação por CNPJ.
- Até 1 milhão de linhas por arquivo final.
- Uma execução de fluxo ativa por aplicativo, sem concorrer com a limpeza local.
- Raiz BQ: dry run e limite de 1 GB por consulta; até 1 milhão de documentos. Fonte vazia ou indisponível interrompe a execução, sem fallback silencioso para Neon.
- Credenciais permanecem fora dos snapshots dos jobs. Consultas PostgreSQL do worker usam conexões próprias, tempo limite e transações de leitura.
- Os fluxos usam o endpoint direto do mesmo banco Neon, com até duas conexões, para aplicar leitura e timeout de sessão sem incompatibilidade com o pooler.
- Schema Receita: CNPJ textual, razão social e situação cadastral obrigatórios. Colunas necessárias aos filtros escolhidos são verificadas antes da consulta. A paginação exige CNPJ único e ordenado.

Validação local de 30/09/2026: 146 testes unitários/integração, smoke Electron da interface, typecheck e build. O acesso BQ foi confirmado com o login local do gcloud e dry run da consulta histórica (379.017.321 bytes estimados na segunda verificação). A Receita foi configurada nesta máquina com a mesma fonte PostgreSQL do PortalDados, fora do projeto/instalador. Foram verificados os 42 campos de `public.empresas`, os filtros SQL e o esquema do enriquecimento, usando consultas sem retorno de registros. Leitura e timeouts de sessão foram confirmados em conexões diretas. Listas reais não foram utilizadas nos testes.

Benchmark com 50 mil empresas artificiais: XLSX em 5,39 s (3,34 MB); XLSX + CSV em 6,34 s (CSV adicional de 5,25 MB). São medidas locais, sem latência de banco. Execute `node scripts/benchmark-flows.cjs 50000` e acrescente `--csv` para repetir.
