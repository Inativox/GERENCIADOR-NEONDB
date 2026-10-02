# BigQuery como Auto Raiz na Limpeza Local

Na aba **Limpeza Local**, ative **Auto Raiz**, escolha a **Fonte do Auto Raiz** e selecione **BigQuery**. O campo **Pipeline** oferece C6 — Abertura (90), Santander (119), PagBank (34) e Mercado Pago — Hunter (58). Os nomes e IDs usam o mesmo catálogo das operações do aplicativo.

Use **Entrar no Google**, **Importar chave BQ** ou **Testar acesso** nessa própria aba. O acesso Google é compartilhado com a integração BQ existente, sem exigir a criação de um fluxo. Para login por navegador, o Google Cloud CLI precisa estar instalado; uma chave privada importada também pode ser utilizada. Credenciais continuam no armazenamento privado desta máquina.

Depois, adicione as listas e clique **Iniciar limpeza**. A raiz é consultada uma vez para o lote inteiro, antes de ler, criar backups ou alterar qualquer planilha. Ela contém CNPJs que passaram pelo pipeline selecionado no histórico disponível de negócios e fases do Bitrix, incluindo negócios perdidos ou movidos. A limpeza remove as empresas presentes nessa raiz e mantém o processamento sequencial e o cruzamento entre as listas.

O log identifica operação, pipeline e quantidade de CNPJs carregados. CNPJs com zero inicial perdido por célula numérica e CNPJs alfanuméricos são comparados corretamente com a raiz BQ. Fonte vazia, falta de acesso, timeout ou falha da consulta interrompem o lote antes de alterar os originais; não há troca automática para outra raiz. Logout ou fechamento da janela cancelam uma consulta BQ em andamento.

**Banco de Dados** permanece como fonte padrão para preferências antigas. A fonte e a operação selecionadas são lembradas nas preferências da interface. Desligar Auto Raiz permite selecionar um arquivo raiz ou usar Modo cadência. **Alimentar Raiz (BD)** importa documentos para o banco do Gerenciador.

A blocklist e a consulta de números inválidos continuam usando o banco do Gerenciador, também quando a raiz vem do BigQuery. A blocklist permanece obrigatória para todos, exceto o username autenticado exatamente Davi. A fonte e o pipeline ficam bloqueados durante a execução do lote.

Validação com arquivos e acesso sintéticos: quatro operações, carregamento único por lote, ordem sequencial, conteúdo dos XLSX resultantes, preferências restauradas, consulta indisponível/vazia, preservação dos originais, cancelamento da raiz no logout, políticas de blocklist e restauração dos controles após falha. Os testes não consultam bancos ou listas de produção.
