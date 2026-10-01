# Gerenciador de Bases

Aplicativo desktop (Electron) para gerenciamento e processamento de bases de dados comerciais. Desenvolvido para operações internas da MB Finance.

## Funcionalidades

| Aba | Descrição |
|---|---|
| **Limpeza Local** | Limpeza e deduplicação de planilhas por lista raiz |
| **Consulta CNPJ API** | Fila de processamento de CNPJs via API externa |
| **Enriquecimento** | Enriquecimento de planilhas com dados do banco de dados |
| **Monitoramento** | Dashboard de métricas e acompanhamento operacional |
| **Blocklist** | Gerenciamento de registros bloqueados |
| **Relacionamento** | Pipeline de relacionamento comercial |
| **Limpeza de Colunas** | Padronização de XLSX em NOME, CPF e FONE1, com interface React e processamento em worker |
| **Gerar listas** | Geração pela Receita, enriquecimento, consulta API opcional, filtros finais e exportação com checkpoints |
| **Consulta Situação Receita** | Consulta da situação cadastral na base Receita |

## Tecnologias

- **Electron** `^28` — shell desktop cross-platform
- **Node.js** — runtime
- **PostgreSQL** (via `pg`) — banco de dados principal
- **ExcelJS / xlsx** — leitura e geração de planilhas
- **Axios** — requisições HTTP (APIs externas)
- **electron-builder** — empacotamento e distribuição
- **React + TypeScript** — interface da aba Limpeza de Colunas; migração gradual das demais abas
- **Vite** — compilação de recursos locais do frontend, sem servidor em produção

## Pré-requisitos

- Node.js 20.19+ ou 22.12+ (recomendado Node 22 LTS)
- Acesso ao banco PostgreSQL (NeonDB ou equivalente)

## Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/Inativox/GERENCIADOR-NEONDB.git
cd GERENCIADOR-NEONDB

# 2. Instale as dependências
npm install

# 3. Configure as variáveis de ambiente
cp .env.example .env
# Edite .env com suas credenciais

# 4. Inicie o app
npm start
```

## Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com base no [`.env.example`](.env.example).

| Variável | Descrição |
|---|---|
| `SMTP_USER` | E-mail remetente para notificações |
| `SMTP_PASS` | Senha de app do Gmail (ou equivalente) |
| `API_KEY` | Chave da API OpenAI |
| `C6_CLIENT_ID` | Client ID da API C6 Bank |
| `C6_CLIENT_SECRET` | Client Secret da API C6 Bank |
| `IM_CLIENT_ID` | Client ID alternativo |
| `IM_CLIENT_SECRET` | Client Secret alternativo |

> **Nunca** versione o arquivo `.env` — ele está no `.gitignore`.

## Login e Usuários

O acesso ao app é controlado por `users.json` (não versionado). Cada usuário possui papel (`role`) que define quais abas ficam visíveis.

## Build / Distribuição

```bash
# Gerar instalador Windows (.exe via NSIS)
npm run dist
```

O instalador é gerado na pasta `dist/`.

`npm start`, `npm run dist` e `npm run publish` restauram o painel compilado da instalação 1.8.0 em `out/renderer/` antes de iniciar ou empacotar. O painel preservado está versionado em `recovery/installed-v1.8.0/renderer/`; a pasta `out/` continua ignorada.

Os fontes React originais da interface 1.8.0 não estavam no instalador e ainda não foram recuperados. Os arquivos em `src/renderer/react/` são da revisão anterior. `npm run build:renderer:source` e `npm run dev:renderer` compilam esses fontes antigos e substituem temporariamente o painel em `out/renderer/`; `npm start` restaura o painel recuperado. Antes de alterar a interface em TypeScript, recupere os fontes correspondentes ou reconstrua os componentes. Veja a [origem dos arquivos e as correções](recovery/installed-v1.8.0/README.md).

### Desenvolvimento e verificações

```bash
npm test                 # Regras e regressões de backend/preload/logs
npm run typecheck        # Tipos do frontend React
npm run build:renderer   # Restaura o painel recuperado 1.8.0
npm run test:renderer    # Integração no Electron, sem usuários ou banco reais
npm run build:renderer:source # Compila os fontes React antigos (ver limitação acima)
npm run dev:renderer     # Observa os fontes React antigos
npm run benchmark:columns # Base sintética de 50 mil linhas
```

O teste de integração abre uma janela oculta com dados temporários e percorre React → preload → IPC → worker → XLSX. Também verifica navegação sem internet, limites dos logs e notificação de atualização sem bloquear o trabalho. A captura de tela fica em `out/renderer-smoke.png`.

O smoke também percorre a Limpeza Local com dois arquivos temporários, verifica a conclusão sequencial, o bloqueio de início duplicado e o salvamento de preferências. As capturas do workspace claro ficam em `out/workspace-light.png` e `out/workspace-compact.png`.

Para verificar os mesmos fluxos usando os arquivos do pacote ASAR, gere `npm run dist -- --dir` e execute `npm run test:packaged`.

### Interface recuperada e fontes da primeira migração

Os fontes da primeira migração cobrem **Limpeza de Colunas**. O painel compilado recuperado da instalação 1.8.0 também inclui **Gerar listas** e **Consulta Situação Receita**. Banco e arquivos permanecem no backend, acessados por IPC nomeado, sem Node no renderer.

A limpeza de colunas executa um lote por vez em worker, com limite de 512 MiB para o heap JavaScript. Os workbooks ainda são carregados inteiros nesse worker: arquivos que excedam o limite precisam ser divididos. Esse limite não limita todo o uso de memória nativa do processo. Streaming dos fluxos maiores de Limpeza Local e Enriquecimento é uma etapa seguinte.

Logs legados exibem as últimas 1.000 linhas; a nova aba exibe as últimas 300. Essas janelas de atividade não substituem um arquivo de auditoria permanente. Atualizações baixadas são instaladas no encerramento normal do aplicativo.

### Workspace e limpeza sequencial

O tema claro é o padrão desta revisão, conforme escolha do usuário; o tema escuro continua disponível. A preferência passa a usar `workspace-theme`, iniciando o novo visual em claro e preservando as escolhas feitas depois da revisão. As opções úteis da Limpeza Local ficam diretamente na tela, sem o antigo painel de ferramentas adicionais.

Excluir lote, dividir lista XLSX, organizar planilha diária, mesclar listas, atualizar raiz com blocklist e consultar/salvar CNPJs no histórico foram retirados da interface, da bridge e dos respectivos handlers. A seção Ações auxiliares e a exportação de histórico também foram removidas. Dados já existentes no banco não são apagados. A consulta CNPJ via API, a raiz automática, os filtros de telefone e a divisão de CSV da aba Blocklist continuam disponíveis.

Limpeza Local processa uma lista por vez e recusa lotes concorrentes e arquivos repetidos na mesma seleção. A próxima lista só começa depois da gravação e do resumo da anterior. Os controles são bloqueados durante a execução e liberados pelo evento `cleaning-finished`, preservando restrições do perfil. A Limpeza Local ainda lê planilhas completas no processo principal; a fila sequencial não equivale à migração desse fluxo para streaming ou worker.

O cruzamento é automático dentro de cada lote, inclusive em modo cadência. A ordem exibida dos arquivos define a prioridade: mantém a primeira ocorrência que sobreviver aos filtros e for gravada. CNPJ repetido remove a linha; telefone repetido remove apenas a célula nas ocorrências seguintes. A comparação normaliza pontuação, zeros à esquerda do documento e DDI 55 dos telefones. Também remove duplicações dentro de uma lista. Os índices do cruzamento são descartados ao terminar o lote.

O ajuste de telefones é obrigatório, mesmo com uma preferência antiga desativada. Ele aproveita a leitura e gravação da limpeza, sem reabrir a planilha em outra etapa. Limpa números fora de 10/11 dígitos (após retirar DDI), sequências de um único dígito no número ou após o DDD, e notação científica truncada. Não valida existência da linha telefônica. Depois dos filtros, compacta contatos na ordem fone1, fone2 etc. e exclui linhas sem nenhum telefone restante. Arquivos sem colunas fone são preservados com aviso no log, pois não permitem esse ajuste. A limpeza continua gravando no próprio arquivo, com backup quando a opção estiver ativada.

Com Auto Raiz desligado aparece a indicação **Modo cadência**; com Auto Raiz ligado ela desaparece. O ajuste obrigatório permanece ativo nos dois modos e após reiniciar os controles.

A Blocklist permanece marcada e bloqueada para todos os usuários, exceto a conta `Davi`, que pode ativar ou desativar a opção. A regra vale ao restaurar preferências, reiniciar a tela e concluir um lote. O processo principal determina a obrigatoriedade pela sessão autenticada; pedidos de outros usuários com a opção desligada ainda passam pela Blocklist e exigem conexão com o banco.

Veja o [desenho da modernização](docs/superpowers/specs/2026-09-29-modernizacao-design.md) e o [plano da primeira entrega](docs/superpowers/plans/2026-09-29-modernizacao.md).

## Estrutura do Projeto

### Configuração privada e atualização 1.7

O instalador público inclui somente código e recursos do aplicativo. `.env`, `users.json`, licenças `.mbkey`, acessos `.mbconfig` e scripts internos ficam fora do pacote. O hook `afterPack` verifica o ASAR e executa os testes Electron de interface e acesso antes que o electron-builder possa publicar.

Ao atualizar uma instalação NSIS existente, o instalador preserva uma cópia local do ASAR anterior, antes da desinstalação, em `MB Finance/Gerenciador de Bases/legacy-app.asar` no AppData do usuário (instalação individual) ou ProgramData (instalação para todos). O app extrai os três arquivos de configuração desse arquivo local para `private/access.json` no seu userData. Uma configuração já importada nunca é sobrescrita. Se o instalador não conseguir preservar o arquivo, cancela antes de remover a instalação anterior. O backup legado permanece local para outros perfis do Windows que ainda não iniciaram a nova versão.

Instalações novas mostram **Importar acesso** na tela de login. O responsável pode gerar um arquivo por usuário com `npm run config:export -- NomeDoUsuario`; a saída fica em `private-exports/`, ignorada pelo Git e pelo empacotamento. Esse arquivo contém credenciais: entregue somente ao destinatário por um canal privado, nunca como anexo da release. A conexão Neon continua na configuração local existente, e a importação de licença de API continua disponível.

Em desenvolvimento, `users.json` e `.env` locais continuam funcionando. Em produção, o app não procura esses arquivos dentro do instalador; as variáveis são aplicadas a `process.env` a partir da configuração local, respeitando valores já definidos no ambiente. A autenticação continua local; esta mudança não implanta um servidor de identidade.

Validações adicionais: `npm run test:private-config`, `node scripts/smoke-installer-migration.cjs` (Windows/NSIS) e `npm run check:package`. A migração usa dados sintéticos nos testes e não conecta a serviços de produção.

Esta mudança não remove segredos de versões públicas anteriores. Se algum instalador antigo os tiver incluído, as credenciais correspondentes precisam ser rotacionadas e os artefatos antigos revistos separadamente.

```
.
├── main.js               # Entry point do Electron (1 linha → carrega src/main/)
├── preload.js            # Bridge segura entre main e renderer (contextBridge)
├── index.html            # Interface principal do app (carrega src/renderer/)
├── login.html            # Tela de login
├── package.json          # Dependências e configurações de build
├── .env.example          # Template de variáveis de ambiente
│
├── src/
│   ├── main/                         # ── Processo Principal (Node.js) ──
│   │   ├── index.js                  # Inicializa app, janelas, auto-updater
│   │   ├── state.js                  # Estado compartilhado (pool, janelas, usuário)
│   │   ├── database/
│   │   │   ├── connection.js         # Pool PostgreSQL, retry automático, CNAEs proibidos
│   │   │   └── cache.js              # Cache em memória de CNPJs e blocklist
│   │   └── handlers/                 # Um arquivo por funcionalidade
│   │       ├── auth.js               # Login, logout, sessão, configurações de UI
│   │       ├── files.js              # Diálogos de arquivo, leitura/escrita de planilhas
│   │       ├── limpeza.js            # Fila local, filtros e cruzamento entre listas
│   │       ├── cnpj.js               # Fila da API, modo Fish, agendamento, locks
│   │       ├── enriquecimento.js     # Carga e enriquecimento de dados no BD
│   │       ├── blocklist.js          # Feed, verificação e stats da blocklist
│   │       ├── monitoramento.js      # Relatórios de monitoramento e Bitrix
│   │       └── relacionamento.js     # Pipeline de relacionamento comercial
│   │
│   └── renderer/                     # ── Processo Renderer (Browser) ──
│       └── index.js                  # Toda a lógica de interface e abas
│
├── gerenciador-backup/               # Cópia dos arquivos originais (referência)
│
├── CLAUDE.md             # Instruções do projeto para o Claude (versionado)
├── CLAUDE.local.md       # Overrides pessoais do Claude (gitignored)
│
└── .claude/
    ├── settings.json     # Permissões e config do Claude Code
    ├── commands/
    │   ├── push.md       # Comando /push — commit e push automático
    │   └── review.md     # Comando /review — revisão do código alterado
    └── rules/
        ├── code-style.md
        └── electron-conventions.md
```

### Como o código flui

```
npm start
   └── Electron carrega main.js
          └── main.js carrega src/main/index.js
                 ├── Registra todos os handlers IPC (src/main/handlers/)
                 ├── Conecta ao banco (src/main/database/)
                 └── Abre a janela → index.html
                        └── index.html carrega src/renderer/index.js
                               └── Interface das abas + comunicação com main via preload.js
```

---

## Como usar o Claude Code neste projeto

Este projeto usa o **Claude Code** (IA no terminal) com uma estrutura organizada para que o assistente entenda o contexto do projeto sem precisar de explicação toda vez.

### Os arquivos e para que servem

#### `CLAUDE.md` — Instruções da equipe (versionado no git)
É o "manual do projeto" para o Claude. Toda vez que você abre o Claude Code nesta pasta, ele lê este arquivo automaticamente. Aqui ficam:
- O que o projeto faz
- Quais arquivos fazem o quê
- Regras que **todo o time** deve seguir

> Commite este arquivo. Todos os membros do time vão se beneficiar.

#### `CLAUDE.local.md` — Seus overrides pessoais (gitignored)
Igual ao `CLAUDE.md`, mas **só para você**. Use para:
- Notas do seu ambiente local
- Lembretes temporários ("a aba X está quebrada, não mexa")
- Contexto que não faz sentido para o time todo

> Não commite este arquivo — ele é ignorado pelo git.

#### `.claude/settings.json` — Permissões do projeto (versionado)
Define o que o Claude pode fazer automaticamente sem pedir confirmação.
Exemplo: permitir rodar `git` e `npm` sem perguntar toda hora.

#### `.claude/settings.local.json` — Suas permissões pessoais (gitignored)
Igual ao `settings.json`, mas para permissões que só fazem sentido na sua máquina.

#### `.claude/commands/` — Seus comandos slash customizados
Cada arquivo `.md` aqui vira um comando que você pode chamar digitando `/nome` no Claude Code.

Exemplos deste projeto:
- `/push` — faz commit e push automaticamente
- `/review` — revisa o código alterado

Para criar um novo comando, basta criar um arquivo `.md` nesta pasta:
```
.claude/commands/meu-comando.md
```
E dentro escrever em português o que o Claude deve fazer quando você chamar `/meu-comando`.

#### `.claude/rules/` — Regras de código (aplicadas automaticamente)
Arquivos de instrução que o Claude lê automaticamente antes de escrever qualquer código. Aqui ficam convenções do projeto:
- Estilo de código
- Padrões de arquitetura
- O que não fazer

---

### Fluxo de trabalho típico

```
1. Abra o terminal na pasta do projeto
2. Digite: claude
3. Peça o que quiser em português
4. Use /push para commitar, /review para revisar
```

O Claude já vai saber o contexto do projeto porque leu o `CLAUDE.md` e as `rules/` automaticamente.

---

## Licença

Uso interno — MB Finance. Todos os direitos reservados.
