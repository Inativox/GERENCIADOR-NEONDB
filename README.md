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

## Tecnologias

- **Electron** `^28` — shell desktop cross-platform
- **Node.js** — runtime
- **PostgreSQL** (via `pg`) — banco de dados principal
- **ExcelJS / xlsx** — leitura e geração de planilhas
- **Axios** — requisições HTTP (APIs externas)
- **electron-builder** — empacotamento e distribuição

## Pré-requisitos

- Node.js 18+
- Acesso ao banco PostgreSQL (NeonDB ou equivalente)

## Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/MB-Finance/gerenciador-de-bases.git
cd gerenciador-de-bases

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

## Estrutura do Projeto

```
.
├── main.js               # Processo principal Electron + lógica de negócio
├── renderer.js           # Lógica do frontend (processo renderer)
├── preload.js            # Bridge segura entre main e renderer (contextBridge)
├── index.html            # Interface principal do app
├── login.html            # Tela de login
├── package.json          # Dependências e configurações de build
├── .env.example          # Template de variáveis de ambiente
│
├── CLAUDE.md             # Instruções do projeto para o Claude (versionado)
├── CLAUDE.local.md       # Overrides pessoais do Claude (gitignored)
│
└── .claude/
    ├── settings.json     # Permissões e config do Claude Code (versionado)
    ├── settings.local.json  # Permissões pessoais (gitignored)
    ├── commands/         # Comandos slash customizados (/push, /review)
    │   ├── push.md
    │   └── review.md
    └── rules/            # Regras de código aplicadas automaticamente
        ├── code-style.md
        └── electron-conventions.md
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
