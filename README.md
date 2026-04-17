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
├── main.js          # Processo principal Electron + lógica de negócio
├── renderer.js      # Lógica do frontend (processo renderer)
├── preload.js       # Bridge segura entre main e renderer (contextBridge)
├── index.html       # Interface principal do app
├── login.html       # Tela de login
├── package.json     # Dependências e configurações de build
└── .env.example     # Template de variáveis de ambiente
```

## Licença

Uso interno — MB Finance. Todos os direitos reservados.
