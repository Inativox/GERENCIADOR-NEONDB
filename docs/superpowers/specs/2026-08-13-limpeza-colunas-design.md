# Limpeza de Colunas — Design

**Data:** 2026-08-13
**Status:** Aprovado, aguardando plano de implementação

## Problema

As planilhas de origem chegam com dezenas de colunas irrelevantes e com os campos de
telefone e CNPJ sujos: notação científica, sufixo decimal `,00`, pontos, traços,
parênteses e espaços. Hoje isso é limpo à mão antes de subir a base pro discador.

## Objetivo

Uma aba nova no app que recebe uma ou mais planilhas e devolve, para cada uma, um
arquivo com exatamente três colunas — `NOME`, `CPF`, `FONE1` — já normalizadas e
prontas para uso.

## Escopo

Só a **primeira aba** de cada arquivo é lida. Nenhum registro é removido: a feature
opera sobre colunas, não sobre registros. A única exceção é a linha em que as três
colunas ficam vazias — normalmente sobra de formatação no fim da planilha, e não é um
registro. Não há consulta a banco de dados nem a API.

## Arquitetura

Handler novo em `src/main/handlers/limpezaColunas.js`, seguindo o padrão dos demais
handlers do projeto: exporta `register()`, que é chamado por `src/main/index.js`.

| Camada | Mudança |
|---|---|
| `src/main/handlers/limpezaColunas.js` | **novo** — leitura, limpeza e escrita |
| `src/main/index.js` | `require` do handler + chamada a `.register()` |
| `preload.js` | `startLimpezaColunas`, `onLimpezaColunasLog`, `onLimpezaColunasFinished` |
| `index.html` | item na sidebar após Relacionamento + `<div class="page-content" id="limpezaColunas">` |
| `src/renderer/index.js` | entrada em `gridConfigs`, entrada em `tabInfo`, listeners dos botões |

### Canais IPC

| Canal | Direção | Payload |
|---|---|---|
| `start-limpeza-colunas` | renderer → main (`send`) | `string[]` com os caminhos dos arquivos |
| `limpeza-colunas-log` | main → renderer | `string` — uma linha de log |
| `limpeza-colunas-finished` | main → renderer | `{ success, processados, pulados }` |

O handler é protegido por `isAdmin()` e registra a execução via `logSystemAction`,
igual às demais ferramentas do app.

### Biblioteca

**ExcelJS** para leitura e escrita, conforme a regra do `CLAUDE.md`. É também o que dá
controle de `numFmt` por célula, necessário para a máscara do CNPJ. Formatos de entrada
aceitos: `.xlsx` e `.csv`. O `.xls` legado não é suportado pelo ExcelJS — nesse caso o
arquivo é pulado com log pedindo conversão para `.xlsx`.

## Regras de limpeza

### Colunas de saída

A saída tem exatamente três colunas, nesta ordem:

| Cabeçalho procurado na origem | Vira | Tipo da célula | Formato |
|---|---|---|---|
| Nome do Negócio | `NOME` | texto | — |
| CNPJ | `CPF` | número | `00000000000000` |
| Telefone Celular | `FONE1` | número | `0` |

A máscara `00000000000000` no CPF preserva o zero à esquerda de CNPJs como
`04.252.011/0001-10` na exibição, mantendo a célula numérica. O formato `0` no FONE1
impede que o Excel volte a exibir o número em notação científica.

### Localização do cabeçalho

O nome do cabeçalho é normalizado antes da comparação: maiúsculas, acentos removidos,
espaços colapsados e aparados. A busca tenta, em ordem:

1. Match exato do nome normalizado (`NOME DO NEGOCIO`, `CNPJ`, `TELEFONE CELULAR`)
2. Contém o termo específico (`NEGOCIO`, `CNPJ`, `CELULAR`)
3. Contém o termo genérico (`NOME`, `TELEFONE`)

O CNPJ não tem passo 3 — o termo já é específico o bastante.

### Normalização de FONE1 e CPF

As duas colunas usam a mesma função. A ordem dos passos é significativa:

```
1. célula numérica?     -> String(Math.round(valor))
                           (o sufixo ,00 e a notação científica somem sozinhos)
2. texto com e/E?       -> Number() primeiro, depois o passo 1
3. texto: corta decimal -> remove o casamento de /[.,]\d{1,2}$/
4. remove todo caractere que não seja dígito
                           (cobre  .  ,  espaço  -  (  )  / )
5. resultado vazio      -> célula em branco, nunca zero
```

O passo 3 **precisa** vir antes do passo 4. Sem ele, `"5521998364849,00"` teria a
vírgula removida pelo passo 4 e viraria `552199836484900`, um telefone inválido.

Exemplos:

```
5,52199E+12        -> 5521998364849
5521998364849,00   -> 5521998364849
(21) 99836-4849    -> 21998364849
04.252.011/0001-10 -> 4252011000110   (exibido como 04252011000110)
""                 -> célula em branco
```

## Fluxo de uso

0. A aba exibe um aviso fixo instruindo a preparar a planilha de origem antes: formatar
   as colunas CNPJ e Telefone Celular como **Número** com **0 casas decimais**. É o
   único jeito de garantir que os dígitos cheguem inteiros — coluna como Texto exibindo
   `5,52199E+12` já perdeu a informação no arquivo.
1. O usuário seleciona uma ou mais planilhas (seleção múltipla no diálogo nativo).
2. Clica em "Iniciar Limpeza".
3. Cada arquivo gera um `<nome>_LIMPO.xlsx` na **mesma pasta do original**.
4. Ao final, **nada é aberto fora do app** — o usuário permanece no gerenciador. O
   caminho da pasta de saída vai para o log, para ele saber onde os arquivos caíram
   sem trocar de janela.

O arquivo original nunca é aberto para escrita. Se `<nome>_LIMPO.xlsx` já existir, o
próximo vira `<nome>_LIMPO_1.xlsx`, `<nome>_LIMPO_2.xlsx` e assim por diante — a
feature nunca sobrescreve um arquivo existente.

## Tratamento de erro

Todo erro é tratado **por arquivo**, nunca aborta o lote:

| Situação | Comportamento |
|---|---|
| Uma das três colunas não encontrada | Pula o arquivo, log diz qual coluna faltou |
| Arquivo corrompido ou ilegível | Pula o arquivo, log com a mensagem do erro |
| `.xls` legado | Pula o arquivo, log pede conversão para `.xlsx` |
| Planilha com cabeçalho mas sem linhas de dados | Gera o arquivo só com os cabeçalhos, log avisa |
| Planilha totalmente vazia | Cai na regra de coluna não encontrada e é pulada |

Exemplo de log:

```
base_jul.xlsx  -> OK (12.480 linhas)
base_ago.xlsx  -> PULADO
   coluna "Telefone Celular" nao encontrada
base_set.xlsx  -> OK (9.112 linhas)

Concluido: 2 de 3 arquivos.
```

## Testes

O projeto não tem framework de teste instalado. As regras de normalização serão
**funções puras exportadas** pelo handler:

- `normalizarDigitos(valor)` — os cinco passos da seção anterior
- `acharCabecalho(cabecalhos, alvo)` — a busca em três níveis, devolve o índice
  0-based ou `-1`
- `normalizarCabecalho(valor)` / `textoDe(valor)` / `desembrulhar(valor)` — auxiliares

Isso permite um script de asserções em `node` puro, sem subir o Electron, cobrindo:

- os quatro exemplos da tabela de normalização
- valor vazio, `null` e `undefined`
- texto sem nenhum dígito
- cabeçalho com acento, com espaço sobrando e em caixa mista
- cabeçalho ausente (deve retornar `null`, não lançar)

O fluxo de arquivo em si — seleção, escrita, sufixo anti-colisão, pular arquivo com
coluna faltando — é validado rodando o app com planilhas reais.

## Decisões registradas

| Decisão | Escolha | Motivo |
|---|---|---|
| "Tirar os 00 do final" | É o sufixo decimal `,00` / `.00` | Confirmado pelo usuário; os zeros não são dígitos reais do telefone |
| Tipo da coluna CPF | Número com máscara de 14 dígitos | Preserva o zero à esquerda sem abrir mão do tipo numérico |
| Saída | Um arquivo por entrada, na pasta de origem | Permite lote; original intacto |
| Coluna faltando | Pula o arquivo, segue o lote | Um arquivo ruim não deve travar os outros |
| Posição na HUD | Aba própria na sidebar, após Relacionamento | Ferramenta independente do pipeline de relacionamento |

## Emenda — CSV recusado na entrada (2026-08-13)

O CSV chegou a ser aceito na entrada e foi **removido**. Entra `.xlsx`, sai `.xlsx`.

### Por que CSV não serve aqui

O motivo é o mesmo problema que a feature existe para resolver. Quando o Excel exibe um
telefone como `5,52199E+12` e a planilha é exportada para CSV, ele grava o **texto
exibido**, não o valor: sobram 6 algarismos significativos de 13. Os outros 7 dígitos
deixam de existir no arquivo.

Expandir esse texto produz `5521990000000` — um telefone plausível e errado, que iria
para o discador. Foi exatamente o sintoma relatado: colunas inteiras terminando em zeros.

No `.xlsx` isso não acontece: o valor numérico está guardado com precisão total e a
notação científica é só máscara de exibição. É por isso que a conversão funciona e o
`,00` some — a parte decimal nunca foi dígito de telefone.

### Guarda contra científico truncado

Um `.xlsx` ainda pode conter a string `"5,52199E+12"` numa coluna formatada como Texto.
Para esse caso `normalizarDigitos` compara os algarismos significativos do texto com o
tamanho do número expandido: se o texto tem menos, o valor é irrecuperável e a função
devolve vazio em vez de inventar dígito.

Esses casos são contados por arquivo (`truncados`) e o log explica ao usuário como
reexportar. Célula em branco é melhor que telefone errado: não discar é melhor que
discar para o número de outra pessoa.

## Fora de escopo

- Remoção de linhas duplicadas ou de telefones inválidos — já existe em outras abas
- Validação de dígito verificador de CNPJ
- Leitura de abas além da primeira
- Suporte a `.xls` legado
