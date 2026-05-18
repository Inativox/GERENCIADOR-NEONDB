# Sistema de Relacionamento — Guia de Implementação

Este documento descreve como implementar o pipeline completo de Relacionamento em outro projeto Node.js.
O pipeline cruza um relatório de elegíveis com dados do CRM (Bitrix), time de consultores e faturamento,
e gera uma base de clientes qualificados pronta para distribuição por responsável.

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Dependências](#2-dependências)
3. [Arquivos de Entrada — Formatos e Colunas](#3-arquivos-de-entrada--formatos-e-colunas)
4. [Lógica de Cruzamento das Planilhas](#4-lógica-de-cruzamento-das-planilhas)
   - 4.1 [O que é apagado / filtrado](#41-o-que-é-apagado--filtrado)
   - 4.2 [O que é cruzado e como](#42-o-que-é-cruzado-e-como)
   - 4.3 [O que é preservado na saída](#43-o-que-é-preservado-na-saída)
5. [Backend — Pipeline Principal](#5-backend--pipeline-principal)
   - 5.1 [Utilitários de normalização](#51-utilitários-de-normalização)
   - 5.2 [runFullPipeline](#52-runfullpipeline)
   - 5.3 [Modo Máquina vs Modo Relacionamento](#53-modo-máquina-vs-modo-relacionamento)
6. [Backend — Divisão por Responsável](#6-backend--divisão-por-responsável)
7. [Frontend — HTML](#7-frontend--html)
8. [Frontend — JavaScript](#8-frontend--javascript)
9. [Estrutura do arquivo de saída](#9-estrutura-do-arquivo-de-saída)
10. [Fluxo completo de uso](#10-fluxo-completo-de-uso)

---

## 1. Visão Geral

O sistema faz um **join** entre 4 planilhas usando CNPJ como chave, aplica filtros de elegibilidade e produz
uma lista final de clientes com as colunas `fase`, `responsavel`, `Supervisor` e `faturamento` preenchidas
automaticamente por lookup.

```
Relatório (elegíveis) ──┐
Bitrix (CRM)            ├──► runFullPipeline ──► elegiveis_final.xlsx
Time (supervisores)     │
Contatos (faturamento) ─┘

elegiveis_final.xlsx ──► splitByResponsible ──► pasta/ com um arquivo por consultor
```

**Dois modos de processamento:**

| Modo | Filtros aplicados | Uso |
|---|---|---|
| `maquina` | FL_ELEGIVEL=1, PJ, DT_APROVACAO vazia, STATUS_CC=LIBERADA | Base fria para URA/robô |
| `relacionamento` | PJ, STATUS_CC=LIBERADA, NIVEL_ANTERIOR=0, ALVO_ATIVACAO="QUALQUER NÍVEL" | Base para consultores |

---

## 2. Dependências

```bash
npm install xlsx exceljs
```

| Pacote | Uso |
|---|---|
| `xlsx` | Leitura e escrita de planilhas (todos os arquivos de entrada e saída) |
| `exceljs` | Divisão por responsável (copia estilos e formatação das células) |

---

## 3. Arquivos de Entrada — Formatos e Colunas

### Formatos aceitos

**Todos os 4 arquivos devem ser `.xlsx`.**
O pipeline usa `XLSX.read(buffer)` — não aceita `.csv` nem `.xls` diretamente.
Se o arquivo exportado for CSV, converta para `.xlsx` antes de usar.

### Os 4 arquivos

| # | Nome interno | O que é | Obrigatório |
|---|---|---|---|
| 1 | `relatorio` | Relatório principal de elegíveis exportado do sistema de BD | **Sim** |
| 2 | `bitrix` | Exportação do CRM Bitrix com fase do funil e responsável por CNPJ | **Sim** |
| 3 | `time` | Planilha do time — mapeia cada consultor para seu supervisor/equipe | **Sim** |
| 4 | `contatos` | Exportação de contatos do Bitrix com CNPJ e faixa de faturamento | Não |

---

### Arquivo 1 — Relatório

Lido por **nome de cabeçalho** (case-insensitive, sem acentos). Se o nome exato não for encontrado,
o sistema usa a **letra de coluna como fallback**.

| Coluna usada | Nome exato esperado | Fallback (letra) | Modo |
|---|---|---|---|
| Flag de elegível | `FL_ELEGIVEL_VENDA_C6PAY` | `AK` | Máquina |
| Data de aprovação | `DT_APROVACAO_PAY` | `AM` | Máquina |
| Tipo de pessoa | `TIPO_PESSOA` | `H` | Ambos |
| Status da conta | `STATUS_CC` | `Y` | Ambos |
| Nível anterior | `NIVEL_ANTERIOR` | `CE` | Relacionamento |
| Alvo de ativação | `ALVO_ATIVACAO` | `CD` | Relacionamento |
| CNPJ do cliente | qualquer cabeçalho contendo `"CNPJ"` | — | Ambos |

> **Atenção:** O sistema **não edita nem apaga colunas** do Relatório. Ele lê o arquivo inteiro,
> insere 4 colunas em branco nas posições C–F (fase, responsavel, Supervisor, faturamento),
> aplica os filtros de linhas e preenche essas 4 colunas via lookup.

---

### Arquivo 2 — Bitrix (CRM)

Lido por **posição fixa de coluna** (não por nome de cabeçalho). A linha 1 é ignorada (cabeçalho).

| Coluna | Posição | O que contém |
|---|---|---|
| CNPJ | **H** (índice 7) | CNPJ do cliente — chave de join com o Relatório |
| Fase | **B** (índice 1) | Fase do funil no CRM (ex: "Prospecção", "Negociação") |
| Responsável | **E** (índice 4) | Nome do consultor responsável pelo cliente |

> Linhas com CNPJ vazio na coluna H são ignoradas automaticamente.

---

### Arquivo 3 — Time (Supervisores)

Lido por **nome de cabeçalho** (busca parcial, case-insensitive).

| Coluna | Como é detectada | O que contém |
|---|---|---|
| Consultor | Cabeçalho que contenha `"CONSULTOR"` (ou 1ª coluna como fallback) | Nome do consultor |
| Equipe/Supervisor | Cabeçalho que contenha `"EQUIPE"` (ou 2ª coluna como fallback) | Nome da equipe ou supervisor |

---

### Arquivo 4 — Contatos (Faturamento) — opcional

Lido por **nome de cabeçalho**.

| Coluna | Como é detectada | O que contém |
|---|---|---|
| CNPJ | 1ª coluna que contenha `"CNPJ"` | CNPJ do cliente — chave de join |
| Faturamento | 2ª coluna do arquivo (índice 1, qualquer nome) | Faixa de faturamento |

> Se este arquivo não for fornecido, a coluna `faturamento` fica como `"Não encontrado"` para todos.

---

## 4. Lógica de Cruzamento das Planilhas

Esta é a seção mais importante. Descreve exatamente o que acontece com cada linha e coluna.

---

### 4.1 O que é apagado / filtrado

**Nenhum arquivo de entrada é modificado.** Todo o processamento ocorre em memória.
O que muda é o **arquivo de saída** — que contém apenas as linhas que passaram em todos os filtros.

#### Modo `maquina` — linhas REMOVIDAS se:

| Condição | Coluna | Valor que remove a linha |
|---|---|---|
| 1 | `FL_ELEGIVEL_VENDA_C6PAY` | Diferente de `1` (inteiro ou string) |
| 2 | `TIPO_PESSOA` | Diferente de `"PJ"` (remove PF e campos vazios) |
| 3 | `DT_APROVACAO_PAY` | Preenchida com qualquer valor (mantém só quem ainda não foi aprovado) |
| 4 | `STATUS_CC` | Diferente de `"LIBERADA"` |

Os filtros são aplicados **em cadeia** — cada filtro opera sobre o resultado do anterior.
Uma linha precisa passar nos **4 filtros** para entrar no arquivo de saída.

#### Modo `relacionamento` — linhas REMOVIDAS se:

| Condição | Coluna | Valor que remove a linha |
|---|---|---|
| 1 | `TIPO_PESSOA` | Diferente de `"PJ"` |
| 2 | `STATUS_CC` | Diferente de `"LIBERADA"` |
| 3 | `NIVEL_ANTERIOR` | Diferente de `0` (inteiro ou string `"0"`) |
| 4 | `ALVO_ATIVACAO` | Diferente de `"QUALQUER NÍVEL"` (case-insensitive) |

---

### 4.2 O que é cruzado e como

O cruzamento é feito via **lookup em memória** — sem JOIN de banco, tudo em objetos JavaScript.
São construídos 2 mapas antes de percorrer as linhas filtradas:

#### Mapa 1 — Bitrix: `CNPJ → { fase, responsavel }`

```
Chave:  CNPJ do arquivo Bitrix (coluna H), normalizado: remove tudo que não for dígito
Valor:  { fase: string, responsavel: string }

Exemplo:
  "12345678000195" → { fase: "Prospecção", responsavel: "João Silva" }
```

**Como o CNPJ é normalizado:**
- O valor bruto (ex: `"12.345.678/0001-95"` ou `12345678000195`) passa por `.replace(/\D/g, '')`
- Isso remove pontos, barras, traços — garante que `"12.345.678/0001-95"` e `12345678000195` são a mesma chave

**Como o join é feito:**
- Para cada linha filtrada do Relatório, o sistema busca qualquer cabeçalho que contenha `"CNPJ"`
- Normaliza o valor encontrado da mesma forma
- Consulta o mapa: se encontrar → preenche `fase` e `responsavel`; se não → escreve `"Não encontrado"`

#### Mapa 2 — Time: `NOME_CONSULTOR → equipe/supervisor`

```
Chave:  nome do consultor em MAIÚSCULAS (remove espaços extras)
Valor:  nome da equipe/supervisor

Exemplo:
  "JOÃO SILVA" → "Equipe Norte"
```

**Como o join é feito:**
- Após o lookup do Bitrix, o `responsavel` encontrado é normalizado para MAIÚSCULAS
- Esse nome é usado como chave no mapa do Time
- Se encontrar → preenche `Supervisor`; se não → escreve `"Não encontrado"`

#### Mapa 3 — Contatos: `CNPJ → faturamento` (opcional)

```
Chave:  CNPJ normalizado (só dígitos)
Valor:  faixa de faturamento (string ou número, o que vier na 2ª coluna)
```

**Como o join é feito:**
- O mesmo CNPJ já normalizado do lookup do Bitrix é reutilizado
- Se encontrar no mapa → preenche `faturamento`; se não → escreve `"Não encontrado"`

#### Diagrama do fluxo de lookup por linha:

```
Linha do Relatório (filtrada)
        │
        ▼
  extrai CNPJ → normaliza (só dígitos)
        │
        ├──► Mapa Bitrix ──► fase + responsavel
        │                         │
        │                         ▼
        │                  normaliza responsavel (MAIÚSCULAS)
        │                         │
        │                         └──► Mapa Time ──► Supervisor
        │
        └──► Mapa Contatos ──► faturamento
```

---

### 4.3 O que é preservado na saída

#### Modo `maquina`
- **Todas** as colunas originais do Relatório são mantidas
- As 4 colunas novas (`fase`, `responsavel`, `Supervisor`, `faturamento`) são **inseridas nas posições C–F**
  — empurrando as colunas originais que estavam a partir de C para a direita
- Linhas que não passaram nos filtros são **descartadas** (não aparecem no arquivo de saída)
- Linhas sem match no Bitrix aparecem com `"Não encontrado"` — **não são removidas**

#### Modo `relacionamento`
- As colunas originais do Relatório são **substituídas** por uma seleção específica de 14 colunas
  (ver seção 9 — Estrutura do arquivo de saída)
- Campos de CNPJ e telefone são convertidos para tipo `Number` (sem formatação)
- Campos de data (`data_base`, `dt_conta_criada`) são convertidos de serial Excel para `Date` do JavaScript

---

## 5. Backend — Pipeline Principal

### 5.1 Utilitários de normalização

```javascript
// Normaliza qualquer valor para string, nunca retorna null/undefined
const norm = v => (v === null || v === undefined) ? '' : String(v).trim();

// Normaliza em MAIÚSCULAS (usado como chave de lookup de nomes)
const normKey = v => norm(v).toUpperCase();

// Remove tudo que não for dígito (usado como chave de lookup de CNPJ)
const normCnpjKey = v => norm(v).replace(/\D/g, '');

// Converte serial de data do Excel para Date do JavaScript
const excelDateToJSDate = (serial) => {
    if (typeof serial !== 'number' || isNaN(serial)) return null;
    const utc_days = Math.floor(serial - 25569);
    return new Date(utc_days * 86400 * 1000);
};
```

---

### 5.2 runFullPipeline

```javascript
const path = require('path');
const fs = require('fs');
const fsp = require('fs').promises;
const XLSX = require('xlsx');

const NOME_PLANILHA_PRINCIPAL     = 'Sheet1';
const NOME_PLANILHA_RELACIONAMENTO = 'C6 - Relacionamento';
const NOME_PLANILHA_SUPERVISORES  = 'supervisores';

/**
 * Executa o pipeline completo de relacionamento.
 *
 * @param {object}   filePaths          - Caminhos dos 4 arquivos de entrada
 * @param {string}   filePaths.relatorio - Caminho do relatório principal
 * @param {string}   filePaths.bitrix    - Caminho do export do Bitrix (CRM)
 * @param {string}   filePaths.time      - Caminho do arquivo de time/supervisores
 * @param {string}   [filePaths.contatos]- Caminho do arquivo de contatos/faturamento (opcional)
 * @param {string}   modo               - 'maquina' | 'relacionamento'
 * @param {function} log                - Callback de log (ex: console.log)
 * @param {function} onSaveDialog       - Async fn que retorna o caminho onde salvar.
 *                                        Em Electron: usa dialog.showSaveDialog.
 *                                        Em outros ambientes: retorna um path fixo ou string.
 * @returns {{ success: boolean }}
 */
async function runFullPipeline(filePaths, modo, log = console.log, onSaveDialog) {
    try {
        log('Iniciando pipeline completo...');

        // ── 1. Lê o Relatório Principal ───────────────────────────────────
        log(`Lendo relatório: ${path.basename(filePaths.relatorio)}`);
        const relWb = XLSX.read(await fsp.readFile(filePaths.relatorio));
        const relDataAoA = XLSX.utils.sheet_to_json(
            relWb.Sheets[relWb.SheetNames[0]], { header: 1, defval: null }
        );

        if (relDataAoA.length === 0) { log('Relatório vazio. Abortando.'); return { success: false }; }

        // Insere 4 colunas em branco nas posições C, D, E, F e nomeia os cabeçalhos
        const dataComNovasColunas = relDataAoA.map(row => {
            const newRow = [...row];
            while (newRow.length < 2) newRow.push(null);
            newRow.splice(2, 0, null, null, null, null); // insere nas posições 2,3,4,5
            return newRow;
        });
        const headerRow = dataComNovasColunas[0];
        headerRow[2] = 'fase';
        headerRow[3] = 'responsavel';
        headerRow[4] = 'Supervisor';
        headerRow[5] = 'faturamento';

        const elegiveisWb = XLSX.utils.book_new();
        const elegiveisWs = XLSX.utils.aoa_to_sheet(dataComNovasColunas, { cellDates: true });
        XLSX.utils.book_append_sheet(elegiveisWb, elegiveisWs, NOME_PLANILHA_PRINCIPAL);

        // ── 2. Lê o Bitrix (CRM) ─────────────────────────────────────────
        log(`Lendo Bitrix: ${path.basename(filePaths.bitrix)}`);
        const bitrixWb = XLSX.read(await fsp.readFile(filePaths.bitrix));
        const bitrixDataAoA = XLSX.utils.sheet_to_json(
            bitrixWb.Sheets[bitrixWb.SheetNames[0]], { header: 1, defval: null }
        );

        // Colunas fixas do Bitrix: B=fase, E=responsavel, H=CNPJ
        const idxB = XLSX.utils.decode_col('B'); // 1
        const idxE = XLSX.utils.decode_col('E'); // 4
        const idxH = XLSX.utils.decode_col('H'); // 7

        const bitrixRows = [['CNPJ', 'Fase', 'Responsavel']];
        for (let i = 1; i < bitrixDataAoA.length; i++) {
            const r = bitrixDataAoA[i];
            if (!r[idxH] && r[idxH] !== 0) continue; // pula linhas sem CNPJ
            bitrixRows.push([r[idxH], r[idxB], r[idxE]]);
        }
        const relacionamentoWs = XLSX.utils.aoa_to_sheet(bitrixRows);
        XLSX.utils.book_append_sheet(elegiveisWb, relacionamentoWs, NOME_PLANILHA_RELACIONAMENTO);
        log(`${bitrixRows.length - 1} registros carregados do Bitrix.`);

        // ── 3. Lê o arquivo de Time (Supervisores) ────────────────────────
        log(`Lendo Time: ${path.basename(filePaths.time)}`);
        const timeWb = XLSX.read(await fsp.readFile(filePaths.time));
        const timeDataJson = XLSX.utils.sheet_to_json(timeWb.Sheets[timeWb.SheetNames[0]], { defval: '' });

        const timeHeaders = Object.keys(timeDataJson[0] || {});
        // Detecta a coluna de consultor e de equipe/supervisor pelo nome, com fallback para índice
        const hConsultor = timeHeaders.find(h => h && h.toUpperCase().includes('CONSULTOR')) || timeHeaders[0];
        const hEquipe    = timeHeaders.find(h => h && h.toUpperCase().includes('EQUIPE'))    || timeHeaders[1] || timeHeaders[0];

        const supervisoresRows = [['Consultor', 'Equipe']];
        for (const row of timeDataJson) {
            supervisoresRows.push([row[hConsultor] || '', row[hEquipe] || '']);
        }
        const supervisoresWs = XLSX.utils.aoa_to_sheet(supervisoresRows);
        XLSX.utils.book_append_sheet(elegiveisWb, supervisoresWs, NOME_PLANILHA_SUPERVISORES);
        log(`${supervisoresRows.length - 1} consultores carregados.`);

        // ── 4. Lê o arquivo de Contatos/Faturamento (opcional) ───────────
        const mapFaturamento = {};
        if (filePaths.contatos && fs.existsSync(filePaths.contatos)) {
            log(`Lendo Contatos: ${path.basename(filePaths.contatos)}`);
            const contatosWb = XLSX.read(await fsp.readFile(filePaths.contatos));
            const contatosDataJson = XLSX.utils.sheet_to_json(
                contatosWb.Sheets[contatosWb.SheetNames[0]], { defval: '' }
            );
            if (contatosDataJson.length > 0) {
                const contatosHeaders = Object.keys(contatosDataJson[0] || {});
                const hCnpjContatos   = contatosHeaders.find(h => h && h.toUpperCase().includes('CNPJ')) || contatosHeaders[0];
                const hFaturamento    = contatosHeaders[1];
                for (const row of contatosDataJson) {
                    const key = normCnpjKey(row[hCnpjContatos]);
                    if (key) mapFaturamento[key] = row[hFaturamento];
                }
                log('Mapa de faturamento criado.');
            }
        } else {
            log('⚠️ Arquivo de Contatos não fornecido — faturamento não será preenchido.');
        }

        // ── 5. Converte a planilha principal para JSON e aplica filtros ───
        log('Aplicando filtros de elegibilidade...');
        const elegiveisWsJson = XLSX.utils.sheet_to_json(elegiveisWs, { defval: null });
        if (elegiveisWsJson.length === 0) {
            log('Aviso: planilha principal vazia após conversão. Abortando.');
            return { success: false };
        }

        // Helper para encontrar um cabeçalho por nome ou fallback por letra de coluna
        const headersRel = Object.keys(elegiveisWsJson[0]);
        const findHeader = (primaryName, fallbackLetter) => {
            let header = headersRel.find(h => h && h.trim().toUpperCase() === primaryName.toUpperCase());
            if (header) return header;
            const colIndex = XLSX.utils.decode_col(fallbackLetter);
            if (headersRel[colIndex]) {
                log(`AVISO: "${primaryName}" não encontrado. Usando fallback '${fallbackLetter}' (${headersRel[colIndex]}).`);
                return headersRel[colIndex];
            }
            return null;
        };

        const colElegivel       = findHeader('FL_ELEGIVEL_VENDA_C6PAY', 'AK');
        const colDataAprovacao  = findHeader('DT_APROVACAO_PAY',        'AM');
        const colTipoPessoa     = findHeader('TIPO_PESSOA',             'H');
        const colStatusCC       = findHeader('STATUS_CC',               'Y');

        let dadosFiltrados = [];
        log(`Total de linhas antes do filtro: ${elegiveisWsJson.length}. Modo: ${modo}`);

        if (modo === 'relacionamento') {
            // ── Filtros do modo Relacionamento ────────────────────────────
            const colNivelAnterior = findHeader('NIVEL_ANTERIOR', 'CE');
            const colAlvoAtivacao  = findHeader('ALVO_ATIVACAO',  'CD');

            if (!colNivelAnterior || !colAlvoAtivacao || !colTipoPessoa || !colStatusCC) {
                log('Erro: colunas de filtro do modo Relacionamento não encontradas. Abortando.');
                return { success: false };
            }

            const f1 = elegiveisWsJson.filter(r => String(r[colTipoPessoa]).toUpperCase() === 'PJ');
            log(`Após TIPO_PESSOA = "PJ": ${f1.length}`);
            const f2 = f1.filter(r => String(r[colStatusCC]).toUpperCase() === 'LIBERADA');
            log(`Após STATUS_CC = "LIBERADA": ${f2.length}`);
            const f3 = f2.filter(r => r[colNivelAnterior] === 0 || r[colNivelAnterior] === '0');
            log(`Após NIVEL_ANTERIOR = 0: ${f3.length}`);
            dadosFiltrados = f3.filter(r => String(r[colAlvoAtivacao]).toUpperCase() === 'QUALQUER NÍVEL');
            log(`Após ALVO_ATIVACAO = "QUALQUER NÍVEL": ${dadosFiltrados.length}`);

        } else {
            // ── Filtros do modo Máquina ───────────────────────────────────
            if (!colElegivel || !colTipoPessoa || !colDataAprovacao || !colStatusCC) {
                log('Erro: colunas de filtro do modo Máquina não encontradas. Abortando.');
                return { success: false };
            }

            const f1 = elegiveisWsJson.filter(r => r[colElegivel] === 1 || r[colElegivel] === '1');
            log(`Após FL_ELEGIVEL = 1: ${f1.length}`);
            const f2 = f1.filter(r => String(r[colTipoPessoa]).toUpperCase() === 'PJ');
            log(`Após TIPO_PESSOA = "PJ": ${f2.length}`);
            const f3 = f2.filter(r => r[colDataAprovacao] === null || r[colDataAprovacao] === '' || r[colDataAprovacao] === undefined);
            log(`Após DT_APROVACAO_PAY vazia: ${f3.length}`);
            dadosFiltrados = f3.filter(r => String(r[colStatusCC]).toUpperCase() === 'LIBERADA');
            log(`Após STATUS_CC = "LIBERADA": ${dadosFiltrados.length}`);
        }

        if (dadosFiltrados.length === 0) {
            log('Nenhuma linha restou após os filtros. Arquivo gerado com cabeçalhos apenas.');
            return { success: true };
        }

        // ── 6. Monta os mapas de lookup ───────────────────────────────────
        log('Montando mapas de lookup CNPJ→(fase, responsavel) e Consultor→Supervisor...');

        // Mapa: CNPJ (só dígitos) → { fase, responsavel }
        const mapBitrix = {};
        for (let i = 1; i < bitrixRows.length; i++) {
            const [cnpjRaw, fase, responsavel] = bitrixRows[i];
            const key = normCnpjKey(cnpjRaw);
            if (key) mapBitrix[key] = { fase: norm(fase), responsavel: norm(responsavel) };
        }

        // Mapa: NOME_CONSULTOR_EM_MAIÚSCULAS → nome da equipe/supervisor
        const mapTime = {};
        for (let i = 1; i < supervisoresRows.length; i++) {
            const [consultorRaw, equipeRaw] = supervisoresRows[i];
            const key = normKey(consultorRaw);
            if (key && !mapTime[key]) mapTime[key] = norm(equipeRaw);
        }

        // ── 7. Aplica os lookups em cada linha filtrada ───────────────────
        log('Executando lookups...');
        let semCnpj = 0, semResp = 0, semFaturamento = 0;

        const dadosComLookups = dadosFiltrados.map(row => {
            // Encontra a chave CNPJ na linha (busca por qualquer header que contenha "CNPJ")
            const possibleCnpjKeys = Object.keys(row).filter(k => k && k.toUpperCase().includes('CNPJ'));
            const rawCnpj = possibleCnpjKeys.length > 0
                ? row[possibleCnpjKeys[0]]
                : (row['CNPJ'] || row['cnpj'] || '');
            const cnpjKey = normCnpjKey(rawCnpj);

            let fase        = 'Não encontrado';
            let responsavel = 'Não encontrado';
            let supervisor  = 'Não encontrado';
            let faturamento = 'Não encontrado';

            if (cnpjKey && mapBitrix[cnpjKey]) {
                fase        = mapBitrix[cnpjKey].fase        || 'Não encontrado';
                responsavel = mapBitrix[cnpjKey].responsavel || 'Não encontrado';
            } else { semCnpj++; }

            const respKey = normKey(responsavel);
            if (respKey && mapTime[respKey]) {
                supervisor = mapTime[respKey];
            } else if (responsavel !== 'Não encontrado') { semResp++; }

            if (cnpjKey && mapFaturamento[cnpjKey] !== undefined) {
                faturamento = mapFaturamento[cnpjKey];
            } else { semFaturamento++; }

            return { ...row, fase, responsavel, 'Supervisor': supervisor, faturamento };
        });

        log(`Lookups: ${semCnpj} CNPJs não encontrados no Bitrix | ${semResp} sem supervisor | ${semFaturamento} sem faturamento.`);

        // ── 8. Modo Relacionamento: renomeia e seleciona colunas de saída ─
        let dadosFinais = dadosComLookups;
        if (modo === 'relacionamento') {
            log('Modo relacionamento: selecionando e renomeando colunas para saída...');
            dadosFinais = dadosComLookups.map(row => {
                const findKey = (name) => Object.keys(row).find(k => k.toLowerCase() === name.toLowerCase());

                const cpfRaw  = row[findKey('cd_cpf_cnpj_cliente')];
                const foneRaw = row[findKey('telefone_master')];

                return {
                    'DATA_BASE':        excelDateToJSDate(row[findKey('data_base')]),
                    'SUPERVISOR':       row['Supervisor'],
                    'RESPONSÁVEL':      row['responsavel'],
                    'CPF':              cpfRaw  ? Number(String(cpfRaw).replace(/\D/g, ''))  : null,
                    'livre1':           row['fase'],
                    'nome':             row[findKey('nome_cliente')],
                    'fone1':            foneRaw ? Number(String(foneRaw).replace(/\D/g, '')) : null,
                    'chave':            row[findKey('email')],
                    'livre2':           row[findKey('vl_cash_in_mtd')],
                    'livre3':           row[findKey('qual a faixa de faturamento mensal da sau empresa')] || row['faturamento'],
                    'LIMITE_CONTA':     row[findKey('limite_conta')],
                    'DT_CONTA_CRIADA':  excelDateToJSDate(row[findKey('dt_conta_criada')]),
                    'CHAVES_PIX_FORTE': row[findKey('chaves_pix_forte')],
                    'LIMITE_CARTAO':    row[findKey('limite_cartao')]
                };
            });
        }

        // ── 9. Solicita caminho para salvar e grava o arquivo ─────────────
        // ADAPTE: em sistemas não-Electron, substitua onSaveDialog por um path fixo ou param
        const savePath = await onSaveDialog(`elegiveis_auto_${modo}_${Date.now()}.xlsx`);
        if (!savePath) { log('Salvamento cancelado.'); return { success: true }; }

        log(`Salvando em: ${savePath}`);
        const finalSheet = XLSX.utils.json_to_sheet(dadosFinais, { skipHeader: false, cellDates: true });
        const finalWb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(finalWb, finalSheet,      NOME_PLANILHA_PRINCIPAL);
        XLSX.utils.book_append_sheet(finalWb, relacionamentoWs, NOME_PLANILHA_RELACIONAMENTO);
        XLSX.utils.book_append_sheet(finalWb, supervisoresWs,   NOME_PLANILHA_SUPERVISORES);
        XLSX.writeFile(finalWb, savePath);

        log(`✅ Pipeline concluído. Arquivo salvo com sucesso.`);
        return { success: true };

    } catch (err) {
        log(`❌ Erro no pipeline: ${err.message}`);
        log(err.stack);
        return { success: false, error: err };
    }
}
```

---

### 5.3 Modo Máquina vs Modo Relacionamento

| Aspecto | `maquina` | `relacionamento` |
|---|---|---|
| Filtro extra | `FL_ELEGIVEL = 1` + `DT_APROVACAO vazia` | `NIVEL_ANTERIOR = 0` + `ALVO_ATIVACAO = "QUALQUER NÍVEL"` |
| Colunas de saída | Todas as colunas originais + fase/responsavel/Supervisor/faturamento | Seleção reduzida com renomeação (ver seção 8 do pipeline) |
| Público-alvo | URA / robô / disparo em massa | Consultores de relacionamento |

---

## 6. Backend — Divisão por Responsável

Ferramenta independente do pipeline. Recebe o arquivo gerado pelo pipeline (ou qualquer planilha)
e cria um arquivo `.xlsx` separado para cada valor único encontrado na coluna `RESPONSÁVEL`.

```javascript
const path = require('path');
const fs = require('fs');
const ExcelJS = require('exceljs');

/**
 * Divide um arquivo Excel em múltiplos arquivos, um por responsável.
 * Cria uma subpasta "<nome_do_arquivo>_Divididos/" com os resultados.
 *
 * @param {string}   filePath - Caminho do arquivo Excel de entrada
 * @param {function} log      - Callback de log
 * @returns {{ success: boolean, message: string }}
 */
async function splitByResponsible(filePath, log = console.log) {
    if (!fs.existsSync(filePath)) {
        log('❌ Arquivo não encontrado.');
        return { success: false, message: 'Arquivo não encontrado.' };
    }

    log(`Lendo: ${path.basename(filePath)}. Pode demorar dependendo do tamanho...`);

    try {
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.readFile(filePath);
        const worksheet = workbook.getWorksheet(1);

        if (!worksheet) throw new Error('O arquivo não possui nenhuma aba.');
        log(`Arquivo carregado. Total de linhas: ${worksheet.rowCount}`);

        // ── Detecta a coluna do responsável ──────────────────────────────
        // Procura um cabeçalho que contenha "RESPONSA" (cobre RESPONSAVEL, RESPONSÁVEL, etc.)
        let respColIndex = 3; // padrão: coluna C
        const headerRow = worksheet.getRow(1);
        headerRow.eachCell((cell, colNumber) => {
            const val = String(cell.value || '').toUpperCase();
            if (val.includes('RESPONSA')) {
                respColIndex = colNumber;
                log(`Coluna RESPONSÁVEL detectada: índice ${colNumber} ("${cell.value}")`);
            }
        });

        // ── Agrupa as linhas por responsável ─────────────────────────────
        const groups = {};
        worksheet.eachRow((row, rowNumber) => {
            if (rowNumber === 1) return;
            const cellValue = row.getCell(respColIndex).value;
            const respName  = cellValue ? String(cellValue).trim() : 'Sem Responsável';
            // Remove caracteres inválidos para nome de arquivo
            const safeName  = respName.replace(/[^a-zA-Z0-9\-_ ]/g, '').trim() || 'Desconhecido';
            if (!groups[safeName]) groups[safeName] = [];
            groups[safeName].push(row);
        });

        const responsibleNames = Object.keys(groups);
        log(`Encontrados ${responsibleNames.length} responsáveis diferentes.`);

        // ── Cria a pasta de saída ─────────────────────────────────────────
        const outputDir = path.join(
            path.dirname(filePath),
            path.basename(filePath, path.extname(filePath)) + '_Divididos'
        );
        if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);

        // ── Gera um arquivo por responsável ──────────────────────────────
        for (const respName of responsibleNames) {
            log(`Gerando: ${respName} (${groups[respName].length} linhas)...`);

            const newWb = new ExcelJS.Workbook();
            const newWs = newWb.addWorksheet('Lista');

            // Copia o cabeçalho com estilo
            const newHeaderRow = newWs.getRow(1);
            headerRow.eachCell({ includeEmpty: true }, (cell, colNumber) => {
                const newCell = newHeaderRow.getCell(colNumber);
                newCell.value = cell.value;
                newCell.style = JSON.parse(JSON.stringify(cell.style)); // cópia profunda do estilo
                const colWidth = worksheet.getColumn(colNumber).width;
                if (colWidth) newWs.getColumn(colNumber).width = colWidth;
            });
            newHeaderRow.height = headerRow.height;
            newHeaderRow.commit();

            // Copia as linhas do grupo com estilos
            for (const srcRow of groups[respName]) {
                const newRow = newWs.addRow([]);
                srcRow.eachCell({ includeEmpty: true }, (cell, colNumber) => {
                    const newCell = newRow.getCell(colNumber);
                    newCell.value = cell.value;
                    newCell.style = JSON.parse(JSON.stringify(cell.style));
                });
                newRow.height = srcRow.height;
                newRow.commit();
            }

            const savePath = path.join(outputDir, `${respName}.xlsx`);
            await newWb.xlsx.writeFile(savePath);
        }

        log(`🎉 Divisão concluída! Arquivos salvos em: ${outputDir}`);
        return { success: true, message: 'Divisão concluída com sucesso!' };

    } catch (err) {
        log(`❌ Erro: ${err.message}`);
        return { success: false, message: err.message };
    }
}
```

**Comportamento:**
- A coluna de responsável é detectada automaticamente procurando por `"RESPONSA"` no cabeçalho.
- Se não encontrar, usa Coluna C (índice 3) como padrão.
- Os arquivos são salvos numa subpasta `<nome_original>_Divididos/` no mesmo diretório do arquivo de entrada.
- O nome de cada arquivo é o nome do responsável sanitizado (sem caracteres especiais).
- Os estilos e larguras de coluna do original são preservados.

---

## 7. Frontend — HTML

```html
<!-- ════════════ ABA RELACIONAMENTO ════════════ -->
<div id="relacionamento-tab">
    <div class="grid">

        <!-- ── Card: Pipeline Principal ── -->
        <div class="card">
            <h2>Pipeline de Relacionamento</h2>
            <p>Selecione os arquivos necessários e o modo de processamento para gerar a base de elegíveis.</p>

            <div class="tool-group">
                <label>1. Arquivo de Relatório Principal:</label>
                <button id="relacionamentoSelectRelatorioBtn">Selecionar Arquivo</button>
                <div id="relacionamentoRelatorioPath" class="files"></div>
            </div>

            <div class="tool-group">
                <label>2. Arquivo do Bitrix (CRM — Relacionamento):</label>
                <button id="relacionamentoSelectBitrixBtn">Selecionar Arquivo</button>
                <div id="relacionamentoBitrixPath" class="files"></div>
            </div>

            <div class="tool-group">
                <label>3. Arquivo de Time (Supervisores):</label>
                <button id="relacionamentoSelectTimeBtn">Selecionar Arquivo</button>
                <div id="relacionamentoTimePath" class="files"></div>
            </div>

            <div class="tool-group">
                <label>4. Arquivo Contatos Bitrix (Faturamento) — opcional:</label>
                <button id="relacionamentoSelectContatosBtn">Selecionar Arquivo</button>
                <div id="relacionamentoContatosPath" class="files"></div>
            </div>

            <div class="tool-group">
                <label>5. Modo de Processamento:</label>
                <div>
                    <label>
                        <input type="radio" name="relacionamentoModo" value="maquina" checked>
                        Máquina (URA / Robô)
                    </label>
                    <label>
                        <input type="radio" name="relacionamentoModo" value="relacionamento">
                        Relacionamento (Consultores)
                    </label>
                </div>
            </div>

            <button id="relacionamentoStartBtn" style="width:100%;">Iniciar Processo</button>
        </div>

        <!-- ── Card: Logs do Pipeline (largura total) ── -->
        <div class="card full-width">
            <h2>Logs do Processo</h2>
            <div id="relacionamentoLog" class="logs">Aguardando o início do processo...</div>
        </div>

        <!-- ── Card: Divisão por Responsável (largura total, ferramenta independente) ── -->
        <div class="card full-width">
            <h2>Divisão por Responsável</h2>
            <p>
                Ferramenta independente para separar uma lista grande em várias listas por responsável.
                Funciona com qualquer planilha que tenha uma coluna "RESPONSÁVEL".
            </p>

            <div class="tool-group">
                <label>Selecione a Lista para Dividir:</label>
                <button id="selectSplitByResponsibleFileBtn">Selecionar Arquivo</button>
                <div id="splitByResponsibleFilePath" class="files"></div>
            </div>

            <button id="startSplitByResponsibleBtn" style="width:100%;">Iniciar Divisão e Salvar</button>

            <div id="splitByResponsibleLog" class="logs" style="margin-top:10px; min-height:80px;">
                Aguardando arquivo...
            </div>
        </div>

    </div>
</div>
```

---

## 8. Frontend — JavaScript

```javascript
// ════════════ RELACIONAMENTO — JS do renderer ════════════

// ── Pipeline Principal ────────────────────────────────────────────────────────
const relacionamentoLog = document.getElementById('relacionamentoLog');
let relatorioFile = null;
let bitrixFile    = null;
let timeFile      = null;
let contatosFile  = null;

const relatorioBtn  = document.getElementById('relacionamentoSelectRelatorioBtn');
const bitrixBtn     = document.getElementById('relacionamentoSelectBitrixBtn');
const timeBtn       = document.getElementById('relacionamentoSelectTimeBtn');
const contatosBtn   = document.getElementById('relacionamentoSelectContatosBtn');
const startBtn      = document.getElementById('relacionamentoStartBtn');

const relatorioPathDiv  = document.getElementById('relacionamentoRelatorioPath');
const bitrixPathDiv     = document.getElementById('relacionamentoBitrixPath');
const timePathDiv       = document.getElementById('relacionamentoTimePath');
const contatosPathDiv   = document.getElementById('relacionamentoContatosPath');

// ── Log ──────────────────────────────────────────────────────────────────────
function appendRelacionamentoLog(msg) {
    if (!relacionamentoLog) return;
    if (relacionamentoLog.textContent.trim() === 'Aguardando o início do processo...') {
        relacionamentoLog.innerHTML = '';
    }
    const p = document.createElement('p');
    p.textContent = `> ${msg.trim()}`;
    relacionamentoLog.appendChild(p);
    relacionamentoLog.scrollTop = relacionamentoLog.scrollHeight;
}

// ── Seletores de arquivo ──────────────────────────────────────────────────────
// Cria um listener genérico de seleção de arquivo para cada botão
async function bindFileSelector(button, pathDiv, setter) {
    button?.addEventListener('click', async () => {
        // ADAPTE: abra um file picker nativo ou <input type="file">
        const files = await window.electronAPI.selectFile({ title: 'Selecione o arquivo', multi: false });
        if (files && files.length > 0) {
            setter(files[0]);
            pathDiv.textContent = files[0].split(/[\\/]/).pop(); // exibe só o nome do arquivo
            appendRelacionamentoLog(`Arquivo selecionado: ${files[0].split(/[\\/]/).pop()}`);
        }
    });
}

bindFileSelector(relatorioBtn, relatorioPathDiv, v => relatorioFile = v);
bindFileSelector(bitrixBtn,    bitrixPathDiv,    v => bitrixFile    = v);
bindFileSelector(timeBtn,      timePathDiv,      v => timeFile      = v);
bindFileSelector(contatosBtn,  contatosPathDiv,  v => contatosFile  = v);

// ── Botão Iniciar ─────────────────────────────────────────────────────────────
startBtn?.addEventListener('click', () => {
    // Contatos é opcional — os outros 3 são obrigatórios
    if (!relatorioFile || !bitrixFile || !timeFile) {
        appendRelacionamentoLog('❌ ERRO: Selecione os arquivos "Relatório", "Bitrix" e "Time" antes de iniciar.');
        return;
    }

    const modo = document.querySelector('input[name="relacionamentoModo"]:checked').value;
    appendRelacionamentoLog(`Iniciando pipeline no modo "${modo}"...`);
    startBtn.disabled = true;
    startBtn.textContent = 'Processando...';

    const filePaths = {
        relatorio: relatorioFile,
        bitrix:    bitrixFile,
        time:      timeFile,
        contatos:  contatosFile // pode ser null
    };

    // ADAPTE: chame sua API ou IPC
    window.electronAPI.runRelacionamentoPipeline(filePaths, modo);
});

// ── Eventos de retorno do backend ─────────────────────────────────────────────
window.electronAPI.onRelacionamentoLog((msg) => {
    appendRelacionamentoLog(msg);
});

window.electronAPI.onRelacionamentoFinished((success) => {
    if (success) {
        appendRelacionamentoLog('✅ Processo concluído com sucesso!');
    } else {
        appendRelacionamentoLog('❌ Ocorreu um erro durante o processo. Verifique os logs acima.');
    }
    startBtn.disabled = false;
    startBtn.textContent = 'Iniciar Processo';
});

// ── Divisão por Responsável (ferramenta independente) ─────────────────────────
const splitByResponsibleLog    = document.getElementById('splitByResponsibleLog');
const selectSplitBtn           = document.getElementById('selectSplitByResponsibleFileBtn');
const startSplitBtn            = document.getElementById('startSplitByResponsibleBtn');
const splitByResponsiblePathDiv = document.getElementById('splitByResponsibleFilePath');
let splitByResponsibleFile = null;

function appendSplitLog(msg) {
    if (!splitByResponsibleLog) return;
    if (splitByResponsibleLog.textContent.trim() === 'Aguardando arquivo...') {
        splitByResponsibleLog.innerHTML = '';
    }
    const p = document.createElement('p');
    p.textContent = `> ${msg.trim()}`;
    splitByResponsibleLog.appendChild(p);
    splitByResponsibleLog.scrollTop = splitByResponsibleLog.scrollHeight;
}

selectSplitBtn?.addEventListener('click', async () => {
    const files = await window.electronAPI.selectFile({ title: 'Selecione a Lista para Dividir', multi: false });
    if (files && files.length > 0) {
        splitByResponsibleFile = files[0];
        splitByResponsiblePathDiv.textContent = files[0].split(/[\\/]/).pop();
        appendSplitLog(`Arquivo selecionado: ${files[0].split(/[\\/]/).pop()}`);
    }
});

startSplitBtn?.addEventListener('click', () => {
    if (!splitByResponsibleFile) {
        appendSplitLog('❌ ERRO: Selecione um arquivo para dividir.');
        return;
    }
    startSplitBtn.disabled = true;
    appendSplitLog('Iniciando divisão...');

    // ADAPTE: chame sua API ou IPC
    window.electronAPI.splitByResponsible(splitByResponsibleFile);
});

window.electronAPI.onSplitByResponsibleLog((msg) => {
    appendSplitLog(msg);
});

window.electronAPI.onSplitByResponsibleFinished(({ success, message }) => {
    appendSplitLog(success ? `✅ ${message}` : `❌ ${message}`);
    startSplitBtn.disabled = false;
});
```

---

## 9. Estrutura do arquivo de saída

### Modo `maquina`

Todas as colunas originais do relatório são mantidas. Quatro colunas são **preenchidas** nas posições C–F:

| Posição | Coluna | Origem |
|---|---|---|
| C | `fase` | Lookup CNPJ → Bitrix |
| D | `responsavel` | Lookup CNPJ → Bitrix |
| E | `Supervisor` | Lookup responsavel → Time |
| F | `faturamento` | Lookup CNPJ → Contatos |

### Modo `relacionamento`

Colunas selecionadas e renomeadas (ordem fixa na saída):

| Coluna de saída | Origem no relatório |
|---|---|
| `DATA_BASE` | `data_base` (convertido para Date) |
| `SUPERVISOR` | lookup via Time |
| `RESPONSÁVEL` | lookup via Bitrix |
| `CPF` | `cd_cpf_cnpj_cliente` (somente dígitos, tipo Number) |
| `livre1` | `fase` (do Bitrix) |
| `nome` | `nome_cliente` |
| `fone1` | `telefone_master` (somente dígitos, tipo Number) |
| `chave` | `email` |
| `livre2` | `vl_cash_in_mtd` |
| `livre3` | faixa de faturamento (do relatório ou do Contatos) |
| `LIMITE_CONTA` | `limite_conta` |
| `DT_CONTA_CRIADA` | `dt_conta_criada` (convertido para Date) |
| `CHAVES_PIX_FORTE` | `chaves_pix_forte` |
| `LIMITE_CARTAO` | `limite_cartao` |

O arquivo de saída contém 3 abas:
1. **Sheet1** — os dados filtrados com lookups aplicados
2. **C6 - Relacionamento** — a extração bruta do Bitrix (CNPJ, Fase, Responsável)
3. **supervisores** — a tabela de time (Consultor, Equipe)

---

## 10. Fluxo completo de uso

```
SETUP (uma vez)
└── Não requer banco de dados — tudo opera sobre arquivos locais

EXECUÇÃO DO PIPELINE
1. Exportar do sistema:
   ├── Relatório de elegíveis → arquivo Relatório
   ├── Funil Bitrix → arquivo Bitrix
   ├── Planilha do time → arquivo Time
   └── Contatos Bitrix → arquivo Contatos (opcional)

2. Selecionar os 4 arquivos na interface

3. Escolher o modo:
   ├── "Máquina"       → clientes elegíveis para URA/robô
   └── "Relacionamento" → clientes para os consultores abordarem

4. Clicar em "Iniciar Processo"
   └── Pipeline gera: elegiveis_auto_<modo>_<timestamp>.xlsx

DISTRIBUIÇÃO (opcional)
5. Selecionar o arquivo gerado na ferramenta "Divisão por Responsável"
6. Clicar em "Iniciar Divisão"
   └── Gera pasta: <nome_arquivo>_Divididos/
       ├── Joao Silva.xlsx
       ├── Maria Souza.xlsx
       └── ...um arquivo por consultor
```

---

> **Nota sobre adaptação para sistemas não-Electron:**
> As chamadas `window.electronAPI.*` são a bridge IPC do Electron.
> Em um sistema web convencional, substitua por chamadas `fetch`:
> - `runRelacionamentoPipeline(filePaths, modo)` → `POST /api/relacionamento/run`
> - `splitByResponsible(filePath)` → `POST /api/relacionamento/split`
> - Os logs em tempo real podem ser enviados via WebSocket ou Server-Sent Events (SSE).
> - O `onSaveDialog` pode ser substituído por um path de saída configurável no servidor.
