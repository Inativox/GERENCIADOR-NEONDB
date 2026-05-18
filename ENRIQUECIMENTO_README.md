# Sistema de Enriquecimento — Guia de Implementação

Este documento detalha **tudo** sobre o sistema de enriquecimento: schema do banco, formato dos arquivos,
lógica de carga, lógica de enriquecimento, estratégias de preenchimento, opções de ano e interface.
Nenhum detalhe foi omitido — siga à risca para uma implementação sem falhas.

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Dependências](#2-dependências)
3. [Schema do Banco de Dados](#3-schema-do-banco-de-dados)
4. [Fase 1 — Carga no BD (Planilha Mestra)](#4-fase-1--carga-no-bd-planilha-mestra)
   - 4.1 [Formato e colunas do arquivo mestre](#41-formato-e-colunas-do-arquivo-mestre)
   - 4.2 [O que é lido e como é salvo](#42-o-que-é-lido-e-como-é-salvo)
   - 4.3 [Código completo: startDbLoad](#43-código-completo-startdbload)
5. [Fase 2 — Enriquecimento das Planilhas](#5-fase-2--enriquecimento-das-planilhas)
   - 5.1 [Formato e colunas do arquivo a enriquecer](#51-formato-e-colunas-do-arquivo-a-enriquecer)
   - 5.2 [Opções de ano de busca](#52-opções-de-ano-de-busca)
   - 5.3 [Estratégias de preenchimento](#53-estratégias-de-preenchimento)
   - 5.4 [Query SQL executada](#54-query-sql-executada)
   - 5.5 [O que é escrito no arquivo](#55-o-que-é-escrito-no-arquivo)
   - 5.6 [Código completo: runEnrichmentProcess](#56-código-completo-runenrichmentprocess)
6. [Download dos dados enriquecidos](#6-download-dos-dados-enriquecidos)
7. [Frontend — HTML](#7-frontend--html)
8. [Frontend — JavaScript](#8-frontend--javascript)
9. [Fluxo completo de uso](#9-fluxo-completo-de-uso)

---

## 1. Visão Geral

O sistema funciona em **duas fases independentes**:

```
FASE 1 — CARGA
Planilha Mestra (.xlsx) ──► startDbLoad ──► BD PostgreSQL
                                              ├── tabela: empresas  (cnpj, ano)
                                              ├── tabela: telefones (empresa_id, numero)
                                              └── tabela: socios    (empresa_id, cpf, nome, telefones[])

FASE 2 — ENRIQUECIMENTO
Planilha a enriquecer (.xlsx) ──► runEnrichmentProcess ──► mesma planilha sobrescrita
  (tem coluna cnpj/cpf)              (busca por CNPJ no BD)    (colunas fone preenchidas)
                                                                (coluna status: "Enriquecido"/"Pobre")
```

A Fase 1 precisa ser feita **uma única vez** (ou quando a base mestre for atualizada).
A Fase 2 pode ser executada quantas vezes quiser sobre qualquer planilha que tenha CNPJ.

---

## 2. Dependências

```bash
npm install exceljs pg
```

| Pacote | Uso |
|---|---|
| `exceljs` | Leitura e escrita de `.xlsx` em ambas as fases (streaming na carga, in-memory no enriquecimento) |
| `pg` | Cliente PostgreSQL — pool de conexões com `connect()`/`BEGIN`/`COMMIT`/`ROLLBACK` |

**Formato de arquivo:** apenas `.xlsx`. O sistema **não aceita** `.csv`, `.xls` ou outros formatos.

---

## 3. Schema do Banco de Dados

Execute este SQL **uma única vez** antes de usar o sistema:

```sql
-- Tabela principal: uma linha por CNPJ
CREATE TABLE IF NOT EXISTS empresas (
    id   SERIAL PRIMARY KEY,
    cnpj TEXT    NOT NULL UNIQUE,
    ano  INTEGER
);

-- Telefones diretos da empresa (N telefones por empresa)
CREATE TABLE IF NOT EXISTS telefones (
    id         SERIAL  PRIMARY KEY,
    empresa_id INTEGER NOT NULL REFERENCES empresas(id) ON DELETE CASCADE,
    numero     TEXT    NOT NULL,
    UNIQUE (empresa_id, numero)   -- evita duplicatas por empresa
);

-- Sócios da empresa com seus telefones pessoais
CREATE TABLE IF NOT EXISTS socios (
    id         SERIAL   PRIMARY KEY,
    empresa_id INTEGER  NOT NULL REFERENCES empresas(id) ON DELETE CASCADE,
    cpf        TEXT     NOT NULL,
    nome       TEXT     NOT NULL DEFAULT 'Desconhecido',
    telefones  TEXT[]   DEFAULT '{}',          -- array de strings
    UNIQUE (empresa_id, cpf)
);
```

### Relacionamento entre as tabelas

```
empresas (id, cnpj, ano)
    │
    ├──── telefones (empresa_id → empresas.id, numero)
    │         └── um registro por telefone direto da empresa
    │
    └──── socios (empresa_id → empresas.id, cpf, nome, telefones[])
              └── array de telefones dentro do próprio registro do sócio
```

### Por que `ON DELETE CASCADE`?

Se uma empresa for deletada, todos os seus telefones e sócios são deletados automaticamente.
Isso evita registros órfãos.

---

## 4. Fase 1 — Carga no BD (Planilha Mestra)

### 4.1 Formato e colunas do arquivo mestre

**Formato:** `.xlsx` obrigatório.
**Aba lida:** sempre a primeira aba (`workbook.worksheets[0]`).
**Linha 1:** cabeçalho — os nomes das colunas determinam o que é lido.

O sistema detecta as colunas exclusivamente pelo **nome exato do cabeçalho** (lowercase, sem espaços extras).
Não há fallback por letra de coluna.

#### Coluna obrigatória

| Nome no cabeçalho | O que é | Regra de validação |
|---|---|---|
| `cpf` **ou** `cnpj` | CNPJ/CPF da empresa — chave primária | `.replace(/\D/g, '')` → deve ter **≥ 8 dígitos** após limpeza |

Linhas cujo CNPJ tenha menos de 8 dígitos após limpeza são **ignoradas silenciosamente**.

#### Colunas de telefones diretos da empresa

Detectadas por regex: `/^(fone|telefone|celular)/` (case-insensitive, após lowercase).

Exemplos de nomes que são capturados:
- `fone1`, `fone2`, `fone16`
- `telefone1`, `telefone_principal`
- `celular`, `celular1`

Regra: o valor da célula é lido como string (`.value`), convertido com `.trim()`, e aceito se não estiver vazio.
Telefones com menos de 8 dígitos após `.replace(/\D/g, '')` são **descartados** no momento do INSERT.

#### Colunas de sócios

Os sócios são identificados por um sufixo numérico `_N` (ex: `_1`, `_2`, `_3`).

| Padrão de nome | Regex | O que é |
|---|---|---|
| `cpf_socio_1`, `cpf_socio_2` | `/^cpf_socio_(\d+)$/` | CPF do N-ésimo sócio |
| `nome_socio_1`, `nome_socio_2` | `/^nome_socio_(\d+)$/` | Nome do N-ésimo sócio |
| `celular1_socio_1`, `celular2_socio_1` | `/^celular\d+_socio_(\d+)$/` | Telefone X do N-ésimo sócio |

O número `N` agrupa as colunas: `cpf_socio_1`, `nome_socio_1`, `celular1_socio_1`, `celular2_socio_1` pertencem ao mesmo sócio 1.

**Regras de validação do sócio:**
- CPF: `.replace(/\D/g, '').padStart(11, '0')` — o zero à esquerda perdido pelo Excel é restaurado
- CPF com menos de 10 dígitos após limpeza (antes do padStart) é **ignorado**
- Nome: se vazio ou ausente, usa `"Desconhecido"`
- Telefones do sócio: mesma regra dos telefones diretos (≥ 8 dígitos)

**Exemplo de cabeçalho válido:**
```
cnpj | fone1 | fone2 | cpf_socio_1 | nome_socio_1 | celular1_socio_1 | cpf_socio_2 | nome_socio_2
```

---

### 4.2 O que é lido e como é salvo

#### Fluxo por linha do arquivo mestre:

```
Para cada linha (a partir da linha 2):
  1. Extrai CNPJ → .replace(/\D/g, '').trim()
  2. Se CNPJ < 8 dígitos → IGNORA a linha

  3. Se tiver colunas fone/telefone/celular:
     → coleta todos os valores não-vazios → acumula em phoneDataMap[cnpj]

  4. Se tiver colunas cpf_socio_N:
     → para cada N, extrai { cpf, nome, phones }
     → CPF < 10 dígitos → ignora esse sócio
     → acumula em socioDataMap[cnpj]

  5. A cada 5.000 linhas → chama saveChunk() e limpa os mapas
  6. Ao final do arquivo → chama saveChunk() com o restante
```

#### O que saveChunk() faz no banco (dentro de uma transação):

```
BEGIN

1. INSERT INTO empresas (cnpj, ano)
   SELECT unnest($1::text[]), $2
   ON CONFLICT (cnpj) DO UPDATE SET ano = EXCLUDED.ano
   -- Se o CNPJ já existir, ATUALIZA o ano

2. SELECT id, cnpj FROM empresas WHERE cnpj = ANY(...)
   -- Busca os IDs gerados para montar o mapa cnpj → empresa_id

3. Para cada telefone direto:
   INSERT INTO telefones (empresa_id, numero) ...
   ON CONFLICT (empresa_id, numero) DO NOTHING
   -- Ignora duplicatas silenciosamente

4. Para cada sócio:
   INSERT INTO socios (empresa_id, cpf, nome, telefones)
   ON CONFLICT (empresa_id, cpf) DO UPDATE
   SET nome = EXCLUDED.nome, telefones = EXCLUDED.telefones
   -- Se o sócio já existir, ATUALIZA nome e telefones

COMMIT (ou ROLLBACK em caso de erro)
```

**Importante:** a carga é **idempotente** — pode ser executada múltiplas vezes sobre o mesmo arquivo
sem criar duplicatas. O `ON CONFLICT` garante isso.

---

### 4.3 Código completo: startDbLoad

```javascript
const path = require('path');
const fs = require('fs');
const ExcelJS = require('exceljs');

/**
 * Carrega arquivos mestre (.xlsx) no banco de dados.
 *
 * @param {object}   pool        - pg.Pool
 * @param {string[]} masterFiles - Array de caminhos absolutos dos arquivos .xlsx
 * @param {number}   year        - Ano a associar aos CNPJs (ex: 2024)
 * @param {function} log         - Callback de log
 * @param {function} onProgress  - Callback(current, total, fileName, totalCnpjsProcessed)
 */
async function startDbLoad(pool, masterFiles, year, log = console.log, onProgress = () => {}) {
    if (!year) {
        log('❌ ERRO CRÍTICO: O ano não foi fornecido para a carga no banco de dados.');
        return;
    }

    log(`--- Iniciando Carga para o Banco de Dados (Ano: ${year}) ---`);

    // Garante que as tabelas de sócios existem (idempotente)
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS socios (
                id         SERIAL  PRIMARY KEY,
                empresa_id INTEGER NOT NULL REFERENCES empresas(id) ON DELETE CASCADE,
                cpf        TEXT    NOT NULL,
                nome       TEXT    NOT NULL DEFAULT 'Desconhecido',
                telefones  TEXT[]  DEFAULT '{}',
                UNIQUE (empresa_id, cpf)
            );
        `);
        await pool.query(`ALTER TABLE socios ADD COLUMN IF NOT EXISTS nome TEXT NOT NULL DEFAULT 'Desconhecido';`);
        await pool.query(`ALTER TABLE socios ADD COLUMN IF NOT EXISTS telefones TEXT[] DEFAULT '{}';`);
    } catch (err) {
        log(`❌ Erro ao inicializar tabelas de sócios: ${err.message}`);
        return;
    }

    let totalCnpjsProcessed = 0;

    // Salva um chunk no BD dentro de uma transação atômica
    const saveChunk = async (phoneDataMap, socioDataMap, filePath) => {
        // phoneDataMap: Map<cnpj_string, string[]>  (telefones diretos)
        // socioDataMap: Map<cnpj_string, {cpf, nome, phones[]}[]>
        const allCnpjs = new Set([...phoneDataMap.keys(), ...socioDataMap.keys()]);
        if (allCnpjs.size === 0) return;

        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            const uniqueCnpjs = Array.from(allCnpjs);

            // 1. Upsert das empresas
            await client.query(
                `INSERT INTO empresas (cnpj, ano)
                 SELECT unnest($1::text[]), $2
                 ON CONFLICT (cnpj) DO UPDATE SET ano = EXCLUDED.ano;`,
                [uniqueCnpjs, year]
            );

            // 2. Busca os IDs gerados
            const { rows: empresaRows } = await client.query(
                `SELECT id, cnpj FROM empresas WHERE cnpj = ANY($1::text[])`,
                [uniqueCnpjs]
            );
            const empresaIdMap = new Map(empresaRows.map(r => [r.cnpj, r.id]));

            // 3. Insert dos telefones diretos
            const phoneValues = [];
            for (const [cnpj, phones] of phoneDataMap.entries()) {
                const empresaId = empresaIdMap.get(cnpj);
                if (!empresaId) continue;
                const uniquePhones = [...new Set(phones)]
                    .filter(p => String(p).replace(/\D/g, '').length >= 8);
                uniquePhones.forEach(phone =>
                    phoneValues.push({ empresa_id: empresaId, numero: String(phone) })
                );
            }
            if (phoneValues.length > 0) {
                await client.query(
                    `INSERT INTO telefones (empresa_id, numero)
                     SELECT (d.v->>'empresa_id')::int, d.v->>'numero'
                     FROM jsonb_array_elements($1::jsonb) d(v)
                     ON CONFLICT (empresa_id, numero) DO NOTHING`,
                    [JSON.stringify(phoneValues)]
                );
            }

            // 4. Upsert dos sócios
            for (const [cnpj, socios] of socioDataMap.entries()) {
                const empresaId = empresaIdMap.get(cnpj);
                if (!empresaId) continue;
                for (const { cpf, nome, phones } of socios) {
                    if (!cpf) continue;
                    const uniquePhones = [...new Set(phones)]
                        .filter(p => String(p).replace(/\D/g, '').length >= 8);
                    await client.query(
                        `INSERT INTO socios (empresa_id, cpf, nome, telefones)
                         VALUES ($1, $2, $3, $4)
                         ON CONFLICT (empresa_id, cpf)
                         DO UPDATE SET nome = EXCLUDED.nome, telefones = EXCLUDED.telefones`,
                        [empresaId, cpf, nome, uniquePhones]
                    );
                }
            }

            await client.query('COMMIT');
            totalCnpjsProcessed += allCnpjs.size;
        } catch (error) {
            await client.query('ROLLBACK');
            log(`❌ ERRO no lote do arquivo ${path.basename(filePath)}: ${error.message}`);
        } finally {
            client.release();
        }
    };

    for (let fileIndex = 0; fileIndex < masterFiles.length; fileIndex++) {
        const filePath = masterFiles[fileIndex];
        const fileName = path.basename(filePath);
        onProgress(fileIndex + 1, masterFiles.length, fileName, totalCnpjsProcessed);
        log(`\nProcessando arquivo mestre: ${fileName}`);

        try {
            const workbook = new ExcelJS.Workbook();
            await workbook.xlsx.readFile(filePath);
            const worksheet = workbook.worksheets[0];

            if (!worksheet || worksheet.rowCount === 0) {
                log(`⚠️ Arquivo ${fileName} vazio ou inválido. Pulando.`);
                continue;
            }

            // ── Lê o cabeçalho e monta o mapa de colunas ─────────────────
            const headerMap = new Map(); // colNum → header_lowercase
            worksheet.getRow(1).eachCell({ includeEmpty: true }, (cell, colNum) => {
                headerMap.set(colNum, String(cell.value || '').trim().toLowerCase());
            });

            let cnpjColIdx = -1;
            const phoneColIdxs = [];
            // socioColMap: Map<N, { cpfColIdx, nomeColIdx, phoneColIdxs[] }>
            const socioColMap = new Map();

            for (const [colNum, header] of headerMap.entries()) {
                // Coluna CNPJ/CPF da empresa
                if (header === 'cpf' || header === 'cnpj') {
                    cnpjColIdx = colNum;
                    continue;
                }
                // CPF do sócio N: cpf_socio_1, cpf_socio_2, ...
                const cpfSocioMatch = header.match(/^cpf_socio_(\d+)$/);
                if (cpfSocioMatch) {
                    const n = parseInt(cpfSocioMatch[1]);
                    if (!socioColMap.has(n)) socioColMap.set(n, { cpfColIdx: -1, nomeColIdx: -1, phoneColIdxs: [] });
                    socioColMap.get(n).cpfColIdx = colNum;
                    continue;
                }
                // Nome do sócio N: nome_socio_1, nome_socio_2, ...
                const nomeSocioMatch = header.match(/^nome_socio_(\d+)$/);
                if (nomeSocioMatch) {
                    const n = parseInt(nomeSocioMatch[1]);
                    if (!socioColMap.has(n)) socioColMap.set(n, { cpfColIdx: -1, nomeColIdx: -1, phoneColIdxs: [] });
                    socioColMap.get(n).nomeColIdx = colNum;
                    continue;
                }
                // Telefone X do sócio N: celular1_socio_1, celular2_socio_1, ...
                const celularSocioMatch = header.match(/^celular\d+_socio_(\d+)$/);
                if (celularSocioMatch) {
                    const n = parseInt(celularSocioMatch[1]);
                    if (!socioColMap.has(n)) socioColMap.set(n, { cpfColIdx: -1, nomeColIdx: -1, phoneColIdxs: [] });
                    socioColMap.get(n).phoneColIdxs.push(colNum);
                    continue;
                }
                // Telefones diretos da empresa: fone*, telefone*, celular*
                if (/^(fone|telefone|celular)/.test(header)) {
                    phoneColIdxs.push(colNum);
                }
            }

            // Ordena colunas de telefone dos sócios por posição
            for (const [, data] of socioColMap.entries()) {
                data.phoneColIdxs.sort((a, b) => a - b);
            }

            // Diagnóstico: loga colunas suspeitas encontradas
            const suspectHeaders = [...headerMap.values()].filter(h =>
                h.includes('socio') || h.includes('cpf') || h.includes('celular') || h.includes('fone')
            );
            if (suspectHeaders.length > 0) {
                log(`Colunas encontradas: ${suspectHeaders.join(' | ')}`);
            }

            if (cnpjColIdx === -1) {
                log(`❌ ERRO: Coluna CNPJ/CPF não encontrada em ${fileName}. Pulando.`);
                continue;
            }
            if (phoneColIdxs.length === 0 && socioColMap.size === 0) {
                log(`❌ ERRO: Nenhuma coluna de telefone ou sócio encontrada em ${fileName}. Pulando.`);
                continue;
            }
            log(`Colunas: CNPJ=col${cnpjColIdx} | Fones diretos=${phoneColIdxs.length} | Sócios=${socioColMap.size}`);

            // ── Processa as linhas em chunks de 5000 ─────────────────────
            let phoneDataMap = new Map();
            let socioDataMap = new Map();

            for (let i = 2; i <= worksheet.rowCount; i++) {
                const row = worksheet.getRow(i);
                const cnpj = String(row.getCell(cnpjColIdx).value || '').replace(/\D/g, '').trim();
                if (cnpj.length < 8) continue; // linha inválida

                // Telefones diretos
                if (phoneColIdxs.length > 0) {
                    const phones = phoneColIdxs
                        .map(idx => String(row.getCell(idx).value || '').trim())
                        .filter(Boolean);
                    if (phones.length > 0) {
                        phoneDataMap.set(cnpj, [...(phoneDataMap.get(cnpj) || []), ...phones]);
                    }
                }

                // Sócios
                if (socioColMap.size > 0) {
                    const sociosForRow = [];
                    for (const [, { cpfColIdx, nomeColIdx, phoneColIdxs: socioPhoneCols }] of socioColMap.entries()) {
                        if (cpfColIdx === -1) continue;
                        const cpfRaw = String(row.getCell(cpfColIdx).value || '').replace(/\D/g, '').trim();
                        if (cpfRaw.length < 10) continue; // CPF inválido
                        const cpf = cpfRaw.padStart(11, '0'); // restaura zero à esquerda do Excel
                        const nome = (nomeColIdx !== -1
                            ? String(row.getCell(nomeColIdx).value || '').trim()
                            : '') || 'Desconhecido';
                        const phones = socioPhoneCols
                            .map(idx => String(row.getCell(idx).value || '').trim())
                            .filter(Boolean);
                        sociosForRow.push({ cpf, nome, phones });
                    }
                    if (sociosForRow.length > 0) {
                        socioDataMap.set(cnpj, [...(socioDataMap.get(cnpj) || []), ...sociosForRow]);
                    }
                }

                // Flush a cada 5.000 linhas para não estourar memória
                if (i % 5000 === 0) {
                    await saveChunk(phoneDataMap, socioDataMap, filePath);
                    phoneDataMap = new Map();
                    socioDataMap = new Map();
                    onProgress(fileIndex + 1, masterFiles.length, fileName, totalCnpjsProcessed);
                }
            }

            // Flush final
            if (phoneDataMap.size > 0 || socioDataMap.size > 0) {
                await saveChunk(phoneDataMap, socioDataMap, filePath);
            }

        } catch (err) {
            log(`❌ ERRO ao processar ${fileName}: ${err.message}`);
        }
    }

    log(`\n✅ Carga finalizada. Total de ${totalCnpjsProcessed} CNPJs únicos processados.`);
}
```

---

## 5. Fase 2 — Enriquecimento das Planilhas

### 5.1 Formato e colunas do arquivo a enriquecer

**Formato:** `.xlsx` obrigatório.
**Aba lida:** sempre a primeira aba (`workbook.worksheets[0]`).
O arquivo é **sobrescrito** no mesmo caminho ao final. Use a opção `backup` para preservar o original.

#### Colunas detectadas por nome exato no cabeçalho (lowercase):

| Nome no cabeçalho | Obrigatório | O que é |
|---|---|---|
| `cpf` ou `cnpj` | **Sim** | Chave de busca no BD — identifica a empresa |
| `fone1`, `fone2`, ... `foneN` | Sim (ao menos 1) | Colunas onde os telefones serão escritos |
| `status` | Não | Onde será escrito `"Enriquecido"` ou `"Pobre"` — criada automaticamente se ausente |

**Regras de detecção:**
- `fone`: qualquer cabeçalho que **comece com** `"fone"` (ex: `fone1`, `fone2`, `fone16`)
- As colunas fone são ordenadas por índice de coluna crescente antes de preencher
- Se `status` não existir, é criada na primeira coluna após a última coluna existente

---

### 5.2 Opções de ano de busca

O ano controla qual coluna `ano` da tabela `empresas` é consultada.

| Opção no frontend | `useAllDb` | `usePadrao` | `anosDeBusca` na query |
|---|---|---|---|
| Ano específico (ex: 2024) | `false` | `false` | `[2024]` |
| Ano + "213 PADRÃO" | `false` | `true` | `[2024, 1]` — inclui registros com `ano = 1` |
| "Todo Banco" | `true` | — | Sem filtro de ano (cláusula `AND e.ano = ANY(...)` removida) |

**O que é o "213 PADRÃO" (ano = 1)?**
Alguns registros de bases antigas são carregados com `ano = 1` (código interno para
filiais ou matrizes sem ano definido). Ativar o padrão garante que esses registros
também apareçam no resultado.

---

### 5.3 Estratégias de preenchimento

Define o que fazer quando o CNPJ da linha **é encontrado** no BD e tem telefones disponíveis.

| Estratégia | `strategy` | Condição para processar | Como preenche |
|---|---|---|---|
| **Anexar** | `"append"` | Há pelo menos uma coluna fone **vazia** na linha | Une os telefones existentes na planilha com os do BD. Remove duplicatas. Preenche do fone1 em diante. |
| **Sobrescrever** | `"overwrite"` | Sempre (ignora o que está na linha) | Apaga todos os valores nas colunas fone e escreve os telefones do BD. |
| **Ignorar** | `"ignore"` | Todas as colunas fone estão **vazias** | Escreve os telefones do BD apenas se a linha não tiver nenhum telefone. |

**Lógica de decisão exata (código):**
```javascript
const existingPhones = phoneCols
    .map(idx => String(row.getCell(idx).value || '').trim())
    .filter(Boolean); // lista de telefones que já estão na linha

const shouldProcess =
    (strategy === "overwrite") ||
    (strategy === "append"    && existingPhones.length < phoneCols.length) || // tem espaço livre
    (strategy === "ignore"    && existingPhones.length === 0);                // está completamente vazia
```

**Como os telefones são combinados (por estratégia):**
```javascript
let finalPhones = [];
if (strategy === "overwrite") {
    finalPhones = [...new Set(phonesFromDb)]; // só os do BD, deduplicados
} else if (strategy === "append") {
    finalPhones = [...new Set([...existingPhones, ...phonesFromDb])]; // existentes + BD, deduplicados
} else { // "ignore"
    finalPhones = [...new Set(phonesFromDb)]; // só os do BD (a linha estava vazia)
}
// Apenas os primeiros N telefones cabem (N = número de colunas fone na planilha)
finalPhones = finalPhones.slice(0, phoneCols.length);
```

**Como os telefones são escritos na célula:**
```javascript
// Antes de escrever: apaga TODAS as colunas fone da linha
phoneCols.forEach(idx => { row.getCell(idx).value = null; });

// Depois: escreve cada telefone como Number (sem formatação de texto)
finalPhones.forEach((phone, index) => {
    const numericStr = String(phone).replace(/\D/g, '');
    if (numericStr) {
        const cell = row.getCell(phoneCols[index]);
        cell.value = Number(numericStr);  // tipo numérico
        cell.numFmt = '0';               // formato sem casas decimais
    }
});
```

---

### 5.4 Query SQL executada

Para cada lote de CNPJs, uma única query busca **telefones da empresa + telefones dos sócios**:

```sql
SELECT
    e.cnpj,
    -- Telefones dos sócios: desaninha o array telefones[] de todos os sócios
    (
        SELECT array_agg(p)
        FROM socios s, unnest(s.telefones) AS p
        WHERE s.empresa_id = e.id
    ) AS socio_phones,
    -- CPFs dos sócios: agrega todos os CPFs dos sócios da empresa
    (
        SELECT array_agg(s.cpf) FROM socios s WHERE s.empresa_id = e.id
    ) AS socio_cpfs,
    -- Telefones diretos da empresa, ordenados por id de inserção
    array_agg(t.numero ORDER BY t.id) AS empresa_phones
FROM empresas e
LEFT JOIN telefones t ON e.id = t.empresa_id
WHERE e.cnpj = ANY($1::text[])
  AND e.ano = ANY($2::integer[])   -- ← OMITIDO se useAllDb = true
GROUP BY e.id, e.cnpj;
```

**Como o resultado é processado:**
```javascript
result.rows.forEach(row => {
    const socioPhones   = row.socio_phones  || [];
    const empresaPhones = (row.empresa_phones || []).filter(Boolean); // remove nulls do LEFT JOIN
    const socioCpfs     = row.socio_cpfs    || [];

    // Combina os dois arrays e remove duplicatas com Set
    const combinedPhones = [...new Set([...socioPhones, ...empresaPhones])];

    // Armazena um objeto com telefones e CPFs
    if (combinedPhones.length > 0 || socioCpfs.length > 0) {
        enrichmentDataForBatch.set(row.cnpj, {
            phones: combinedPhones,
            cpfs: socioCpfs
        });
    }
    // CNPJs sem dados (telefones ou CPFs) não entram no mapa
});
```

---

### 5.5 O que é escrito no arquivo

Para **cada linha** processada, independente de o CNPJ ter sido encontrado ou não:

| Situação | Coluna `status` | Colunas `fone` |
|---|---|---|
| CNPJ encontrado no BD e `shouldProcess = true` | `"Enriquecido"` | Preenchidas conforme a estratégia |
| CNPJ encontrado no BD mas `shouldProcess = false` | `"Pobre"` | Não alteradas |
| CNPJ **não encontrado** no BD | `"Pobre"` | Não alteradas |
| CNPJ vazio na linha | `"Pobre"` | Não alteradas |

O arquivo é salvo com `workbook.xlsx.writeFile(filePath)` ao final do processamento de cada arquivo
(não a cada lote — o ExcelJS mantém tudo em memória e salva uma única vez).

---

### 5.6 Código completo: runEnrichmentProcess

```javascript
const path = require('path');
const fs = require('fs');
const ExcelJS = require('exceljs');

function formatEta(totalSeconds) {
    if (!isFinite(totalSeconds) || totalSeconds < 0) return 'Calculando...';
    const m = Math.floor(totalSeconds / 60);
    const s = Math.floor(totalSeconds % 60);
    return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

/**
 * Enriquece planilhas com telefones do banco de dados.
 *
 * @param {object}   options
 * @param {Array}    options.filesToEnrich  - Array de { path: string, id: string }
 * @param {string}   options.strategy       - 'append' | 'overwrite' | 'ignore'
 * @param {boolean}  options.backup         - Criar cópia .backup_enrich_<ts>.xlsx antes de processar
 * @param {number}   options.year           - Ano para filtro no BD (null se useAllDb)
 * @param {number}   options.batchSize      - Linhas por lote de consulta (padrão: 2000)
 * @param {boolean}  options.usePadrao      - Inclui ano=1 ("213 PADRÃO") na busca
 * @param {boolean}  options.useAllDb       - Ignora filtro de ano completamente
 * @param {object}   pool                   - pg.Pool
 * @param {function} log                    - Callback de log
 * @param {function} onProgress             - Callback(id, percent, etaString)
 * @param {function} onFinish               - Chamado ao finalizar todos os arquivos
 */
async function runEnrichmentProcess(options, pool, log = console.log, onProgress = () => {}, onFinish = () => {}) {
    const { filesToEnrich, strategy, backup, year, batchSize, usePadrao, useAllDb } = options;

    if (!useAllDb && !year) {
        log('❌ ERRO CRÍTICO: O ano não foi fornecido para o enriquecimento.');
        onFinish();
        return;
    }

    const BATCH_SIZE = batchSize || 2000;

    // Monta o array de anos para o filtro SQL
    let anosDeBusca = [];
    if (!useAllDb) {
        anosDeBusca = usePadrao ? [year, 1] : [year];
    }

    log(`--- Iniciando Processo de Enriquecimento ---`);
    log(`Tamanho do Lote: ${BATCH_SIZE.toLocaleString('pt-BR')} registros.`);
    log(`Ano(s) de Busca: ${useAllDb ? 'TODO O BANCO' : anosDeBusca.join(', ')}${usePadrao ? ' (213 PADRÃO ATIVADO)' : ''}`);

    let totalEnriched = 0, totalProcessed = 0, totalNotFound = 0;

    try {
        for (const fileObj of filesToEnrich) {
            const { path: filePath, id } = fileObj;
            const startTime = Date.now();
            log(`\nProcessando: ${path.basename(filePath)}`);
            onProgress(id, 0, null);

            if (backup) {
                const p = path.parse(filePath);
                fs.copyFileSync(filePath, path.join(p.dir, `${p.name}.backup_enrich_${Date.now()}${p.ext}`));
                log(`Backup criado.`);
            }

            try {
                const workbook = new ExcelJS.Workbook();
                await workbook.xlsx.readFile(filePath);
                const worksheet = workbook.worksheets[0];

                // ── Detecta colunas ───────────────────────────────────────
                let cnpjCol = -1, statusCol = -1;
                const phoneCols = [];

                worksheet.getRow(1).eachCell((cell, colNum) => {
                    const h = String(cell.value || '').trim().toLowerCase();
                    if (h === 'cpf' || h === 'cnpj') {
                        cnpjCol = colNum;
                    } else if (h.startsWith('fone')) {
                        phoneCols.push(colNum);
                    } else if (h === 'status') {
                        statusCol = colNum;
                    }
                });

                phoneCols.sort((a, b) => a - b);

                if (cnpjCol === -1) {
                    log(`❌ ERRO: Coluna 'cpf'/'cnpj' não encontrada em ${path.basename(filePath)}. Pulando.`);
                    continue;
                }

                // Cria coluna 'status' se não existir
                if (statusCol === -1) {
                    statusCol = worksheet.columnCount + 1;
                    worksheet.getCell(1, statusCol).value = 'status';
                }

                const totalRows = worksheet.rowCount - 1;
                const totalBatches = Math.ceil(totalRows / BATCH_SIZE);
                let enrichedInFile = 0, notFoundInFile = 0;

                log(`${totalRows} linhas, ${totalBatches} lote(s) de ${BATCH_SIZE}.`);

                // ── Processa em lotes ─────────────────────────────────────
                for (let i = 2; i <= worksheet.rowCount; i += BATCH_SIZE) {
                    const currentBatchNum = Math.floor((i - 2) / BATCH_SIZE) + 1;
                    const endIndex = Math.min(i + BATCH_SIZE - 1, worksheet.rowCount);

                    // Coleta CNPJs do lote: Map<cnpj, { rowNum, row }>
                    const cnpjsInBatch = new Map();
                    for (let j = i; j <= endIndex; j++) {
                        const row = worksheet.getRow(j);
                        const cnpj = String(row.getCell(cnpjCol).text || '').replace(/\D/g, '').trim();
                        if (cnpj) cnpjsInBatch.set(cnpj, { rowNum: j, row });
                    }
                    if (cnpjsInBatch.size === 0) continue;

                    log(`Lote ${currentBatchNum}/${totalBatches}: ${cnpjsInBatch.size} CNPJs...`);

                    // ── Query no BD ───────────────────────────────────────
                    const cnpjKeys = Array.from(cnpjsInBatch.keys());
                    const enrichmentData = new Map(); // cnpj → string[] de telefones

                    let queryText = `
                        SELECT
                            e.cnpj,
                            (SELECT array_agg(p) FROM socios s, unnest(s.telefones) AS p
                             WHERE s.empresa_id = e.id) AS socio_phones,
                            array_agg(t.numero ORDER BY t.id) AS empresa_phones
                        FROM empresas e
                        LEFT JOIN telefones t ON e.id = t.empresa_id
                        WHERE e.cnpj = ANY($1::text[])
                    `;
                    const queryParams = [cnpjKeys];

                    if (!useAllDb) {
                        queryText += ` AND e.ano = ANY($2::integer[])`;
                        queryParams.push(anosDeBusca);
                    }
                    queryText += ` GROUP BY e.id, e.cnpj;`;

                    const result = await pool.query(queryText, queryParams);
                    result.rows.forEach(dbRow => {
                        const socioPhones   = dbRow.socio_phones || [];
                        const empresaPhones = (dbRow.empresa_phones || []).filter(Boolean);
                        const combined = [...new Set([...socioPhones, ...empresaPhones])];
                        if (combined.length > 0) enrichmentData.set(dbRow.cnpj, combined);
                    });

                    log(`Lote ${currentBatchNum}/${totalBatches}: ${enrichmentData.size} CNPJs com telefones. Atualizando planilha...`);

                    // ── Aplica na planilha ────────────────────────────────
                    for (const [cnpj, { row }] of cnpjsInBatch.entries()) {
                        let rowWasEnriched = false;

                        if (enrichmentData.has(cnpj)) {
                            const phonesFromDb = enrichmentData.get(cnpj);
                            const existingPhones = phoneCols
                                .map(idx => String(row.getCell(idx).value || '').trim())
                                .filter(Boolean);

                            const shouldProcess =
                                (strategy === 'overwrite') ||
                                (strategy === 'append' && existingPhones.length < phoneCols.length) ||
                                (strategy === 'ignore'  && existingPhones.length === 0);

                            if (shouldProcess) {
                                rowWasEnriched = true;

                                let finalPhones = [];
                                if (strategy === 'overwrite') {
                                    finalPhones = [...new Set(phonesFromDb)];
                                } else if (strategy === 'append') {
                                    finalPhones = [...new Set([...existingPhones, ...phonesFromDb])];
                                } else {
                                    finalPhones = [...new Set(phonesFromDb)];
                                }

                                // Limpa todas as colunas fone da linha
                                phoneCols.forEach(idx => { row.getCell(idx).value = null; });

                                // Escreve como Number com formato '0'
                                finalPhones.slice(0, phoneCols.length).forEach((phone, index) => {
                                    const numericStr = String(phone).replace(/\D/g, '');
                                    if (numericStr) {
                                        const cell = row.getCell(phoneCols[index]);
                                        cell.value = Number(numericStr);
                                        cell.numFmt = '0';
                                    }
                                });
                            }
                        } else {
                            if (cnpj) notFoundInFile++;
                        }

                        // Marca status em TODA linha (independente de ter encontrado ou não)
                        row.getCell(statusCol).value = rowWasEnriched ? 'Enriquecido' : 'Pobre';
                        if (rowWasEnriched) enrichedInFile++;
                    }

                    const processedRowsInFile = endIndex - 1;
                    const elapsed = Date.now() - startTime;
                    const eta = formatEta((totalRows - processedRowsInFile) / (processedRowsInFile / elapsed) * 1000);
                    onProgress(id, Math.round((processedRowsInFile / totalRows) * 100), eta);
                }

                // Salva o arquivo sobrescrevendo o original
                await workbook.xlsx.writeFile(filePath);
                onProgress(id, 100, '00:00');
                log(`✅ ${path.basename(filePath)} concluído! Enriquecidos: ${enrichedInFile}. Não encontrados: ${notFoundInFile}.`);
                totalEnriched    += enrichedInFile;
                totalNotFound    += notFoundInFile;
                totalProcessed   += totalRows;

            } catch (err) {
                log(`❌ ERRO catastrófico em ${path.basename(filePath)}: ${err.message}`);
            }
        }
    } finally {
        log(`\n--- ✅ Enriquecimento Finalizado ---`);
        log(`Total Processadas: ${totalProcessed} | Enriquecidas: ${totalEnriched} | Não Encontradas: ${totalNotFound}`);
        onFinish();
    }
}
```

---

## 6. Download dos dados enriquecidos

Exporta toda a tabela `empresas` + `telefones` para um `.xlsx` com o formato:

| Coluna | Conteúdo |
|---|---|
| `cpf` | CNPJ da empresa (string) |
| `fone1` | Primeiro telefone |
| `fone2` | Segundo telefone |
| ... | ... |
| `foneN` | Enésimo telefone (N = máximo de telefones de qualquer empresa) |

Telefones são escritos como `Number` com formato `'0'`.

```javascript
async function downloadEnrichedData(pool, savePath) {
    const query = `
        SELECT e.cnpj, array_agg(t.numero ORDER BY t.id) AS telefones
        FROM empresas e
        LEFT JOIN telefones t ON e.id = t.empresa_id
        GROUP BY e.id, e.cnpj
        ORDER BY e.id;
    `;
    const { rows } = await pool.query(query);
    if (rows.length === 0) return { success: false, message: 'Nenhum dado encontrado.' };

    const maxPhones = rows.reduce((max, row) =>
        Math.max(max, row.telefones ? row.telefones.length : 0), 0
    );
    const headers = ['cpf', ...Array.from({ length: maxPhones }, (_, i) => `fone${i + 1}`)];

    const data = rows.map(row => {
        const phones = row.telefones || [];
        return [
            row.cnpj,
            ...Array.from({ length: maxPhones }, (_, i) => {
                const phone = phones[i];
                if (!phone) return null;
                const num = Number(String(phone).replace(/\D/g, ''));
                return isNaN(num) ? phone : num;
            })
        ];
    });

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Dados Enriquecidos');
    worksheet.addRow(headers);
    worksheet.addRows(data);
    for (let i = 2; i <= maxPhones + 1; i++) {
        worksheet.getColumn(i).numFmt = '0';
    }
    await workbook.xlsx.writeFile(savePath);
    return { success: true, message: `Arquivo salvo: ${savePath}` };
}
```

---

## 7. Frontend — HTML

```html
<!-- ════════════ ABA ENRIQUECIMENTO ════════════ -->
<div id="enriquecimento-tab">

    <!-- Painel de estatísticas no topo -->
    <div class="top-info-panel">
        <div class="info-item">
            <div class="info-item-title">Total de CNPJs Enriquecidos</div>
            <div id="enrichedCnpjCount" class="info-item-value">Carregando...</div>
        </div>
        <button id="refreshCountBtn">Atualizar</button>
        <button id="downloadEnrichedDataBtn">Baixar Dados</button>
    </div>

    <div class="grid">

        <!-- ── Card: Carga no BD (Planilha Mestra) ── -->
        <!-- Este card pode ficar em uma seção "Mais Opções" / colapsável -->
        <div class="card" id="master-load-card">
            <h2>Carregar Planilha Mestra no BD</h2>
            <p>
                Execute esta etapa uma vez para popular o banco com CNPJs e telefones.
                O arquivo deve ter coluna <code>cnpj</code> ou <code>cpf</code> e colunas <code>fone*</code>.
            </p>

            <button id="selectMasterFilesBtn">Selecionar Planilhas Mestras</button>
            <div id="selectedMasterFiles" class="files"></div>

            <label for="master-file-year-input">Ano da Base (Obrigatório):</label>
            <input type="number" id="master-file-year-input" placeholder="Ex: 2024">

            <div id="dbLoadProgressContainer" style="display:none;">
                <div id="dbLoadProgressTitle">Carregando...</div>
                <div class="progress-bar-container">
                    <div class="progress-bar-fill" id="dbLoadProgressBarFill" style="width:0%"></div>
                </div>
                <span id="dbLoadProgressPercent">0%</span>
                <div id="dbLoadProgressText"></div>
                <div id="dbLoadProgressStats"></div>
            </div>

            <button id="startLoadToDbBtn">Iniciar Carga no BD</button>
        </div>

        <!-- ── Card: Enriquecer Planilhas ── -->
        <div class="card">
            <h2>Enriquecer Planilhas</h2>
            <p>Carregue listas com coluna CNPJ/CPF e colunas fone para adicionar telefones do BD.</p>

            <button id="selectEnrichFilesBtn">1. Selecionar Arquivos para Enriquecer</button>
            <div id="selectedEnrichFiles" class="files"></div>

            <label for="enrichment-year-input">Ano para Pesquisa no Banco (Obrigatório):</label>
            <input type="number" id="enrichment-year-input" placeholder="Ex: 2024">

            <label for="enrichment-batch-size-input">Tamanho do Lote:</label>
            <input type="number" id="enrichment-batch-size-input" value="2000" min="100" step="100">

            <!-- Toggle: 213 PADRÃO -->
            <label>
                <input type="checkbox" id="enrichment-padrao-checkbox">
                Usar "213 PADRÃO" (busca ano selecionado + registros com ano=1)
            </label>

            <!-- Toggle: Todo Banco — desabilita os inputs de ano quando marcado -->
            <label>
                <input type="checkbox" id="enrichment-all-db-checkbox">
                Usar "Todo Banco" (ignora filtro de ano)
            </label>

            <!-- Estratégia de preenchimento -->
            <fieldset>
                <legend>Estratégia de Preenchimento:</legend>
                <label><input type="radio" name="enrichStrategy" value="append"    checked> Anexar</label>
                <label><input type="radio" name="enrichStrategy" value="overwrite">         Sobrescrever</label>
                <label><input type="radio" name="enrichStrategy" value="ignore">            Ignorar</label>
            </fieldset>

            <!-- Backup (referencia o checkbox global da aba, adapte conforme seu sistema) -->
            <label>
                <input type="checkbox" id="backupCheckbox">
                Criar backup antes de sobrescrever
            </label>

            <button id="startEnrichmentBtn">2. Iniciar Enriquecimento</button>
        </div>

        <!-- ── Card: Logs e barra de progresso (largura total) ── -->
        <div class="card full-width">
            <h2>Logs e Progresso</h2>
            <!-- Barras de progresso por arquivo são injetadas aqui pelo JS -->
            <div id="enrichmentProgressContainer"></div>
            <div id="enrichmentLog" class="logs">Aguardando início...</div>
        </div>

    </div>
</div>
```

---

## 8. Frontend — JavaScript

```javascript
// ════════════ ENRIQUECIMENTO — JS do renderer ════════════

const enrichedCnpjCountSpan     = document.getElementById('enrichedCnpjCount');
const refreshCountBtn           = document.getElementById('refreshCountBtn');
const downloadEnrichedDataBtn   = document.getElementById('downloadEnrichedDataBtn');
const selectMasterFilesBtn      = document.getElementById('selectMasterFilesBtn');
const selectedMasterFilesDiv    = document.getElementById('selectedMasterFiles');
const startLoadToDbBtn          = document.getElementById('startLoadToDbBtn');
const dbLoadProgressContainer   = document.getElementById('dbLoadProgressContainer');
const dbLoadProgressTitle       = document.getElementById('dbLoadProgressTitle');
const dbLoadProgressBarFill     = document.getElementById('dbLoadProgressBarFill');
const dbLoadProgressPercent     = document.getElementById('dbLoadProgressPercent');
const dbLoadProgressText        = document.getElementById('dbLoadProgressText');
const dbLoadProgressStats       = document.getElementById('dbLoadProgressStats');
const selectEnrichFilesBtn      = document.getElementById('selectEnrichFilesBtn');
const selectedEnrichFilesDiv    = document.getElementById('selectedEnrichFiles');
const startEnrichmentBtn        = document.getElementById('startEnrichmentBtn');
const enrichmentLogDiv          = document.getElementById('enrichmentLog');
const enrichmentProgressContainer = document.getElementById('enrichmentProgressContainer');
const enrichmentAllDbCheckbox   = document.getElementById('enrichment-all-db-checkbox');
const enrichmentYearInput       = document.getElementById('enrichment-year-input');
const enrichmentPadraoCheckbox  = document.getElementById('enrichment-padrao-checkbox');

let enrichmentMasterFiles = [];
let enrichmentEnrichFiles = [];

// ── Log ──────────────────────────────────────────────────────────────────────
function appendEnrichmentLog(msg) {
    if (!enrichmentLogDiv) return;
    if (enrichmentLogDiv.textContent.trim() === 'Aguardando início...') {
        enrichmentLogDiv.innerHTML = '';
    }
    msg.split('\n').forEach(line => {
        const p = document.createElement('p');
        p.textContent = `> ${line.trim()}`;
        enrichmentLogDiv.appendChild(p);
    });
    enrichmentLogDiv.scrollTop = enrichmentLogDiv.scrollHeight;
}

// ── Contador de CNPJs enriquecidos ────────────────────────────────────────────
async function updateEnrichedCnpjCount() {
    if (!enrichedCnpjCountSpan) return;
    enrichedCnpjCountSpan.textContent = 'Carregando...';
    // ADAPTE: chame sua API ou IPC
    const count = await window.electronAPI.getEnrichedCnpjCount();
    enrichedCnpjCountSpan.textContent = count.toLocaleString('pt-BR');
}
refreshCountBtn?.addEventListener('click', updateEnrichedCnpjCount);

// ── Download dos dados enriquecidos ───────────────────────────────────────────
downloadEnrichedDataBtn?.addEventListener('click', async () => {
    downloadEnrichedDataBtn.disabled = true;
    downloadEnrichedDataBtn.textContent = 'Preparando download...';
    // ADAPTE: chame sua API ou IPC
    const result = await window.electronAPI.downloadEnrichedData();
    appendEnrichmentLog(result.success ? `✅ ${result.message}` : `❌ ${result.message}`);
    downloadEnrichedDataBtn.disabled = false;
    downloadEnrichedDataBtn.textContent = 'Baixar Dados';
});

// ── Selecionar planilhas mestras ──────────────────────────────────────────────
selectMasterFilesBtn?.addEventListener('click', async () => {
    // ADAPTE: abra um file picker nativo ou <input type="file">
    const files = await window.electronAPI.selectFile({ title: 'Selecione as Planilhas Mestras', multi: true });
    if (!files?.length) return;
    enrichmentMasterFiles = files;
    selectedMasterFilesDiv.innerHTML = '';
    files.forEach(file => {
        const div = document.createElement('div');
        div.textContent = file.split(/[\\/]/).pop();
        selectedMasterFilesDiv.appendChild(div);
    });
});

// ── Iniciar carga no BD ───────────────────────────────────────────────────────
startLoadToDbBtn?.addEventListener('click', () => {
    if (enrichmentMasterFiles.length === 0) {
        return appendEnrichmentLog('❌ ERRO: Selecione pelo menos uma planilha mestra.');
    }
    const year = document.getElementById('master-file-year-input').value;
    if (!year || isNaN(parseInt(year))) {
        return appendEnrichmentLog('❌ ERRO: Insira um ano válido para a base de dados.');
    }

    startLoadToDbBtn.disabled = true;
    dbLoadProgressContainer.style.display = 'block';
    dbLoadProgressBarFill.style.width = '0%';
    dbLoadProgressPercent.textContent = '0%';
    dbLoadProgressText.textContent = 'Iniciando...';
    appendEnrichmentLog(`Iniciando carga para o ano de ${year}...`);

    // ADAPTE: chame sua API ou IPC
    window.electronAPI.startDbLoad({ masterFiles: enrichmentMasterFiles, year: parseInt(year) });
});

// ── Selecionar arquivos para enriquecer ───────────────────────────────────────
selectEnrichFilesBtn?.addEventListener('click', async () => {
    const files = await window.electronAPI.selectFile({ title: 'Selecione Arquivos para Enriquecer', multi: true });
    if (!files?.length) return;

    enrichmentEnrichFiles = [];
    selectedEnrichFilesDiv.innerHTML = '';
    enrichmentProgressContainer.innerHTML = '';

    files.forEach(file => {
        const id = `enrich-${enrichmentEnrichFiles.length}`;
        enrichmentEnrichFiles.push({ path: file, id });
        appendEnrichmentLog(`Adicionado: ${file.split(/[\\/]/).pop()}`);

        // Exibe nome do arquivo
        const nameDiv = document.createElement('div');
        nameDiv.textContent = file.split(/[\\/]/).pop();
        selectedEnrichFilesDiv.appendChild(nameDiv);

        // Cria barra de progresso individual por arquivo
        enrichmentProgressContainer.innerHTML += `
            <div style="margin-bottom:15px;">
                <div style="display:flex; justify-content:space-between; margin-bottom:4px;">
                    <strong>${file.split(/[\\/]/).pop()}</strong>
                    <span id="eta-${id}" style="font-size:12px; opacity:0.6;"></span>
                </div>
                <div class="progress-bar-container">
                    <div class="progress-bar-fill" id="${id}" style="width:0%; height:5px; background:var(--accent-blue); border-radius:99px;"></div>
                </div>
            </div>
        `;
    });
});

// ── Toggle "Todo Banco" desabilita inputs de ano ──────────────────────────────
enrichmentAllDbCheckbox?.addEventListener('change', () => {
    const isChecked = enrichmentAllDbCheckbox.checked;
    enrichmentYearInput.disabled = isChecked;
    enrichmentPadraoCheckbox.disabled = isChecked;
    if (isChecked) {
        enrichmentYearInput.value = '';
        enrichmentPadraoCheckbox.checked = false;
    }
});

// ── Iniciar enriquecimento ────────────────────────────────────────────────────
startEnrichmentBtn?.addEventListener('click', async () => {
    if (enrichmentEnrichFiles.length === 0) {
        return appendEnrichmentLog('❌ ERRO: Selecione pelo menos um arquivo para enriquecer.');
    }

    const useAllDb  = enrichmentAllDbCheckbox.checked;
    const year      = enrichmentYearInput.value;
    const batchSize = parseInt(document.getElementById('enrichment-batch-size-input').value, 10);

    if (!useAllDb && (!year || isNaN(parseInt(year)) || year.length !== 4)) {
        return appendEnrichmentLog('❌ ERRO: Insira um ano válido de 4 dígitos para pesquisar no banco.');
    }

    // Aviso para lotes muito grandes (pode travar)
    if (batchSize >= 10000) {
        const confirmed = confirm(
            `Lote de ${batchSize.toLocaleString('pt-BR')} registros pode causar lentidão. Continuar?`
        );
        if (!confirmed) return appendEnrichmentLog('⚠️ Operação cancelada.');
    }

    startEnrichmentBtn.disabled = true;

    const strategy  = document.querySelector('input[name="enrichStrategy"]:checked').value;
    const backup    = document.getElementById('backupCheckbox').checked;
    const usePadrao = enrichmentPadraoCheckbox.checked;

    appendEnrichmentLog(`Iniciando enriquecimento — estratégia: ${strategy.toUpperCase()}`);

    // ADAPTE: chame sua API ou IPC
    window.electronAPI.startEnrichment({
        filesToEnrich: enrichmentEnrichFiles,
        strategy,
        backup,
        year:      parseInt(year) || null,
        batchSize: batchSize || null,
        usePadrao,
        useAllDb
    });
});

// ── Receber eventos do backend ────────────────────────────────────────────────
window.electronAPI.onEnrichmentLog((msg) => appendEnrichmentLog(msg));

// Atualiza barra de progresso individual de cada arquivo
window.electronAPI.onEnrichmentProgress(({ id, progress, eta }) => {
    const bar = document.getElementById(id);
    if (bar) bar.style.width = `${progress}%`;
    const etaEl = document.getElementById(`eta-${id}`);
    if (etaEl) etaEl.textContent = progress === 100 ? 'Concluído!' : (eta ? `ETA: ${eta}` : '');
});

// Atualiza barra de progresso da carga no BD
window.electronAPI.onDbLoadProgress(({ current, total, fileName, cnpjsProcessed }) => {
    const percent = Math.round((current / total) * 100);
    if (dbLoadProgressBarFill) dbLoadProgressBarFill.style.width = `${percent}%`;
    if (dbLoadProgressPercent) dbLoadProgressPercent.textContent = `${percent}%`;
    if (dbLoadProgressText)    dbLoadProgressText.textContent    = `Processando: ${fileName}`;
    if (dbLoadProgressStats)   dbLoadProgressStats.textContent   = `${cnpjsProcessed} CNPJs processados`;
});

window.electronAPI.onDbLoadFinished(() => {
    if (startLoadToDbBtn) startLoadToDbBtn.disabled = false;
    updateEnrichedCnpjCount();
    if (dbLoadProgressTitle) dbLoadProgressTitle.textContent = 'Carga Concluída!';
    if (dbLoadProgressBarFill) dbLoadProgressBarFill.style.width = '100%';
    if (dbLoadProgressPercent) dbLoadProgressPercent.textContent = '100%';
    if (dbLoadProgressText) dbLoadProgressText.textContent = 'Finalizado com sucesso';
    setTimeout(() => {
        if (dbLoadProgressContainer) dbLoadProgressContainer.style.display = 'none';
    }, 3000);
});

window.electronAPI.onEnrichmentFinished(() => {
    if (startEnrichmentBtn) startEnrichmentBtn.disabled = false;
});
```

---

## 9. Fluxo completo de uso

```
SETUP (uma única vez)
├── Executar o SQL da seção 3 para criar as 3 tabelas
└── Garantir que pg.Pool está conectado (DATABASE_URL no .env)

FASE 1 — CARGA (sempre que a base mestre for atualizada)
1. Exportar planilha mestre para .xlsx com colunas:
   cnpj | fone1 | fone2 | ... | cpf_socio_1 | nome_socio_1 | celular1_socio_1 | ...
2. Selecionar o(s) arquivo(s) no botão "Selecionar Planilhas Mestras"
3. Informar o ANO da base (ex: 2024)
4. Clicar em "Iniciar Carga no BD"
   └── Progresso mostrado na barra
   └── Resultado: CNPJs, telefones e sócios salvos no banco

FASE 2 — ENRIQUECIMENTO (a qualquer momento)
1. Ter uma planilha de leads com colunas: cnpj/cpf + fone1 + fone2 + ...
2. Selecionar o(s) arquivo(s) no botão "Selecionar Arquivos para Enriquecer"
3. Configurar:
   ├── Ano: mesmo ano usado na carga (ex: 2024)
   ├── "213 PADRÃO": marcar se quiser incluir registros com ano=1
   ├── "Todo Banco": marcar para ignorar filtro de ano
   ├── Estratégia: Anexar / Sobrescrever / Ignorar
   └── Backup: marcar para preservar o arquivo original
4. Clicar em "Iniciar Enriquecimento"
   └── Cada arquivo ganha sua própria barra de progresso com ETA
   └── Resultado: arquivos sobrescritos com colunas fone preenchidas
                  + coluna "status" com "Enriquecido" ou "Pobre"

CONFERÊNCIA
└── Coluna "status" = "Pobre" pode significar:
    ├── CNPJ não existe no BD (nunca foi carregado)
    ├── CNPJ existe mas não tem telefone associado
    ├── CNPJ existe mas no ano errado (usar "Todo Banco" para confirmar)
    └── A estratégia "ignore" ou "append" considerou que a linha já estava completa
```

---

> **Nota sobre adaptação para sistemas não-Electron:**
> Substitua `window.electronAPI.*` por chamadas `fetch` à sua API REST:
> - `startDbLoad({ masterFiles, year })` → `POST /api/enrichment/load` (upload de arquivo + campo year)
> - `startEnrichment({ filesToEnrich, strategy, ... })` → `POST /api/enrichment/run` (upload + opções)
> - `getEnrichedCnpjCount()` → `GET /api/enrichment/count`
> - `downloadEnrichedData()` → `GET /api/enrichment/download` (retorna arquivo)
> - Progresso em tempo real → WebSocket ou Server-Sent Events (SSE)
