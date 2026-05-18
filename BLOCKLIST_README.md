# Sistema de Blocklist — Guia de Implementação

Este documento descreve como implementar o sistema completo de Blocklist em outro projeto Node.js.
Cobre: banco de dados, importação de números, verificação pontual, remoção de bloqueados de planilhas e interface web.

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Dependências](#2-dependências)
3. [Schema do Banco de Dados](#3-schema-do-banco-de-dados)
4. [Backend — Funções principais](#4-backend--funções-principais)
   - 4.1 [Importar arquivo para a blocklist](#41-importar-arquivo-para-a-blocklist)
   - 4.2 [Adicionar números manualmente](#42-adicionar-números-manualmente)
   - 4.3 [Verificar números pontuais](#43-verificar-números-pontuais)
   - 4.4 [Estatísticas](#44-estatísticas)
   - 4.5 [Remover bloqueados de uma planilha](#45-remover-bloqueados-de-uma-planilha)
   - 4.6 [Dividir CSV gigante](#46-dividir-csv-gigante)
5. [Frontend — HTML](#5-frontend--html)
6. [Frontend — JavaScript](#6-frontend--javascript)
7. [Variáveis de ambiente](#7-variáveis-de-ambiente)
8. [Fluxo completo de uso](#8-fluxo-completo-de-uso)

---

## 1. Visão Geral

O sistema de Blocklist funciona como uma lista negra de números de telefone armazenados em um banco PostgreSQL.

**Três operações principais:**

| Operação | Descrição |
|---|---|
| **Importar** | Lê arquivos `.xlsx` ou `.csv` e insere todos os telefones encontrados no BD |
| **Verificar** | Consulta se um ou mais números específicos estão na lista |
| **Filtrar planilha** | Remove de uma planilha todas as linhas cujos telefones estão bloqueados |

---

## 2. Dependências

```bash
npm install xlsx exceljs csv-parse pg nodemailer
```

| Pacote | Uso |
|---|---|
| `xlsx` | Leitura/escrita de planilhas `.xlsx` nas operações de limpeza |
| `exceljs` | Leitura em streaming de arquivos grandes na importação |
| `csv-parse` | Parsing de arquivos `.csv` na importação e divisão |
| `pg` | Cliente PostgreSQL (NeonDB ou qualquer Postgres) |
| `nodemailer` | E-mail de notificação ao concluir importação (opcional) |

---

## 3. Schema do Banco de Dados

Execute este SQL uma vez para criar a tabela:

```sql
CREATE TABLE IF NOT EXISTS blocklist (
    telefone    TEXT PRIMARY KEY,
    data_adicao TIMESTAMPTZ DEFAULT NOW()
);
```

- `telefone` — chave primária, somente dígitos (ex: `"21999998888"`). O `PRIMARY KEY` já garante unicidade e evita duplicatas automaticamente.
- `data_adicao` — preenchido automaticamente pelo banco ao inserir.

---

## 4. Backend — Funções principais

Todas as funções recebem um `pool` do pacote `pg`. Exemplo de como criar o pool:

```javascript
const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
```

---

### 4.1 Importar arquivo para a blocklist

Aceita `.xlsx` e `.csv`. Usa streaming para suportar arquivos gigantes (milhões de linhas).
Insere em lotes de 50.000 usando `ON CONFLICT DO NOTHING` — seguro para rodar múltiplas vezes.

```javascript
const path = require('path');
const fs = require('fs');
const ExcelJS = require('exceljs');
const { parse } = require('csv-parse');

async function feedBlocklist(pool, filePaths, log = console.log) {
    const DB_BATCH_SIZE = 50000;
    let totalNewPhonesAdded = 0;

    const processChunk = async (phoneChunk) => {
        if (phoneChunk.size === 0) return;
        const query = `
            INSERT INTO blocklist (telefone)
            SELECT unnest($1::text[])
            ON CONFLICT (telefone) DO NOTHING;
        `;
        const result = await pool.query(query, [Array.from(phoneChunk)]);
        if (result.rowCount > 0) {
            log(`✅ Lote salvo. ${result.rowCount} novos telefones adicionados.`);
            totalNewPhonesAdded += result.rowCount;
        }
    };

    for (const filePath of filePaths) {
        const fileName = path.basename(filePath);
        log(`\nProcessando: ${fileName}`);
        let phonesInBatch = new Set();
        let rowsProcessed = 0;
        const fileStream = fs.createReadStream(filePath);

        try {
            if (path.extname(filePath).toLowerCase() === '.csv') {
                // ── Leitura de CSV ────────────────────────────────────────
                const csvStream = fileStream.pipe(parse({ delimiter: [',', ';'], relax_column_count: true }));
                for await (const record of csvStream) {
                    record.forEach(value => {
                        const phone = String(value || '').replace(/\D/g, '').trim();
                        if (phone && phone.length >= 8) phonesInBatch.add(phone);
                    });
                    if (phonesInBatch.size >= DB_BATCH_SIZE) {
                        await processChunk(phonesInBatch);
                        phonesInBatch.clear();
                    }
                    rowsProcessed++;
                    if (rowsProcessed % 100000 === 0) log(`... ${rowsProcessed.toLocaleString('pt-BR')} linhas lidas...`);
                }
            } else {
                // ── Leitura de XLSX em streaming ──────────────────────────
                const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(fileStream);
                for await (const worksheetReader of workbookReader) {
                    for await (const row of worksheetReader) {
                        row.eachCell({ includeEmpty: true }, (cell) => {
                            const phone = cell.value ? String(cell.value).replace(/\D/g, '').trim() : null;
                            if (phone && phone.length >= 8) phonesInBatch.add(phone);
                        });
                        if (phonesInBatch.size >= DB_BATCH_SIZE) {
                            await processChunk(phonesInBatch);
                            phonesInBatch.clear();
                        }
                        rowsProcessed++;
                        if (rowsProcessed % 100000 === 0) log(`... ${rowsProcessed.toLocaleString('pt-BR')} linhas lidas...`);
                    }
                }
            }

            // Envia o lote restante
            if (phonesInBatch.size > 0) await processChunk(phonesInBatch);
            log(`✅ Finalizado: ${fileName}. ${rowsProcessed.toLocaleString('pt-BR')} linhas lidas.`);

        } catch (err) {
            log(`❌ Erro ao processar ${fileName}: ${err.message}`);
        }
    }

    log(`\nTotal de telefones novos adicionados: ${totalNewPhonesAdded.toLocaleString('pt-BR')}.`);

    const { rows } = await pool.query('SELECT COUNT(*) FROM blocklist;');
    log(`Total na blocklist agora: ${parseInt(rows[0].count, 10).toLocaleString('pt-BR')}.`);
}
```

**Pontos importantes:**
- O número é sempre normalizado com `.replace(/\D/g, '')` — remove parênteses, traços, espaços e DDI antes de salvar.
- A validação mínima é `length >= 8` para evitar lixo.
- O `Set` garante deduplicação dentro do mesmo arquivo antes de bater no BD.

---

### 4.2 Adicionar números manualmente

Insere um array de strings diretamente. Ideal para um formulário de textarea no frontend.

```javascript
async function addNumbersToBlocklist(pool, numbers) {
    // numbers: string[] — somente dígitos, ex: ['21999998888', '11988887777']
    if (!Array.isArray(numbers) || numbers.length === 0)
        return { success: false, message: 'Nenhum número fornecido.' };

    try {
        const query = `
            INSERT INTO blocklist (telefone)
            SELECT unnest($1::text[])
            ON CONFLICT (telefone) DO NOTHING;
        `;
        const result = await pool.query(query, [numbers]);
        return { success: true, added: result.rowCount, total: numbers.length };
    } catch (e) {
        return { success: false, message: e.message };
    }
}
```

**Retorno:**
```json
{ "success": true, "added": 3, "total": 5 }
// added = novos inseridos; total - added = duplicados ignorados
```

---

### 4.3 Verificar números pontuais

Retorna quais números estão ou não na blocklist, junto com a data em que foram adicionados.

```javascript
async function checkBlocklistNumbers(pool, numbers) {
    // numbers: string[] de dígitos limpos
    if (!numbers || numbers.length === 0)
        return { success: false, message: 'Nenhum número fornecido.' };

    try {
        const query = `
            SELECT
                telefone,
                to_char(data_adicao, 'DD/MM/YYYY HH24:MI:SS') AS data_formatada
            FROM blocklist
            WHERE telefone = ANY($1::text[])
        `;
        const result = await pool.query(query, [numbers]);

        const foundMap = new Map(result.rows.map(r => [r.telefone, r.data_formatada]));

        return {
            success: true,
            data: {
                // Números encontrados, com data de adição
                found: Array.from(foundMap.entries()).map(([telefone, data_adicao]) => ({ telefone, data_adicao })),
                // Números que NÃO estão na blocklist
                notFound: numbers.filter(n => !foundMap.has(n))
            }
        };
    } catch (e) {
        return { success: false, message: e.message };
    }
}
```

**Retorno:**
```json
{
  "success": true,
  "data": {
    "found": [
      { "telefone": "21999998888", "data_adicao": "28/04/2026 14:32:10" }
    ],
    "notFound": ["11988887777", "31977776666"]
  }
}
```

---

### 4.4 Estatísticas

Retorna o total de números na blocklist e quantos foram adicionados hoje.

```javascript
async function getBlocklistStats(pool) {
    try {
        const [totalResult, todayResult] = await Promise.all([
            pool.query('SELECT COUNT(*) FROM blocklist;'),
            pool.query("SELECT COUNT(*) FROM blocklist WHERE data_adicao >= current_date;")
        ]);
        return {
            success: true,
            data: {
                total:      parseInt(totalResult.rows[0].count, 10) || 0,
                addedToday: parseInt(todayResult.rows[0].count, 10) || 0,
            }
        };
    } catch (e) {
        return { success: false, message: e.message, data: { total: 0, addedToday: 0 } };
    }
}
```

---

### 4.5 Remover bloqueados de uma planilha

Esta é a função central do pipeline de limpeza de listas.

**Como funciona:**
1. Recebe as linhas da planilha já em memória (sem o cabeçalho).
2. Recebe os índices das colunas de telefone (`foneIdxs`).
3. Consulta o BD em lotes de 30.000 linhas para não estourar memória.
4. Devolve apenas as linhas cujos telefones **não** estão bloqueados.

```javascript
async function filterBlocklistedRows(rows, foneIdxs, pool, log = console.log) {
    // rows     — array de arrays (cada array = uma linha da planilha, sem cabeçalho)
    // foneIdxs — array de números: índices das colunas de telefone na linha
    //            ex: [7, 8, 9] se fone1=col7, fone2=col8, fone3=col9

    const BATCH_SIZE = 30000;
    const cleanRows = [];
    let removedCount = 0;

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);

        // 1. Coleta todos os telefones do lote em um Set (sem repetição)
        const phonesInBatch = new Set();
        batch.forEach(row => {
            foneIdxs.forEach(idx => {
                const v = row[idx] ? String(row[idx]).replace(/\D/g, '').trim() : '';
                if (v) phonesInBatch.add(v);
            });
        });

        // 2. Consulta quais desses telefones estão bloqueados
        const blocked = new Set();
        if (phonesInBatch.size > 0) {
            const { rows: dbRows } = await pool.query(
                'SELECT telefone FROM blocklist WHERE telefone = ANY($1::text[])',
                [Array.from(phonesInBatch)]
            );
            dbRows.forEach(r => blocked.add(r.telefone));
        }

        // 3. Filtra as linhas: remove qualquer linha que tenha ao menos um fone bloqueado
        for (const row of batch) {
            const isBlocked = foneIdxs.some(idx => {
                const v = row[idx] ? String(row[idx]).replace(/\D/g, '').trim() : '';
                return v && blocked.has(v);
            });
            if (isBlocked) { removedCount++; } else { cleanRows.push(row); }
        }

        log(`Blocklist: ${Math.min(i + BATCH_SIZE, rows.length)}/${rows.length} linhas verificadas...`);
        await new Promise(r => setImmediate(r)); // libera o event loop entre lotes
    }

    log(`Blocklist: ${removedCount} linhas removidas. Restam: ${cleanRows.length}.`);
    return cleanRows;
}
```

**Exemplo de uso completo com uma planilha:**

```javascript
const XLSX = require('xlsx');
const fs = require('fs').promises;

async function limparPlanilhaComBlocklist(filePath, pool) {
    // Lê a planilha
    const buffer = await fs.readFile(filePath);
    const wb = XLSX.read(buffer, { type: 'buffer' });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(sheet, { header: 1 });

    const header = data[0];
    const rows = data.slice(1);

    // Descobre os índices das colunas fone1..fone16
    const foneIdxs = header.reduce((acc, cell, i) => {
        if (typeof cell === 'string' && /^fone([1-9]|1[0-9])$/.test(cell.trim().toLowerCase()))
            acc.push(i);
        return acc;
    }, []);

    // Filtra as linhas bloqueadas
    const cleanRows = await filterBlocklistedRows(rows, foneIdxs, pool);

    // Salva o resultado no mesmo arquivo
    const newWB = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(newWB, XLSX.utils.aoa_to_sheet([header, ...cleanRows]), 'Dados');
    XLSX.writeFile(newWB, filePath);

    console.log(`Concluído. ${rows.length - cleanRows.length} linhas removidas.`);
}
```

---

### 4.6 Dividir CSV gigante

Útil para quebrar arquivos do tipo "Não Perturbe" antes de importar.

```javascript
const { parse } = require('csv-parse');

async function splitLargeCsv(filePath, linesPerSplit, log = console.log) {
    const inputStream = fs.createReadStream(filePath);
    const parser = parse({ delimiter: ',', from_line: 1 });
    const outputDir = path.dirname(filePath);
    const baseName = path.basename(filePath, '.csv');

    let fileCounter = 1;
    let lineCounter = 0;
    let rowsForCurrentFile = [];

    const saveChunk = async (rows, partNumber) => {
        if (rows.length === 0) return;
        const outputPath = path.join(outputDir, `${baseName}_parte_${partNumber}.csv`);
        log(`Gerando: ${path.basename(outputPath)} com ${rows.length.toLocaleString('pt-BR')} linhas...`);

        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Telefones');
        worksheet.columns = [{ header: 'telefone', key: 'telefone', width: 20 }];
        worksheet.addRows(
            rows
                .map(row => ({ telefone: String(row[1] || '').replace(/\D/g, '') }))
                .filter(r => r.telefone)
        );
        await workbook.csv.writeFile(outputPath, { formatterOptions: { delimiter: ';' } });
        log(`✅ Salvo: ${path.basename(outputPath)}`);
    };

    for await (const row of inputStream.pipe(parser)) {
        rowsForCurrentFile.push(row);
        lineCounter++;
        if (lineCounter % 100000 === 0) log(`... ${lineCounter.toLocaleString('pt-BR')} linhas processadas`);
        if (rowsForCurrentFile.length >= linesPerSplit) {
            await saveChunk(rowsForCurrentFile, fileCounter);
            rowsForCurrentFile = [];
            fileCounter++;
        }
    }
    if (rowsForCurrentFile.length > 0) await saveChunk(rowsForCurrentFile, fileCounter);

    log(`\nConcluído! ${lineCounter.toLocaleString('pt-BR')} linhas em ${fileCounter} arquivo(s).`);
}
```

---

## 5. Frontend — HTML

Estrutura completa da aba Blocklist. Adapte os nomes de classes ao seu design system.

```html
<!-- ════════════ ABA BLOCKLIST ════════════ -->
<div id="blocklist-tab">

    <!-- Painel de estatísticas no topo -->
    <div class="top-info-panel">
        <div class="info-item">
            <div class="info-item-title">Total na Blocklist</div>
            <div id="blocklist-total-count" class="info-item-value">Carregando...</div>
        </div>
        <div class="info-item">
            <div class="info-item-title">Adicionados Hoje</div>
            <div id="blocklist-today-count" class="info-item-value">Carregando...</div>
        </div>
        <button id="refreshBlocklistStatsBtn">Atualizar Estatísticas</button>
    </div>

    <div class="grid">

        <!-- ── Card 1: Verificar números ── -->
        <div class="card">
            <h2>Verificar Números na Blocklist</h2>
            <p>Insira números separados por vírgula, espaço ou quebra de linha.</p>
            <textarea
                id="check-numbers-input"
                placeholder="Ex: 21999998888, 11988887777..."
                rows="5"
            ></textarea>
            <button id="check-numbers-btn">Verificar Números</button>
            <!-- Resultados são injetados aqui pelo JS -->
            <div id="check-numbers-results">
                <p>Aguardando verificação...</p>
            </div>
        </div>

        <!-- ── Card 2: Ferramentas ── -->
        <div class="card">
            <h2>Ferramentas da Blocklist</h2>

            <!-- Seção: adicionar manualmente -->
            <section>
                <h3>Adicionar Manualmente</h3>
                <p>Cole números separados por vírgula, espaço ou linha.</p>
                <textarea
                    id="manual-blocklist-input"
                    rows="4"
                    placeholder="Ex:&#10;21999998888&#10;11988887777, 31977776666"
                ></textarea>
                <button id="addManualBlocklistBtn">Adicionar à Blocklist</button>
            </section>

            <hr>

            <!-- Seção: importar arquivo -->
            <section>
                <h3>Alimentar pelo Arquivo</h3>
                <p>Selecione arquivos .xlsx ou .csv com colunas de telefones.</p>
                <label>
                    <input type="checkbox" id="sendBlocklistEmailCheckbox">
                    Enviar e-mail de notificação ao concluir
                </label>
                <button id="feedBlocklistFromTabBtn">Selecionar Arquivos e Alimentar</button>
            </section>

            <hr>

            <!-- Seção: dividir CSV gigante -->
            <section>
                <h3>Dividir Arquivo CSV Gigante</h3>
                <p>Quebra CSVs muito grandes em partes menores antes de alimentar a blocklist.</p>
                <button id="selectCsvToSplitBtn">1. Selecionar Arquivo CSV</button>
                <div id="csvToSplitPath"></div>
                <label for="linesPerCsvSplit">Linhas por Arquivo:</label>
                <input type="number" id="linesPerCsvSplit" value="1000000">
                <button id="splitCsvBtn">2. Iniciar Divisão</button>
            </section>
        </div>

        <!-- ── Card 3: Logs (largura total) ── -->
        <div class="card full-width">
            <h2>Logs e Progresso</h2>
            <div id="blocklistLog">Aguardando início...</div>
        </div>

    </div>
</div>
```

---

## 6. Frontend — JavaScript

Cole este bloco após o DOM estar carregado (`DOMContentLoaded` ou no final do `<body>`).

```javascript
// ════════════ BLOCKLIST — JS do renderer ════════════

const blocklistLogDiv       = document.getElementById('blocklistLog');
const blocklistTotalCount   = document.getElementById('blocklist-total-count');
const blocklistTodayCount   = document.getElementById('blocklist-today-count');
const refreshStatsBtn       = document.getElementById('refreshBlocklistStatsBtn');
const feedBlocklistBtn      = document.getElementById('feedBlocklistFromTabBtn');
const manualInput           = document.getElementById('manual-blocklist-input');
const addManualBtn          = document.getElementById('addManualBlocklistBtn');
const checkNumbersInput     = document.getElementById('check-numbers-input');
const checkNumbersBtn       = document.getElementById('check-numbers-btn');
const checkNumbersResults   = document.getElementById('check-numbers-results');
const selectCsvToSplitBtn   = document.getElementById('selectCsvToSplitBtn');
const csvToSplitPathDiv     = document.getElementById('csvToSplitPath');
const linesPerCsvSplitInput = document.getElementById('linesPerCsvSplit');
const splitCsvBtn           = document.getElementById('splitCsvBtn');
let csvToSplitFile = null;

// ── Utilitário de log ────────────────────────────────────────────────────────
function appendBlocklistLog(msg) {
    if (!blocklistLogDiv) return;
    if (blocklistLogDiv.textContent.trim() === 'Aguardando início...') {
        blocklistLogDiv.innerHTML = '';
    }
    msg.split('\n').forEach(line => {
        const p = document.createElement('p');
        p.textContent = `> ${line.trim()}`;
        blocklistLogDiv.appendChild(p);
    });
    blocklistLogDiv.scrollTop = blocklistLogDiv.scrollHeight;
}

// ── Estatísticas ─────────────────────────────────────────────────────────────
async function updateBlocklistStats() {
    blocklistTotalCount.textContent = 'Carregando...';
    blocklistTodayCount.textContent = 'Carregando...';

    // ADAPTE: chame sua API ou IPC aqui
    const result = await window.electronAPI.getBlocklistStats();
    // result = { success: true, data: { total: 123456, addedToday: 42 } }

    if (result.success) {
        blocklistTotalCount.textContent = result.data.total.toLocaleString('pt-BR');
        blocklistTodayCount.textContent = result.data.addedToday.toLocaleString('pt-BR');
        appendBlocklistLog('Estatísticas atualizadas.');
    } else {
        blocklistTotalCount.textContent = 'Erro';
        blocklistTodayCount.textContent = 'Erro';
        appendBlocklistLog(`❌ Erro: ${result.message}`);
    }
}
refreshStatsBtn?.addEventListener('click', updateBlocklistStats);

// ── Alimentar pelo arquivo ────────────────────────────────────────────────────
feedBlocklistBtn?.addEventListener('click', async () => {
    // ADAPTE: abra um file picker nativo ou <input type="file">
    const files = await window.electronAPI.selectFile({ title: 'Selecione planilhas com telefones', multi: true });
    if (!files || files.length === 0) return appendBlocklistLog('Nenhum arquivo selecionado.');

    appendBlocklistLog(`Iniciando alimentação com ${files.length} arquivo(s).`);
    const sendEmail = document.getElementById('sendBlocklistEmailCheckbox').checked;

    // ADAPTE: chame sua API ou IPC
    window.electronAPI.feedBlocklist({ filePaths: files, sendEmail });
    // Os logs chegam via evento: window.electronAPI.onBlocklistLog(msg => appendBlocklistLog(msg))
});

// ── Adicionar manualmente ─────────────────────────────────────────────────────
addManualBtn?.addEventListener('click', async () => {
    const raw = manualInput?.value.trim() || '';
    if (!raw) return appendBlocklistLog('⚠️ Cole ao menos um número antes de adicionar.');

    // Divide por vírgula, ponto-e-vírgula, espaço ou quebra de linha
    // Remove não-dígitos e descarta números com menos de 8 dígitos
    const numbers = raw
        .split(/[,;\s\n]+/)
        .map(n => n.replace(/\D/g, ''))
        .filter(n => n.length >= 8);

    if (numbers.length === 0) return appendBlocklistLog('⚠️ Nenhum número válido encontrado.');

    appendBlocklistLog(`Adicionando ${numbers.length} número(s)...`);
    addManualBtn.disabled = true;

    // ADAPTE: chame sua API ou IPC
    const result = await window.electronAPI.addNumbersToBlocklist(numbers);
    // result = { success: true, added: 3, total: 5 }

    addManualBtn.disabled = false;
    if (result.success) {
        appendBlocklistLog(`✅ ${result.added} de ${result.total} adicionado(s) (duplicados ignorados).`);
        manualInput.value = '';
        updateBlocklistStats();
    } else {
        appendBlocklistLog(`❌ Erro: ${result.message}`);
    }
});

// ── Verificar números pontuais ────────────────────────────────────────────────
checkNumbersBtn?.addEventListener('click', async () => {
    const rawInput = checkNumbersInput.value.trim();
    if (!rawInput) return appendBlocklistLog('⚠️ Insira ao menos um número para verificar.');

    const numbers = rawInput
        .split(/[,;\s\n]+/)
        .map(n => n.replace(/\D/g, '').trim())
        .filter(n => n.length >= 8);

    if (numbers.length === 0) return appendBlocklistLog('⚠️ Nenhum número válido encontrado na entrada.');

    appendBlocklistLog(`Verificando ${numbers.length} número(s)...`);
    checkNumbersBtn.disabled = true;
    checkNumbersResults.innerHTML = '<p>Verificando...</p>';

    // ADAPTE: chame sua API ou IPC
    const result = await window.electronAPI.checkBlocklistNumbers(numbers);
    // result = { success: true, data: { found: [...], notFound: [...] } }

    if (result.success) {
        const { found, notFound } = result.data;
        let html = '';

        if (found.length > 0) {
            const items = found.map(item =>
                `<li>
                    <strong>${item.telefone}</strong>
                    <span style="font-size:11px; opacity:0.6;">(Adicionado em: ${item.data_adicao})</span>
                </li>`
            ).join('');
            html += `<h4>Encontrados na Blocklist (${found.length}):</h4><ul>${items}</ul>`;
        }

        if (notFound.length > 0) {
            html += `<h4>Não Encontrados (${notFound.length}):</h4>
                     <ul><li>${notFound.join('</li><li>')}</li></ul>`;
        }

        checkNumbersResults.innerHTML = html || '<p>Nenhum resultado para exibir.</p>';
        appendBlocklistLog(`Verificação concluída. Encontrados: ${found.length} | Não encontrados: ${notFound.length}.`);
    } else {
        checkNumbersResults.innerHTML = `<p>Erro: ${result.message}</p>`;
        appendBlocklistLog(`❌ Erro na verificação: ${result.message}`);
    }
    checkNumbersBtn.disabled = false;
});

// ── Dividir CSV gigante ───────────────────────────────────────────────────────
selectCsvToSplitBtn?.addEventListener('click', async () => {
    const files = await window.electronAPI.selectFile({
        title: 'Selecione o arquivo CSV para dividir',
        multi: false,
        filters: [{ name: 'CSV', extensions: ['csv'] }]
    });
    if (files?.length > 0) {
        csvToSplitFile = files[0];
        csvToSplitPathDiv.textContent = csvToSplitFile;
        appendBlocklistLog(`Arquivo selecionado: ${csvToSplitFile}`);
    }
});

splitCsvBtn?.addEventListener('click', () => {
    const linesPerSplit = parseInt(linesPerCsvSplitInput.value, 10);
    if (!csvToSplitFile) return appendBlocklistLog('❌ Selecione um arquivo CSV para dividir.');
    if (!linesPerSplit || linesPerSplit <= 0) return appendBlocklistLog('❌ Insira um número de linhas válido.');

    // ADAPTE: chame sua API ou IPC
    window.electronAPI.splitLargeCsv({ filePath: csvToSplitFile, linesPerSplit });
});

// ── Receber logs do backend via IPC (Electron) ────────────────────────────────
// Se não for Electron, substitua por WebSocket, SSE ou polling
window.electronAPI.onBlocklistLog((msg) => appendBlocklistLog(msg));
```

---

## 7. Variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto (nunca commite este arquivo):

```env
# PostgreSQL / NeonDB
DATABASE_URL=postgresql://usuario:senha@host/banco?sslmode=require

# SMTP para e-mail de notificação (opcional)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SMTP_USER=seu@email.com
SMTP_PASS=sua_senha_de_app
```

Carregue no início do processo principal:

```javascript
require('dotenv').config();
```

---

## 8. Fluxo completo de uso

```
1. SETUP
   └── Executar o SQL do item 3 para criar a tabela blocklist

2. IMPORTAR NÚMEROS (uma ou mais vezes)
   ├── Via arquivo: feedBlocklist(pool, ['caminho/arquivo.xlsx'])
   └── Via formulário: addNumbersToBlocklist(pool, ['21999998888', '11988887777'])

3. ANTES DE DISPARAR UMA CAMPANHA
   └── Para cada planilha de leads:
       a. Ler a planilha com XLSX.utils.sheet_to_json
       b. Descobrir os índices das colunas de telefone
       c. Chamar filterBlocklistedRows(rows, foneIdxs, pool)
       d. Salvar o resultado com XLSX.writeFile

4. VERIFICAÇÃO PONTUAL (opcional)
   └── checkBlocklistNumbers(pool, ['21999998888'])
       → retorna se o número está bloqueado e desde quando

5. MONITORAMENTO
   └── getBlocklistStats(pool)
       → total na lista + adicionados hoje
```

---

> **Nota sobre adaptação para sistemas não-Electron:**
> As chamadas `window.electronAPI.*` são a bridge IPC do Electron.
> Em um sistema web convencional, substitua cada uma por uma chamada `fetch` à sua API REST:
> - `getBlocklistStats()` → `GET /api/blocklist/stats`
> - `addNumbersToBlocklist(numbers)` → `POST /api/blocklist/add` com `{ numbers }`
> - `checkBlocklistNumbers(numbers)` → `POST /api/blocklist/check` com `{ numbers }`
> - `feedBlocklist(options)` → `POST /api/blocklist/feed` com upload de arquivo
> - Os logs em tempo real podem ser feitos via WebSocket ou Server-Sent Events (SSE).
