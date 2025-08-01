const ExcelJS = require('exceljs');
const axios = require('axios');
const path = require('path');

const CAMINHO_ARQUIVO = process.argv[2];
const COLUNA_CNPJ = 'cpf';
const COLUNA_RESPOSTA = 'C';

const CLIENT_ID = 'EA8ZUFeZVSeqMGr49XJSsZKFuxSZub3i';  //imWzrW41HcnoJgvZqHCaLvziUGlhAJAH
const CLIENT_SECRET = 'EUomxjGf6BvBZ1HO';  //A0lAqZO73uW3wryU

const TOKEN_URL = 'https://crm-leads-p.c6bank.info/querie-partner/token';
const CONSULTA_URL = 'https://crm-leads-p.c6bank.info/querie-partner/client/avaliable';

const BATCH_SIZE = 20000;
const RETRY_MS = 6 * 60 * 1000; // 6 minutos
const DELAY_SUCESSO_MS = 5 * 30 * 1000; // 5 minutos

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizeCnpj(cnpj) {
  const onlyDigits = cnpj.toString().replace(/\D/g, '');
  return onlyDigits.length < 14 ? onlyDigits.padStart(14, '0') : onlyDigits;
}

async function gerarToken() {
  const params = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET
  });

  const resp = await axios.post(TOKEN_URL, params.toString(), {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
  });

  return resp.data.access_token;
}

async function consultarLote(token, cnpjs) {
  const resp = await axios.post(
    CONSULTA_URL,
    { CNPJ: cnpjs },
    {
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json'
      }
    }
  );

  return resp.data;
}

async function main() {
  const workbook = new ExcelJS.Workbook();

  try {
    await workbook.xlsx.readFile(CAMINHO_ARQUIVO);
  } catch (err) {
    console.error('❌ Erro ao abrir arquivo Excel:', err.message);
    process.exit(1);
  }

  const worksheet = workbook.worksheets[0];

  const headers = worksheet.getRow(1).values;
  const idxCnpjCol = headers.findIndex(h => h === COLUNA_CNPJ);
  if (idxCnpjCol === -1) throw new Error(`Coluna "${COLUNA_CNPJ}" não encontrada.`);

  const registros = [];
  worksheet.eachRow({ includeEmpty: false }, (row, rowNum) => {
    if (rowNum === 1) return;
    const rawCnpj = row.getCell(idxCnpjCol).text || '';
    registros.push({ cnpj: normalizeCnpj(rawCnpj), rowNum });
  });

  const lotes = chunkArray(registros, BATCH_SIZE);

  for (let i = 0; i < lotes.length; i++) {
    const lote = lotes[i];
    console.log(`\n=== Lote ${i + 1}/${lotes.length} — ${lote.length} CNPJs ===`);

    let sucesso = false;

    while (!sucesso) {
      try {
        const token = await gerarToken();
        console.log('🔑 Token gerado com sucesso.');

        const cnpjArray = lote.map(r => r.cnpj);
        const resposta = await consultarLote(token, cnpjArray);
        console.log('✅ Lote consultado com sucesso.');

        const key = Object.keys(resposta).find(k => k.toLowerCase().includes('cnpj') && Array.isArray(resposta[k]));
        const encontrados = key ? new Set(resposta[key].map(normalizeCnpj)) : new Set();

        lote.forEach(({ cnpj, rowNum }) => {
          const valor = encontrados.has(cnpj) ? 'disponível' : 'cliente';
          worksheet.getCell(`${COLUNA_RESPOSTA}${rowNum}`).value = valor;
        });

        await workbook.xlsx.writeFile(CAMINHO_ARQUIVO);
        console.log('💾 Planilha salva com sucesso.');

        console.log(`⏳ Aguardando ${DELAY_SUCESSO_MS / 40000} minutos antes do próximo lote...`);
        await sleep(DELAY_SUCESSO_MS);

        sucesso = true;
      } catch (err) {
        const status = err.response?.status;
        const mensagem = err.response?.data || err.message || err;
        console.error(`❌ Erro ao consultar lote:`, mensagem);

        console.log(`⏳ Aguardando ${RETRY_MS / 60000} minutos para tentar novamente...`);
        await sleep(RETRY_MS);
      }
    }
  }

  console.log('\n🎉 Todos os lotes processados com sucesso!');
}

main().catch(err => {
  console.error('❌ Erro fatal:', err.message || err);
  process.exit(1);
});
