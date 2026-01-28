/**
 * PESCARIA → Salesforce (via Puppeteer) → Atualiza Postgres
 * - Busca apenas leads com status = 'Pendente'
 * - Telefone com DDI 55 (configurável por ADD_55)
 * - Trata o erro de telefone como erro comum (erro3, via XPath)
 * - Salva o texto exato no campo status_cadastro e classifica "status"
 * - Roda em loop: checa o banco a cada 10s
 */

require('dotenv').config();

const { Pool } = require('pg');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const axios = require('axios');
const readline = require('readline');
puppeteer.use(StealthPlugin());

// Sobrescreve console.log e console.error para adicionar timestamp
constSobrescreve = (() => {
  const originalLog = console.log;
  const originalError = console.error;

  const formatMessage = (args) => {
    const now = new Date();
    const time = `[${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}]`;
    return [time, ...args];
  };

  console.log = (...args) => originalLog.apply(console, formatMessage(args));
  console.error = (...args) => originalError.apply(console, formatMessage(args));
})();


// -------------------------- CONFIG -------------------------------------------
const CHECK_INTERVAL_MS = 5000;          // checar banco a cada 5s
const AFTER_SAVE_WAIT_MS = 5000;        // respiro após "Salvar"
const BATCH_LIMIT = 50;                  // pendentes por ciclo
const SALESFORCE_HOME =
  'https://c6bank.my.site.com/partners/s/lead/Lead/00B5A000008HZEOUA4';

// alternar teste com/sem 55 (pode vir do .env se quiser)
const ADD_55 = String(process.env.ADD_55 || 'false').toLowerCase() === 'true';

// -------------------------- DB POOL (SUPABASE/POSTGRES) ----------------------
const pool = new Pool(
  process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } }
    : {
      host: process.env.PGHOST,
      port: Number(process.env.PGPORT || 6543),
      user: process.env.PGUSER,
      password: process.env.PGPASSWORD,
      database: process.env.PGDATABASE || 'postgres',
      ssl:
        String(process.env.PGSSL || '').toLowerCase() === 'disable'
          ? false
          : { rejectUnauthorized: false },
    }
);

// ------------------------------- HELPERS -------------------------------------
const delay = (ms) => new Promise((r) => setTimeout(r, ms));
const digits = (s) => (s ?? '').toString().replace(/\D+/g, '');

// normalizador só para comparação (não altera o que gravamos)
// normalizador só para comparação (não altera o que gravamos)
const normalize = (s = '') =>
  s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/\s+/g, ' ').trim();

// ------------------------------ WEBHOOK --------------------------------------
async function enviarWebhook(cliente) {
  const webhookUrl = 'https://n8n-n8n.binjfz.easypanel.host/webhook/c7b5e53c-ff2a-4889-adfc-05d6b497d7e6';
  try {
    await axios.post(webhookUrl, cliente);
    console.log('✅ Webhook enviado com sucesso!');
  } catch (error) {
    console.error('❌ Erro ao enviar webhook:', error.message);
  }
}

// ------------------------------ REGRAS DE STATUS -----------------------------
const PHONE_MARKER_RAW = 'O formato correto para o telefone é DDI + DDD + telefone';
const PHONE_MARKER_NORM = normalize(PHONE_MARKER_RAW);

// toasts do Salesforce (erros/sucessos genéricos)
const TOAST_SELECTORS = [
  '.slds-notify_toast',
  '.forceToastMessage',
  '.slds-notify_toast .slds-text-heading_small',
  '.slds-notify_toast .toastContent',
  '.forceToastMessage .toastContent',
];

const EXISTS_MARKERS = [
  'Já existe um lead cadastrado com o CNPJ informado.',
  'Já existe um lead e um cliente cadastrado com o CNPJ informado.',
  'Já existe um cliente cadastrado com o CNPJ informado.',
].map(normalize);

const SUCCESS_MARKERS = [
  'Sucesso',
  'Foi identificada uma conta com o CNPJ informado nesse lead portanto ele será convertido. Clique no botão abaixo para abrir o registro da conta.',
].map(normalize);

// classificador padrão (NÃO altera o texto salvo)
function classificarStatusGenerico(msg) {
  const m = normalize(msg);
  if (EXISTS_MARKERS.some((mark) => m.includes(mark))) return 'Indisponivel para cadastro';
  if (m.includes(PHONE_MARKER_NORM)) return 'Disponivel para cadastro';
  if (SUCCESS_MARKERS.some((mark) => m.includes(mark))) return 'Cadastro salvo com sucesso!';
  return 'Indisponivel para cadastro';
}

// ------------------------------- DB QUERIES ----------------------------------
async function buscarLeads(limit = BATCH_LIMIT) {
  // Lê a ordem do .env (LEAD_ORDER=ASC ou LEAD_ORDER=DESC), com ASC como padrão.
  const order = (process.env.LEAD_ORDER || 'ASC').toUpperCase() === 'DESC' ? 'DESC' : 'ASC';

  let sql = `
    SELECT cnpj, name, email, phone
      FROM public.clients
     WHERE integration_status = 'Cadastrando...'
  `;

  // Adiciona a ordenação pela data de criação.
  sql += ` ORDER BY created_at ${order}`;
  sql += ' LIMIT $1';

  const { rows } = await pool.query(sql, [limit]);
  return rows;
}

async function atualizarLead(cnpj, statusMensagem) {
  const sql = `
    UPDATE public.clients
       SET integration_status = $1
     WHERE cnpj = $2
  `;
  await pool.query(sql, [statusMensagem, cnpj]);
}

// --------------------------- XPATHS DA TUA TELA ------------------------------
const XPATH = {
  novoLeadBtn:
    '/html/body/div[3]/div[2]/div/div[2]/div/div[1]/c-comercial-indicacao-conta-corrente/div/div/button',
  nome:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-body/div/slot/lightning-record-edit-form/lightning-record-edit-form-create/form/slot/slot/div/div[2]/lightning-input-field/lightning-input-name/fieldset/div/div/div[2]/lightning-input/lightning-primitive-input-simple/div[1]/div/input',
  sobrenome:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-body/div/slot/lightning-record-edit-form/lightning-record-edit-form-create/form/slot/slot/div/div[2]/lightning-input-field/lightning-input-name/fieldset/div/div/div[3]/lightning-input/lightning-primitive-input-simple/div[1]/div/input',
  email:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-body/div/slot/lightning-record-edit-form/lightning-record-edit-form-create/form/slot/slot/div/div[3]/lightning-input-field/lightning-input/lightning-primitive-input-simple/div[1]/div/input',
  telefone:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-body/div/slot/lightning-record-edit-form/lightning-record-edit-form-create/form/slot/slot/div/div[4]/lightning-input-field/lightning-input/lightning-primitive-input-simple/div[1]/div/input',
  razao:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-body/div/slot/lightning-record-edit-form/lightning-record-edit-form-create/form/slot/slot/div/div[5]/lightning-input-field/lightning-input/lightning-primitive-input-simple/div[1]/div/input',
  cnpj:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-body/div/slot/lightning-record-edit-form/lightning-record-edit-form-create/form/slot/slot/div/div[6]/lightning-input-field/lightning-input/lightning-primitive-input-simple/div[1]/div/input',
  salvar:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-footer/div/slot/div[1]/div/lightning-button/button',

  // feedback
  sucesso:
    '/html/body/div[3]/div[2]/div/div[2]/div[2]/div/div[2]/c-commercial-check-account-customer-lead/section/div/div[1]/p/text()',
  erro1:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-body/div/slot/lightning-record-edit-form/lightning-record-edit-form-create/form/slot/slot/div/div[1]/lightning-messages/div/div/div/p',
  erro2:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-body/div/slot/lightning-record-edit-form/lightning-record-edit-form-create/form/slot/slot/div/div[1]/lightning-messages/div/div/div/h2',

  // erro3 (telefone) — tratado como erro comum (obs: sem /html/body)
  erro3:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/div/lightning-modal/lightning-modal-body/div/slot/lightning-record-edit-form/lightning-record-edit-form-create/form/slot/slot/div/div[4]/lightning-input-field/lightning-input/lightning-primitive-input-simple/div[2]/span',

  fecharModal:
    '/html/body/lightning-overlay-container/div/lightning-modal-base/lightning-focus-trap/slot/section/div/lightning-button-icon/button',
  labelLead:
    '/html/body/div[3]/div[2]/div/div[1]/div/div[1]/div/records-lwc-highlights-panel/records-lwc-record-layout/forcegenerated-highlightspanel_lead___0125a000001ihmgqas___compact___view___recordlayout2/records-highlights2/div[1]/div[1]/div[1]/div[2]/h1/div/slot/records-entity-label',
};

// ------------------------------- PUPPETEER UTILS -----------------------------
async function xClick(page, xpath, timeout = 30_000) {
  await page.waitForFunction(
    (xp) =>
      document.evaluate(xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null)
        .singleNodeValue,
    { timeout },
    xpath
  );
  await page.evaluate((xp) => {
    const el = document.evaluate(xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null)
      .singleNodeValue;
    if (!el || typeof el.click !== 'function') throw new Error('Elemento não clicável: ' + xp);
    el.click();
  }, xpath);
}

async function xType(page, xpath, text, timeout = 30_000) {
  await page.waitForFunction(
    (xp) =>
      document.evaluate(xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null)
        .singleNodeValue,
    { timeout },
    xpath
  );
  await page.evaluate(
    async (xp, t) => {
      const el = document.evaluate(xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null)
        .singleNodeValue;
      if (!el) throw new Error('Elemento não encontrado: ' + xp);
      el.focus();
      el.value = '';
      for (const ch of String(t)) {
        el.value += ch;
        el.dispatchEvent(new Event('input', { bubbles: true }));
        await new Promise((r) => setTimeout(r, 3));
      }
      el.dispatchEvent(new Event('change', { bubbles: true }));
    },
    xpath,
    text
  );
}

async function xText(page, xpath) {
  return page.evaluate(
    (xp) =>
      document.evaluate(xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null)
        .singleNodeValue?.textContent?.trim() || '',
    xpath
  );
}

// tenta pegar texto por XPath "cru" e, se vazio, por '/html/body' + XPath
async function xTextFlex(page, rawXpath) {
  let t = await xText(page, rawXpath);
  if (t) return t;
  if (!/^\/html\b/i.test(rawXpath)) {
    const prefixed = '/html/body' + (rawXpath.startsWith('/') ? '' : '/') + rawXpath;
    t = await xText(page, prefixed);
    if (t) return t;
  }
  return '';
}

async function cssText(page, selector) {
  return page.evaluate((sel) => {
    const el = document.querySelector(sel);
    return el?.textContent?.trim() || '';
  }, selector);
}

// ------------------------------ TELEFONE 55 ----------------------------------
function escolherTelefone(lead) {
  let tel = digits(lead.phone);
  if (!tel) {
    const ddds = ['11', '21', '31', '41', '51', '61', '71', '81'];
    const ddd = ddds[Math.floor(Math.random() * ddds.length)];
    let num = '9';
    for (let i = 0; i < 8; i++) num += Math.floor(Math.random() * 10);
    tel = `${ADD_55 ? '55' : ''}${ddd}${num}`;
  } else {
    let only = tel.replace(/\D/g, '');
    if (ADD_55 && !only.startsWith('55')) only = '55' + only;
    tel = only;
  }
  return tel;
}

// ------------------------------ CAPTURA FEEDBACK -----------------------------
async function capturarMensagemDaTela(page, timeoutMs = 4000, intervalMs = 800) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    // 1) PRIORIDADE: erro de telefone (mais específico, via XPath)
    //    Usamos xTextFlex para tentar com e sem /html/body.
    let phoneErrorByXpath = await xTextFlex(page, XPATH.erro3);
    // Se encontrar QUALQUER texto no XPath do erro de telefone, assume que é o erro
    // de formato e retorna a mensagem completa e padronizada.
    if (phoneErrorByXpath) {
      // Força o retorno da mensagem completa para garantir a regra de negócio.
      return PHONE_MARKER_RAW;
    }

    // 2) erros padrão no topo do modal (mais genéricos)

    const e1 = await xText(page, XPATH.erro1);
    if (e1) return e1;

    const e2 = await xText(page, XPATH.erro2);
    if (e2) return e2;

    // 3) TOASTs (casos genéricos)
    const toastText = await page.evaluate((sels) => {
      const all = sels.flatMap((sel) => Array.from(document.querySelectorAll(sel)));
      const texts = all
        .map((el) => el.textContent?.trim() || '')
        .filter(Boolean)
        .map((t) => t.replace(/\s+/g, ' ').trim());
      return texts.sort((a, b) => b.length - a.length)[0] || '';
    }, TOAST_SELECTORS);
    if (toastText) return toastText;

    // 4) sucesso (último recurso "positivo")
    try {
      await page.waitForFunction(
        (xp) =>
          document.evaluate(xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null)
            .singleNodeValue,
        { timeout: 800 },
        XPATH.sucesso
      );
      const ok = await xText(page, XPATH.sucesso);
      if (ok) return ok;
    } catch (_) { }

    await delay(intervalMs);
  }
  return 'Resposta desconhecida após o envio.';
}

// --------------------------- GERENCIAMENTO DE SESSÕES ------------------------
async function iniciarNavegador() {
  const browser = await puppeteer.launch({
    headless: false,
    defaultViewport: null,
    channel: 'chrome',
    args: ['--start-maximized', '--no-sandbox', '--disable-setuid-sandbox'],
  });
  return browser;
}

async function logarNaConta(browser, user, pass, indice) {
  console.log(`[Conta ${indice}] Iniciando contexto e login para: ${user}`);
  
  // Cria contexto anônimo para isolar cookies de cada conta
  // Compatibilidade com Puppeteer v23+ (createIncognitoBrowserContext removido)
  const context = browser.createIncognitoBrowserContext
    ? await browser.createIncognitoBrowserContext()
    : await browser.createBrowserContext();
  const page = await context.newPage();

  // Otimização de CPU
  const session = await page.target().createCDPSession();
  await session.send('Emulation.setCPUThrottlingRate', { rate: 1 });
  await session.detach();

  const LOGIN_URL = 'https://c6bank.my.site.com/partners/s/login/';
  await page.goto(LOGIN_URL, { waitUntil: 'networkidle2' });

  try {
    const userXpath = '/html/body/div[3]/div[2]/div/div[2]/div/div[2]/div/div[1]/div/input';
    const passXpath = '/html/body/div[3]/div[2]/div/div[2]/div/div[2]/div/div[2]/div/input';

    await xType(page, userXpath, user);
    await xType(page, passXpath, pass);

    await page.keyboard.press('Enter');
    console.log(`[Conta ${indice}] Login submetido.`);
  } catch (e) {
    console.error(`[Conta ${indice}] Aviso no login:`, e.message);
  }

  return page;
}

async function configurarSessoes(browser) {
  const credenciais = [];
  let k = 1;
  while (process.env[`LOGINSALES${k}`]) {
    credenciais.push({
      u: process.env[`LOGINSALES${k}`],
      p: process.env[`SENHASALES${k}`]
    });
    k++;
  }

  if (credenciais.length === 0) {
    throw new Error('Nenhuma credencial (LOGINSALES...) encontrada no .env');
  }

  const sessoes = [];
  for (let i = 0; i < credenciais.length; i++) {
    const page = await logarNaConta(browser, credenciais[i].u, credenciais[i].p, i + 1);
    sessoes.push(page);
  }

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  await new Promise((resolve) => {
    console.log('\n>>> TODOS OS LOGINS SUBMETIDOS. Realize o MFA nas abas abertas.');
    console.log('>>> Digite "ok" e pressione ENTER para continuar...');
    rl.question('', () => {
      rl.close();
      resolve();
    });
  });

  console.log('Continuando... Acessando Salesforce Home em todas as abas.');
  for (const page of sessoes) {
    await page.goto(SALESFORCE_HOME, { waitUntil: 'domcontentloaded' });
  }
  return sessoes;
}

// --------------------------- PROCESSAR 1 LOTE --------------------------------
async function processarLote(sessoes, contadorSucesso, estadoGlobal) {
  const leads = await buscarLeads();
  if (!leads.length) {
    console.log('Nenhum lead Pendente. Dormindo 10s…');
    return contadorSucesso;
  }

  console.log(`Processando ${leads.length} pendente(s)…`);
  for (let i = 0; i < leads.length; i++) {
    // Seleciona a página de forma cíclica (Round Robin)
    const pageIndex = estadoGlobal.indiceConta % sessoes.length;
    const page = sessoes[pageIndex];
    const contaId = pageIndex + 1;
    estadoGlobal.indiceConta++; // Incrementa para o próximo lead usar a próxima conta

    const L = leads[i];
    const cpfStr = digits(String(L.cnpj));
    const nome = (L.name || '').trim();
    let email = (L.email || '').trim();
    if (!email) email = 'xxx@gmail.com';

    const telefone = escolherTelefone(L);

    // Mapeamento solicitado:
    // Nome -> name
    // Sobrenome -> cnpj
    // Razao -> name
    const nomeForm = nome;
    const sobrenomeForm = cpfStr;
    const razaoForm = nome;

    console.log(`[Conta ${contaId}] [${i + 1}/${leads.length}] ${nome} | ${cpfStr} | tel:${telefone}`);

    try {
      await page.goto(SALESFORCE_HOME, { waitUntil: 'domcontentloaded' });
      await delay(900);

      await xClick(page, XPATH.novoLeadBtn);
      await delay(700);

      await xType(page, XPATH.nome, nomeForm);
      await xType(page, XPATH.sobrenome, sobrenomeForm);
      await xType(page, XPATH.razao, razaoForm);
      await xType(page, XPATH.email, email);

      // telefone + blur/tab para disparar a validação do campo
      await xType(page, XPATH.telefone, telefone);
      await page.evaluate((xp) => {
        const el = document.evaluate(xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null)
          .singleNodeValue;
        if (el) el.blur();
      }, XPATH.telefone);
      await page.keyboard.press('Tab');

      await xType(page, XPATH.cnpj, cpfStr.padStart(14, '0'));

      await xClick(page, XPATH.salvar);
      await delay(AFTER_SAVE_WAIT_MS);

      // captura com polling (erro1 → erro2 → erro3 → toast → sucesso)
      const timeoutCaptura = ADD_55 ? 6000 : 2800;
      let statusMensagem = await capturarMensagemDaTela(page, timeoutCaptura);
      console.log('[capturado]', statusMensagem);


      // --- REGRA ESPECIAL: telefone inválido (pego em erro3 ou onde aparecer) ---
      if (statusMensagem === 'Resposta desconhecida após o envio.') {
        statusMensagem = 'Cadastro salvo com sucesso!';
        contadorSucesso++;
        await atualizarLead(cpfStr, statusMensagem);
        console.log(`[Validação Final] ${statusMensagem}`);
      } else if (normalize(statusMensagem).includes(PHONE_MARKER_NORM)) {
        await atualizarLead(cpfStr, statusMensagem);
        console.log('⚠️ telefone inválido → Disponivel para cadastro. Enviando Webhook...');

        // Dispara o webhook com os dados do cliente
        await enviarWebhook({
          nome: nome,
          cpf: cpfStr,
          email: email,
          telefone: telefone,
          status_mensagem: statusMensagem
        });

      } else {
        const novoStatus = classificarStatusGenerico(statusMensagem);
        await atualizarLead(cpfStr, statusMensagem);

        if (novoStatus !== 'Cadastro salvo com sucesso!') {
          await page
            .evaluate((xp) => {
              const el = document.evaluate(
                xp,
                document,
                null,
                XPathResult.FIRST_ORDERED_NODE_TYPE,
                null
              ).singleNodeValue;
              if (el && typeof el.click === 'function') el.click();
            }, XPATH.fecharModal)
            .catch(() => { });
          
          if (statusMensagem.includes('Ocorreu um erro ao tentar atualizar o registro')) {
            console.log('⚠️ Erro genérico Salesforce (ignorado)');
          } else {
            console.log('⚠️', statusMensagem, '→', novoStatus);
          }
        } else {
          contadorSucesso++;
          console.log(`✅ [${contadorSucesso}]`, statusMensagem);
        }
      }
    } catch (err) {
      const msg = `Erro de automação: ${err.message}`;
      console.error(`❌ [Conta ${contaId}]`, msg, err);
      await atualizarLead(cpfStr, msg);
      try { await page.goto(SALESFORCE_HOME, { waitUntil: 'domcontentloaded' }); } catch { }
    }
  }

  return contadorSucesso;
}

// ------------------------------- LOOP MAIN -----------------------------------
async function main() {
  console.log('Iniciando navegador...');
  const browser = await iniciarNavegador();
  console.log('Configurando sessões (contas)...');
  const sessoes = await configurarSessoes(browser);
  console.log(`Ok. ${sessoes.length} contas ativas. Rodando em loop.`);

  let contadorSucesso = 0;
  const estadoGlobal = { indiceConta: 0 }; // Mantém o índice da conta entre lotes

  let idleCycles = 0;
  const maxIdleCycles = 12; // 12 ciclos * 10s = 120s = 2 minutos

  for (; ;) {
    contadorSucesso = await processarLote(sessoes, contadorSucesso, estadoGlobal);
    const fezAlgo = contadorSucesso > 0;
    if (!fezAlgo) {
      idleCycles++;
      if (idleCycles >= maxIdleCycles) {
        console.log('[Sessão] Inatividade. Atualizando todas as páginas para manter sessão...');
        try {
          for (const p of sessoes) {
            await p.goto(SALESFORCE_HOME, { waitUntil: 'domcontentloaded' });
          }
          console.log('[Sessão] Todas as páginas atualizadas.');
          idleCycles = 0;
        } catch (err) {
          console.error('[Sessão] Erro ao atualizar páginas. Reiniciando processo...', err.message);
          process.exit(1); // O PM2 ou loop externo reiniciará o script
        }
      }
      await delay(CHECK_INTERVAL_MS);
    } else {
      idleCycles = 0;
    }
  }
}

process.on('unhandledRejection', (e) => console.error('UnhandledRejection:', e?.message || e));
process.on('uncaughtException', (e) => console.error('UncaughtException:', e?.message || e));

main().catch(async (e) => {
  console.error('Fatal:', e?.message || e);
  try { await pool.end(); } catch { }
});
