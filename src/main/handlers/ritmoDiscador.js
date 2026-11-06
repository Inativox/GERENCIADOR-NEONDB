/**
 * Handlers do Ritmo do Discador: ajusta a agressividade de cada serviço da Fastway pela ocupação dos operadores.
 * Lê a cada 30 s enquanto a automação está ativa neste PC ou a aba está aberta. Qualquer usuário vê;
 * só admin muda regras ou liga a automação. O login é o login Fastway salvo no Gerenciador.
 */
const { app, ipcMain } = require('electron');
const path = require('node:path');
const Store = require('electron-store');
const state = require('../state');
const logins = require('../runtimeFastwayLogins');
const { createFastwayFmi, FastwayErro, FastwayAuthErro } = require('../alertaMailing/fastwayFmi');
const { configDoAmbiente, baseUrl } = require('../alertaMailing/config');
const { MotorRitmo } = require('../ritmoDiscador/motor');
const { Diario } = require('../ritmoDiscador/diario');
const { validarRegra, LIMITE_PAUSA, TETO_PADRAO, TETO_CALLCENTER } = require('../ritmoDiscador/regras');

const INTERVALO_MS = 30 * 1000;
const CHAVE = 'ritmo_discador'; // por PC: só um PC deve ficar com a automação ativa
const store = new (require('../settingsWriteRetry')(Store))();
const validacao = message => Object.assign(new Error(message), { code: 'RITMO_VALIDACAO' });

let sessao = null; // { owner, credenciais, motor, ativo, assistindo, acordar, timer }

const ehAdmin = () => state.currentUser?.role === 'admin';
const mesmasCredenciais = (a, b) => Boolean(a && b && a.login === b.login && a.password === b.password);
const objeto = valor => (valor && typeof valor === 'object' && !Array.isArray(valor) ? valor : {});

// --- configuração deste PC ----------------------------------------------------------------------------
function config() {
    const salvo = objeto(store.get(CHAVE));
    const teto = Number.isInteger(salvo.teto) && salvo.teto > 0 ? salvo.teto : TETO_PADRAO;
    return { ativa: salvo.ativa === true, teto, regras: objeto(salvo.regras), pausados: objeto(salvo.pausados) };
}
function gravar(mudar) {
    const atual = config();
    mudar(atual);
    store.set(CHAVE, atual);
}
// A automação só ajusta com um admin logado: o login Fastway de outros perfis pode não ter essa permissão.
const armazem = {
    regras: () => config().regras,
    pausados: () => config().pausados,
    ativa: () => config().ativa && ehAdmin(),
    teto: () => config().teto, // soma máxima da agressividade das URAs em execução
    pausar: (id, info) => gravar(c => { c.pausados[id] = info; }),
    retomar: id => gravar(c => { delete c.pausados[id]; }),
};

// --- painel ---------------------------------------------------------------------------------------------
function dadosDoPainel() {
    const login = sessao ? logins.summary(sessao.owner) : { saved: false, login: null, canSave: false };
    const base = { login, admin: ehAdmin(), ativa: armazem.ativa(), teto: armazem.teto(), tetoCallcenter: TETO_CALLCENTER, soma: null, intervaloSegundos: INTERVALO_MS / 1000, limitePausa: LIMITE_PAUSA };
    if (!sessao?.motor) return { ...base, configurado: false, lidoEm: null, lendo: false, conectado: true, erro: null, servicos: [], eventos: [] };
    return { ...base, configurado: true, ...sessao.motor.painel() };
}

function enviarAoPainel() {
    const janela = state.mainWindow;
    if (janela && !janela.isDestroyed()) janela.webContents.send('ritmo-discador-update', dadosDoPainel());
}

// --- ciclo ----------------------------------------------------------------------------------------------
const precisaLer = atual => armazem.ativa() || atual.assistindo;

function esperar(atual, ms) {
    return new Promise(resolve => {
        if (ms != null) atual.timer = setTimeout(resolve, ms);
        atual.acordar = () => { clearTimeout(atual.timer); atual.acordar = null; resolve(); };
    });
}
const acordar = () => sessao?.acordar?.();

async function laco(atual) {
    while (atual.ativo && sessao === atual) {
        if (!mesmasCredenciais(logins.get(atual.owner), atual.credenciais)) { sincronizar(true); return; } // login trocado ou esquecido
        if (!precisaLer(atual)) { await esperar(atual, null); continue; } // ninguém olhando e automação desligada: dorme até alguém acordar
        try {
            await atual.motor.ler();
        } catch (erro) {
            console.error('erro inesperado no ciclo do ritmo do discador', erro);
        }
        if (!atual.ativo || sessao !== atual) return;
        enviarAoPainel();
        await esperar(atual, INTERVALO_MS);
    }
}

function iniciar(owner) {
    const credenciais = logins.get(owner);
    sessao = { owner, credenciais, motor: null, ativo: false, assistindo: false };
    if (!credenciais) return; // a aba pede o login Fastway
    const cfg = configDoAmbiente(credenciais);
    // Leituras a cada ~1,5 s e uma só repetição curta: a volta inteira precisa caber nos 30 s.
    const cliente = createFastwayFmi({ baseUrl: baseUrl(cfg), logon: credenciais.login, senha: credenciais.password, intervaloMinMs: 1500, esperasRetryMs: [5000] });
    const diario = new Diario(path.join(app.getPath('userData'), 'ritmo-discador', 'dia.json'));
    Object.assign(sessao, { ativo: true, motor: new MotorRitmo({ cliente, armazem, diario }) });
    console.log(`ritmo do discador iniciado para ${owner} (automação ${armazem.ativa() ? 'ATIVA' : 'desligada'} neste PC)`);
    void laco(sessao);
}

function parar() {
    if (!sessao) return;
    const atual = sessao;
    sessao = null;
    atual.ativo = false;
    atual.acordar?.();
}

/** Liga o ritmo para quem está logado (ou desliga), sem reiniciar o que já está rodando para a mesma pessoa. */
function sincronizar(forcar = false) {
    if (!state.currentUser) { parar(); return; }
    const owner = state.currentUser.username;
    if (!forcar && sessao?.owner === owner && (sessao.motor || !logins.get(owner))) return;
    const assistindo = sessao?.owner === owner && sessao.assistindo;
    parar();
    iniciar(owner);
    if (sessao) sessao.assistindo = assistindo;
    enviarAoPainel();
}

// --- login Fastway digitado na aba --------------------------------------------------------------------
async function salvarLogin(owner, input) {
    const login = typeof input?.login === 'string' ? input.login.trim() : '';
    const password = typeof input?.password === 'string' ? input.password : '';
    if (!login || !password || login.length > 200 || password.length > 300) throw validacao('Digite o login e a senha da Fastway.');
    if (!logins.summary(owner).canSave) throw validacao('A proteção do Windows não está disponível nesta máquina para salvar o login.');
    const cfg = configDoAmbiente({ login, password });
    const teste = createFastwayFmi({ baseUrl: baseUrl(cfg), logon: login, senha: password, esperasRetryMs: [] });
    try {
        await teste.listarServicos();
    } catch (erro) {
        if (erro instanceof FastwayAuthErro) throw validacao('A Fastway recusou esse login. Confira o usuário e a senha.');
        if (erro instanceof FastwayErro) throw validacao(`Não foi possível falar com a Fastway (${erro.message}). Tente de novo.`);
        throw erro;
    }
    logins.save(owner, { login, password });
    sincronizar(true);
    require('./alertaMailing').sincronizar(true); // o mesmo login serve o Alerta de Mailing
}

// --- ações de admin -------------------------------------------------------------------------------------
function motorAtivo() {
    if (!sessao?.motor) throw validacao('Salve o login Fastway para usar o Ritmo do Discador.');
    return sessao.motor;
}
const usuario = () => state.currentUser.username;

function ligarAutomacao(ligar) {
    const motor = motorAtivo();
    gravar(c => { c.ativa = ligar; });
    motor.esquecer();
    motor.registrar({ tipo: 'sistema', texto: ligar ? `Automação ligada neste PC por ${usuario()}: os ajustes valem na Fastway.` : `Automação desligada neste PC por ${usuario()}. A agressividade ficou como estava.` });
    console.warn(`ritmo do discador: automação ${ligar ? 'LIGADA' : 'desligada'} neste PC`);
    acordar();
}

function salvarRegra(input) {
    const motor = motorAtivo();
    const servicoId = input?.servicoId;
    if (!Number.isInteger(servicoId)) throw validacao('Serviço inválido.');
    const servico = motor.servicos.find(s => s.id === servicoId);
    const nome = servico?.nome || `serviço #${servicoId}`;
    if (input.regra == null) {
        gravar(c => { delete c.regras[servicoId]; delete c.pausados[servicoId]; });
        motor.registrar({ tipo: 'sistema', servicoId, servico: nome, texto: `Regra removida por ${usuario()}. A automação não mexe mais neste serviço.` });
    } else {
        const { regra, erro } = validarRegra(input.regra, servico?.limite ?? null, servico?.modo ?? '');
        if (erro) throw validacao(erro);
        gravar(c => { c.regras[servicoId] = regra; });
        motor.registrar({ tipo: 'sistema', servicoId, servico: nome,
            texto: `Regra salva por ${usuario()}: desce com ${regra.desce}% ou mais, sobe com ${regra.sobe}% ou menos, passo ${regra.passo}, entre ${regra.minimo} e ${regra.maximo}.` });
    }
    acordar(); // relê para a decisão já usar a regra nova
}

function salvarTeto(input) {
    const motor = motorAtivo();
    const valor = input?.valor;
    if (!Number.isInteger(valor) || valor < 1 || valor > 1000000) throw validacao('O teto precisa ser um número inteiro maior que zero.');
    const antes = config().teto;
    gravar(c => { c.teto = valor; });
    motor.registrar({ tipo: 'sistema', texto: `Teto geral mudado por ${usuario()} de ${antes} para ${valor}: a soma das URAs em execução não sobe acima disso.` });
    acordar();
}

function retomar(input) {
    const motor = motorAtivo();
    if (!Number.isInteger(input?.servicoId)) throw validacao('Serviço inválido.');
    const s = motor.retomar(input.servicoId);
    motor.registrar({ tipo: 'sistema', servicoId: input.servicoId, servico: s?.nome || '', texto: `Automação retomada por ${usuario()}${s ? ` a partir de ${s.agressividade}` : ''}.` });
    acordar();
}

// --- IPC ------------------------------------------------------------------------------------------------
function protect(action, { soAdmin = false } = {}) {
    return async (event, input) => {
        if (!state.currentUser || event.sender !== state.mainWindow?.webContents) return { success: false, message: 'Acesso negado. Faça o login.' };
        if (soAdmin && !ehAdmin()) return { success: false, message: 'Só admin altera o Ritmo do Discador.' };
        const owner = state.currentUser.username;
        try {
            sincronizar();
            await action(input, owner);
            if (state.currentUser?.username !== owner) return { success: false, message: 'A sessão mudou. Reabra a aba.' };
            return { success: true, painel: dadosDoPainel() };
        } catch (erro) {
            const conhecido = erro.code === 'RITMO_VALIDACAO' || erro instanceof FastwayErro;
            if (!conhecido) console.error('erro no ritmo do discador', erro);
            return { success: false, message: conhecido ? erro.message : 'Não foi possível concluir. Tente novamente.' };
        }
    };
}

function register() {
    ipcMain.handle('ritmo-discador-state', protect(() => {}));
    ipcMain.handle('ritmo-discador-assistir', protect(input => {
        if (!sessao) return;
        const antes = sessao.assistindo;
        sessao.assistindo = input?.ativo === true;
        if (sessao.assistindo && !antes) acordar(); // abriu a aba: lê na hora
    }));
    ipcMain.handle('ritmo-discador-ler', protect(() => { motorAtivo(); acordar(); }));
    ipcMain.handle('ritmo-discador-login', protect((input, owner) => salvarLogin(owner, input)));
    ipcMain.handle('ritmo-discador-ativa', protect(input => ligarAutomacao(input?.ligar === true), { soAdmin: true }));
    ipcMain.handle('ritmo-discador-regra', protect(salvarRegra, { soAdmin: true }));
    ipcMain.handle('ritmo-discador-retomar', protect(retomar, { soAdmin: true }));
    ipcMain.handle('ritmo-discador-teto', protect(salvarTeto, { soAdmin: true }));
    ipcMain.on('logout', parar);
    app.on('before-quit', parar);
}

module.exports = { register, sincronizar, parar };
