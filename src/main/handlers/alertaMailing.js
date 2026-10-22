/**
 * Handlers do Alerta de Mailing (só admin): monitor em segundo plano, bandeja, notificações e o painel da aba.
 * O login é o login Fastway salvo no Gerenciador (o mesmo do Importar Mailing), nunca um .env.
 */
const { app, ipcMain, Notification, Tray, Menu, nativeImage } = require('electron');
const path = require('node:path');
const Store = require('electron-store');
const state = require('../state');
const logins = require('../runtimeFastwayLogins');
const { Monitor, snapshotParaDict } = require('../alertaMailing/monitor');
const { Estado } = require('../alertaMailing/estado');
const { createFastwayFmi, FastwayErro, FastwayAuthErro } = require('../alertaMailing/fastwayFmi');
const { configDoAmbiente, baseUrl } = require('../alertaMailing/config');
const { corDoSnapshot, resumo, desenharDisco } = require('../alertaMailing/bandeja');

const APP_ID = 'com.daviabraao.gerenciadordebases';
const CHAVE_RECICLADOR = 'alerta_mailing_reciclador'; // por PC: só um PC deve reciclar e parar por vez
const store = new (require('../settingsWriteRetry')(Store))();
const validacao = message => Object.assign(new Error(message), { code: 'ALERTA_VALIDACAO' });

let sessao = null; // { owner, credenciais, monitor, ativo, acordar, timer, bandeja, notificador, foco }
let saindo = false;

const recicladorLigado = () => store.get(CHAVE_RECICLADOR) === true;
const ehAdmin = () => state.currentUser?.role === 'admin';
const mesmasCredenciais = (a, b) => Boolean(a && b && a.login === b.login && a.password === b.password);

function imagemStatus(cor, tamanhos = [[16, 1], [32, 2], [48, 3]]) {
    const imagem = nativeImage.createEmpty();
    for (const [tamanho, escala] of tamanhos) imagem.addRepresentation({ scaleFactor: escala, width: tamanho, height: tamanho, buffer: desenharDisco(tamanho, cor) });
    return imagem;
}

/** Uma notificação por serviço, como as tags do alerta original: a nova substitui a anterior. */
function criarNotificador(aoClicar) {
    const ativas = new Map();
    const fechar = chave => { ativas.get(chave)?.close(); ativas.delete(chave); };
    return {
        notificar(titulo, texto, servicoId = null, nivel = 'ambar') {
            if (!Notification.isSupported()) return;
            const chave = servicoId ?? 'fastway';
            fechar(chave);
            const urgente = nivel === 'vermelho';
            const notificacao = new Notification({ title: titulo, body: texto, icon: imagemStatus(nivel, [[64, 1]]), urgency: urgente ? 'critical' : 'normal', timeoutType: urgente ? 'never' : 'default' });
            notificacao.on('click', () => aoClicar(servicoId));
            notificacao.on('close', () => { if (ativas.get(chave) === notificacao) ativas.delete(chave); });
            ativas.set(chave, notificacao);
            notificacao.show();
        },
        remover: servicoId => fechar(servicoId ?? 'fastway'),
        fecharTodas: () => [...ativas.keys()].forEach(fechar),
    };
}

// --- painel ------------------------------------------------------------------------------------------
function dadosDoPainel() {
    const resumoLogin = sessao ? logins.summary(sessao.owner) : { saved: false, login: null, canSave: false };
    const base = { login: resumoLogin, reciclador: recicladorLigado(), foco: sessao?.foco ?? null };
    if (sessao) sessao.foco = null;
    if (!sessao?.monitor) return { ...base, configurado: false, linhas: [], eventos: [], verificadoEm: null, conectado: true, erro: null };
    const { monitor } = sessao;
    return { ...base, configurado: true, ...snapshotParaDict(monitor.snapshot), verificando: monitor.verificando, intervaloMinutos: sessao.config.intervaloMinutos };
}

function enviarAoPainel() {
    const janela = state.mainWindow;
    if (janela && !janela.isDestroyed()) janela.webContents.send('alerta-mailing-update', dadosDoPainel());
}

function abrirPainel(servicoId = null) {
    const janela = state.mainWindow;
    if (!janela || janela.isDestroyed()) return;
    if (sessao) sessao.foco = servicoId;
    if (janela.isMinimized()) janela.restore();
    janela.show();
    janela.focus();
    janela.webContents.send('alerta-mailing-foco', { servicoId });
}

// --- bandeja -------------------------------------------------------------------------------------------
function alternarReciclador(ligar = !recicladorLigado()) {
    store.set(CHAVE_RECICLADOR, Boolean(ligar));
    console.warn(`reciclagem e parada automáticas ${ligar ? 'LIGADAS' : 'desligadas'} neste PC`);
    if (ligar) sessao?.notificador.notificar('Reciclagem e parada automáticas ligadas neste PC', 'Só um PC deve ficar com elas ligadas por vez. Desligue quando sair.', null, 'verde');
    else sessao?.notificador.notificar('Reciclagem e parada automáticas desligadas neste PC', 'Este PC volta a só receber alertas.', null, 'cinza');
    atualizarBandeja();
    verificarAgora(); // se alguma lista já está em 99%, recicla sem esperar o próximo ciclo
}

function menuDaBandeja() {
    return Menu.buildFromTemplate([
        { label: 'Abrir Alerta de Mailing', click: () => abrirPainel() },
        { label: 'Verificar agora', click: verificarAgora },
        { label: 'Reciclar e parar automaticamente neste PC', type: 'checkbox', checked: recicladorLigado(), click: item => alternarReciclador(item.checked) },
        { label: 'LTDA e PT2 (transbordo): sem reciclagem nem parada automática', enabled: false },
        { type: 'separator' },
        { label: 'Sair do Gerenciador', click: () => { saindo = true; app.quit(); } },
    ]);
}

function atualizarBandeja() {
    const bandeja = sessao?.bandeja, snapshot = sessao?.monitor?.snapshot;
    if (!bandeja || bandeja.isDestroyed() || !snapshot) return;
    bandeja.setImage(imagemStatus(corDoSnapshot(snapshot)));
    bandeja.setToolTip(resumo(snapshot));
    bandeja.setContextMenu(menuDaBandeja());
}

function criarBandeja() {
    const bandeja = new Tray(imagemStatus('cinza'));
    bandeja.setToolTip('Alerta de Mailing: iniciando…');
    bandeja.setContextMenu(menuDaBandeja());
    bandeja.on('click', () => abrirPainel());
    bandeja.on('double-click', () => abrirPainel());
    return bandeja;
}

// --- ciclo ----------------------------------------------------------------------------------------------
function verificarAgora() {
    sessao?.acordar?.();
}

function esperar(atual, ms) {
    return new Promise(resolve => {
        atual.timer = setTimeout(resolve, ms);
        atual.acordar = () => { clearTimeout(atual.timer); resolve(); };
    });
}

async function laco(atual) {
    while (atual.ativo && sessao === atual) {
        if (!mesmasCredenciais(logins.get(atual.owner), atual.credenciais)) { sincronizar(true); return; } // login trocado ou esquecido
        try {
            await atual.monitor.executarCiclo();
        } catch (erro) {
            console.error('erro inesperado no ciclo do alerta de mailing', erro);
        }
        if (!atual.ativo || sessao !== atual) return;
        atualizarBandeja();
        enviarAoPainel();
        await esperar(atual, atual.config.intervaloMinutos * 60 * 1000);
    }
}

function iniciar(owner) {
    const credenciais = logins.get(owner);
    sessao = { owner, credenciais, monitor: null, ativo: false, foco: null, notificador: criarNotificador(abrirPainel) };
    if (!credenciais) return; // a aba pede o login Fastway; sem ele não há monitor nem bandeja
    const config = configDoAmbiente(credenciais);
    const cliente = createFastwayFmi({ baseUrl: baseUrl(config), logon: credenciais.login, senha: credenciais.password });
    const estado = new Estado(path.join(app.getPath('userData'), 'alerta-mailing', 'estado.json'));
    Object.assign(sessao, { config, ativo: true, monitor: new Monitor({ cliente, estado, config, notificador: sessao.notificador, podeReciclar: recicladorLigado }) });
    sessao.bandeja = criarBandeja();
    // Abre com o Windows, já na bandeja, para o alerta não depender de alguém abrir o Gerenciador.
    if (app.isPackaged) app.setLoginItemSettings({ openAtLogin: true, args: ['--bandeja'] });
    console.log(`alerta de mailing iniciado para ${owner} (reciclagem automática ${recicladorLigado() ? 'LIGADA' : 'desligada'})`);
    void laco(sessao);
}

function parar() {
    if (!sessao) return;
    const atual = sessao;
    sessao = null;
    atual.ativo = false;
    atual.acordar?.();
    atual.notificador.fecharTodas();
    if (atual.bandeja && !atual.bandeja.isDestroyed()) atual.bandeja.destroy();
}

/** Liga o alerta para o admin logado (ou desliga), sem reiniciar o que já está rodando para a mesma pessoa. */
function sincronizar(forcar = false) {
    if (!ehAdmin()) { parar(); return; }
    const owner = state.currentUser.username;
    if (!forcar && sessao?.owner === owner && (sessao.monitor || !logins.get(owner))) return;
    parar();
    iniciar(owner);
    enviarAoPainel();
}

/** Com o monitor rodando, fechar a janela só esconde: o alerta segue na bandeja até "Sair do Gerenciador". */
const manterNaBandeja = () => Boolean(sessao?.monitor) && !saindo && ehAdmin();

// --- login Fastway digitado na aba --------------------------------------------------------------------
function credenciaisDigitadas(input) {
    const login = typeof input?.login === 'string' ? input.login.trim() : '';
    const password = typeof input?.password === 'string' ? input.password : '';
    if (!login || !password || login.length > 200 || password.length > 300) throw validacao('Digite o login e a senha da Fastway.');
    return { login, password };
}

/** Confere o login na API FMI antes de salvar: nunca guarda uma senha que a Fastway recusa. */
async function salvarLogin(owner, input) {
    const credenciais = credenciaisDigitadas(input);
    if (!logins.summary(owner).canSave) throw validacao('A proteção do Windows não está disponível nesta máquina para salvar o login.');
    const config = configDoAmbiente(credenciais);
    const teste = createFastwayFmi({ baseUrl: baseUrl(config), logon: credenciais.login, senha: credenciais.password, esperasRetryMs: [] });
    try {
        await teste.listarServicosAtivos();
    } catch (erro) {
        if (erro instanceof FastwayAuthErro) throw validacao('A Fastway recusou esse login. Confira o usuário e a senha.');
        if (erro instanceof FastwayErro) throw validacao(`Não foi possível falar com a Fastway (${erro.message}). Tente de novo.`);
        throw erro;
    }
    logins.save(owner, credenciais);
    sincronizar(true);
}

// --- IPC ------------------------------------------------------------------------------------------------
function protect(action) {
    return async (event, input) => {
        if (!ehAdmin() || event.sender !== state.mainWindow?.webContents) return { success: false, message: 'Acesso negado.' };
        const owner = state.currentUser.username;
        try {
            const result = await action(input, owner);
            if (state.currentUser?.username !== owner) return { success: false, message: 'A sessão mudou. Reabra o alerta.' };
            return { success: true, ...result };
        } catch (erro) {
            const conhecido = erro.code === 'ALERTA_VALIDACAO' || erro instanceof FastwayErro;
            return { success: false, message: conhecido ? erro.message : 'Não foi possível concluir. Tente novamente.' };
        }
    };
}

function monitorAtivo() {
    if (!sessao?.monitor) throw validacao('Salve o login Fastway para usar o alerta.');
    return sessao.monitor;
}

function register() {
    if (process.platform === 'win32') app.setAppUserModelId(app.isPackaged ? APP_ID : process.execPath); // notificações do Windows
    ipcMain.handle('alerta-mailing-state', protect(() => { sincronizar(); return { painel: dadosDoPainel() }; }));
    ipcMain.handle('alerta-mailing-verificar', protect(() => { monitorAtivo(); verificarAgora(); return { painel: dadosDoPainel() }; }));
    ipcMain.handle('alerta-mailing-dispensar', protect(input => {
        monitorAtivo().dispensar(typeof input?.eventoId === 'string' ? input.eventoId : null);
        atualizarBandeja();
        return { painel: dadosDoPainel() };
    }));
    ipcMain.handle('alerta-mailing-limpar-historico', protect(() => { monitorAtivo().limparHistorico(); return { painel: dadosDoPainel() }; }));
    ipcMain.handle('alerta-mailing-reciclador', protect(input => { monitorAtivo(); alternarReciclador(input?.ligar === true); return { painel: dadosDoPainel() }; }));
    ipcMain.handle('alerta-mailing-servico', protect(async input => {
        const monitor = monitorAtivo();
        if (!Number.isInteger(input?.servicoId)) throw validacao('Serviço inválido.');
        await (input.parar === true ? monitor.pararServico(input.servicoId) : monitor.iniciarServico(input.servicoId));
        verificarAgora(); // relê na hora para o painel mostrar a situação nova
        return { painel: dadosDoPainel() };
    }));
    ipcMain.handle('alerta-mailing-login', protect(async (input, owner) => { await salvarLogin(owner, input); return { painel: dadosDoPainel() }; }));
    ipcMain.on('logout', parar);
    app.on('before-quit', () => { saindo = true; parar(); });
}

module.exports = { register, sincronizar, manterNaBandeja, parar, abrirPainel };
