const fs = require('fs');
const path = require('path');
const os = require('os');
const { promisify } = require('util');
const execFile = promisify(require('child_process').execFile);

function credentialPaths(keyFile, environment = process.env) {
    const explicit = keyFile || environment.ENG_DADOS_BQ_KEY || environment.OPERACAO_BQ_SA_KEY || environment.BQ_SA_KEY || environment.GOOGLE_APPLICATION_CREDENTIALS;
    const adc = environment.CLOUDSDK_CONFIG ? path.join(environment.CLOUDSDK_CONFIG, 'application_default_credentials.json') : process.platform === 'win32' ? path.join(environment.APPDATA || os.homedir(), 'gcloud', 'application_default_credentials.json') : path.join(os.homedir(), '.config/gcloud/application_default_credentials.json');
    return { source: explicit || (fs.existsSync(adc) ? adc : null), adc };
}
function loginModeFor(keyFile, environment = process.env) {
    const { source, adc } = credentialPaths(keyFile, environment);
    if (!source) return 'gcloud';
    const credential = JSON.parse(fs.readFileSync(source, 'utf8'));
    if (credential.type === 'service_account') return 'service_account';
    if (credential.type === 'authorized_user' && path.resolve(source) === path.resolve(adc)) return 'adc';
    return 'imported';
}
function authRequired(mode) {
    const renewable = ['gcloud', 'adc'].includes(mode);
    return Object.assign(new Error(renewable ? 'Seu login Google precisa ser renovado. Entre pelo navegador e depois retome a execução.' : 'A credencial BQ precisa ser substituída ou corrigida. Importe uma chave válida em Acessos às fontes.'), { code: renewable ? 'BQ_AUTH_REQUIRED' : 'BQ_CREDENTIAL_INVALID', loginMode: mode });
}
function gcloudCommand(environment = process.env, platform = process.platform) {
    if (platform !== 'win32') return 'gcloud';
    const roots = [environment['ProgramFiles(x86)'] || 'C:/Program Files (x86)', environment.LOCALAPPDATA, environment.ProgramFiles].filter(Boolean);
    return roots.map(root => path.join(root, 'Google/Cloud SDK/google-cloud-sdk/bin/gcloud.cmd')).find(filename => fs.existsSync(filename)) || null;
}
async function runGcloud(args, { commandRunner = execFile, timeout = 20000, signal, environment = process.env, platform = process.platform } = {}) {
    const command = gcloudCommand(environment, platform);
    if (!command || /["%!\r\n]/.test(command)) throw new Error('Instale o Google Cloud CLI nesta máquina para entrar com sua conta Google, ou importe uma chave BQ.');
    const options = { timeout, signal, windowsHide: true, maxBuffer: 256 * 1024 };
    // Only constant, internal arguments are accepted; no renderer input reaches a shell.
    if (args.some(value => !/^[a-zA-Z0-9=-]+$/.test(value))) throw new Error('Comando de acesso Google inválido.');
    if (platform === 'win32') return commandRunner(environment.ComSpec || 'cmd.exe', ['/d', '/s', '/c', `""${command}" ${args.join(' ')}"`], { ...options, windowsVerbatimArguments: true });
    return commandRunner(command, args, options);
}
function createBqLogin({ getMode, validate, onUpdate = () => {}, runner = runGcloud, now = Date.now } = {}) {
    let active = null;
    let lastAutomaticAttempt = -Infinity;
    let status = { state: 'idle', message: '' };
    const publish = (owner, state, message) => { status = { owner, state, message }; try { onUpdate({ ...status }); } catch { /* A closed window cannot interrupt login. */ } };
    return {
        status: owner => status.owner === owner ? { ...status } : { state: 'idle', message: '' },
        isBusy: () => Boolean(active),
        async renew(owner, { automatic = false } = {}) {
            if (active) {
                if (active.owner !== owner) throw new Error('Aguarde a renovação Google já iniciada nesta máquina.');
                return active.promise;
            }
            const mode = getMode();
            if (!['gcloud', 'adc'].includes(mode)) throw authRequired(mode);
            if (automatic && now() - lastAutomaticAttempt < 5 * 60000) throw authRequired(mode);
            if (automatic) lastAutomaticAttempt = now();
            const controller = new AbortController();
            const context = { owner, controller, promise: null };
            active = context;
            publish(owner, 'renewing', 'Entre na sua conta Google no navegador. Depois volte ao aplicativo.');
            context.promise = Promise.resolve().then(async () => {
                try {
                    const args = mode === 'adc' ? ['auth', 'application-default', 'login', '--launch-browser', '--disable-quota-project', '--quiet'] : ['auth', 'login', '--force', '--launch-browser', '--brief', '--quiet'];
                    await runner(args, { timeout: 5 * 60000, signal: controller.signal });
                    if (controller.signal.aborted) throw new Error();
                    await validate();
                    if (controller.signal.aborted) throw new Error();
                    const message = 'Login Google renovado e acesso BQ verificado. Retome a execução interrompida no histórico.';
                    publish(owner, 'ready', message);
                    return { message };
                } catch (error) {
                    const message = controller.signal.aborted ? 'Renovação Google cancelada.' : error.code === 'BQ_FORBIDDEN' ? error.message : 'Não foi possível concluir o login Google. Tente novamente em Acessos às fontes; confirme a conta e as permissões do BQ.';
                    publish(owner, 'failed', message);
                    throw new Error(message);
                } finally { if (active === context) active = null; }
            });
            return context.promise;
        },
        cancel() { active?.controller.abort(); },
    };
}
module.exports = { credentialPaths, loginModeFor, authRequired, gcloudCommand, runGcloud, createBqLogin };
