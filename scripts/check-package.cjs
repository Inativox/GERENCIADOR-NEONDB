const asar = require('@electron/asar');
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const execFile = promisify(require('child_process').execFile);

function checkPackage(archive) {
    const files = asar.listPackage(archive);
    const forbidden = files.filter(file => {
        const normalized = file.replace(/\\/g, '/');
        return /(^|\/)(\.env(?:\..*)?|users\.json|access\.json|chave-api\.mbkey)$|\.(mbkey|mbconfig|py)$/i.test(normalized);
    });
    if (forbidden.length) throw new Error(`Pacote contém arquivos privados: ${forbidden.join(', ')}`);
    // Check actual local credential values too, without ever printing them.
    const root = path.resolve(__dirname, '..');
    const secrets = [];
    const envPath = path.join(root, '.env');
    if (fs.existsSync(envPath)) secrets.push(...Object.entries(require('dotenv').parse(fs.readFileSync(envPath))).filter(([key, value]) => /PASS|SECRET|TOKEN|API_KEY/.test(key) && value.length >= 8).map(([, value]) => value));
    for (const file of files) {
        const name = file.replace(/^[/\\]+/, '');
        if (/^node_modules[/\\]/.test(name) || !/\.(js|cjs|json|html|css|yaml|yml|txt)$/i.test(name)) continue;
        const content = asar.extractFile(archive, name).toString('utf8');
        if (secrets.some(secret => content.includes(secret))) throw new Error(`Conteúdo privado detectado em ${name}; publicação interrompida.`);
    }
    console.log('Pacote verificado: sem arquivos privados ou credenciais locais detectadas.');
}

// afterPack runs before electron-builder uploads artifacts.
module.exports = async context => {
    const archive = path.join(context.appOutDir, 'resources', 'app.asar');
    checkPackage(archive);
    for (const script of ['smoke-renderer.cjs', 'smoke-private-config.cjs']) {
        const { stdout } = await execFile(require('electron'), [path.join(__dirname, script), archive], { windowsHide: true, timeout: 40000 });
        console.log(stdout.trim());
    }
};
module.exports.checkPackage = checkPackage;
if (require.main === module) checkPackage(path.resolve(process.argv[2] || 'dist/win-unpacked/resources/app.asar'));
