// Tests only the migration macro, using a disposable registry key and fake archive.
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const assert = require('assert/strict');
const { execFileSync } = require('child_process');
const id = `CodexMigrationSmoke-${crypto.randomUUID()}`;
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nsis-migration-'));
const backup = path.join(process.env.APPDATA, id);
const source = path.join(root, 'old');
const compiler = path.join(process.env.LOCALAPPDATA, 'electron-builder/Cache/nsis/nsis-3.0.4.1/makensis.exe');
const nsisInclude = path.resolve('node_modules/app-builder-lib/templates/nsis/include');
const plugins = path.join(process.env.LOCALAPPDATA, 'electron-builder/Cache/nsis/nsis-resources-3.4.1/plugins/x86-unicode');
try {
    fs.mkdirSync(path.join(source, 'resources'), { recursive: true });
    fs.writeFileSync(path.join(source, 'resources/app.asar'), 'fake-old-config');
    const executable = path.join(root, 'migration-test.exe');
    const script = `Unicode true
RequestExecutionLevel user
SilentInstall silent
OutFile "${executable}"
!include "LogicLib.nsh"
!addincludedir "${nsisInclude}"
!addplugindir /x86-unicode "${plugins}"
!include "UAC.nsh"
!define INSTALL_REGISTRY_KEY "Software\\${id}"
!define PRIVATE_CONFIG_FOLDER "${id}"
Var installMode
!include "${path.resolve('scripts/installer-private-config.nsh')}"
Section
  WriteRegStr HKCU "\${INSTALL_REGISTRY_KEY}" InstallLocation "${source}"
  StrCpy $installMode CurrentUser
  !insertmacro customInit
  DeleteRegKey HKCU "\${INSTALL_REGISTRY_KEY}"
SectionEnd
`;
    const scriptPath = path.join(root, 'test.nsi');
    fs.writeFileSync(scriptPath, script);
    execFileSync(compiler, ['/V2', scriptPath], { windowsHide: true, stdio: 'pipe' });
    execFileSync(executable, ['/S'], { windowsHide: true, stdio: 'pipe' });
    assert.equal(fs.readFileSync(path.join(backup, 'legacy-app.asar'), 'utf8'), 'fake-old-config');
    fs.writeFileSync(path.join(source, 'resources/app.asar'), 'new-package-without-secrets');
    execFileSync(executable, ['/S'], { windowsHide: true, stdio: 'pipe' });
    assert.equal(fs.readFileSync(path.join(backup, 'legacy-app.asar'), 'utf8'), 'fake-old-config');
    console.log('Migração NSIS aprovada: preserva instalação anterior e não sobrescreve backup existente.');
} finally {
    // Both paths are unique test directories created above, never app data directories.
    fs.rmSync(root, { recursive: true, force: true });
    fs.rmSync(backup, { recursive: true, force: true });
    try { execFileSync('reg.exe', ['delete', `HKCU\\Software\\${id}`, '/f'], { windowsHide: true, stdio: 'ignore' }); } catch {}
}
