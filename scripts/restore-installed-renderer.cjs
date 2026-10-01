const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const source = path.join(root, 'recovery', 'installed-v1.8.0', 'renderer');
const destination = path.join(root, 'out', 'renderer');

fs.mkdirSync(destination, { recursive: true });
for (const name of ['react.js', 'react.css']) {
    fs.copyFileSync(path.join(source, name), path.join(destination, name));
}

console.log('Renderer do aplicativo instalado 1.8.0 restaurado.');
