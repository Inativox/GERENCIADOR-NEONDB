const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

module.exports = function loadModule(filename, dependencies, globals = {}) {
    const absolute = path.resolve(__dirname, '../..', filename);
    const context = {
        module: { exports: {} },
        __dirname: path.dirname(absolute),
        console: { log() {}, warn() {}, error() {} },
        setTimeout: (fn) => { fn(); },
        process: { platform: 'win32', env: {} },
        require(name) {
            if (Object.hasOwn(dependencies, name)) return dependencies[name];
            throw new Error(`Dependência inesperada: ${name}`);
        },
        ...globals,
    };
    vm.runInNewContext(fs.readFileSync(absolute, 'utf8'), context, { filename: absolute });
    return context.module.exports;
};
