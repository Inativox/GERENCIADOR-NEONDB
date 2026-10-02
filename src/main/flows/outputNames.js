'use strict';

const invalidCharacters = /[<>:"/\\|?*\x00-\x1f]/;
const reservedName = /^(con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])(?:\.|$)/i;

function validateOutputName(value = '') {
    if (typeof value !== 'string' || value.length > 100) throw new Error('Use até 100 caracteres no nome dos arquivos.');
    const name = value.trim();
    if (name && (invalidCharacters.test(name) || /\.$/.test(name) || reservedName.test(name))) {
        throw new Error('Nome dos arquivos inválido. Não use caracteres como /, \\, :, * ou nomes reservados do Windows.');
    }
    if (/\.(xlsx|csv)$/i.test(name)) throw new Error('Informe o nome dos arquivos sem a extensão XLSX ou CSV.');
    return name;
}

function resolveOutputName(output, flowName) {
    const custom = validateOutputName(output?.fileName ?? '');
    if (custom) return custom;
    let name = String(flowName || 'Lista').replace(/[<>:"/\\|?*\x00-\x1f]/g, ' ').replace(/\s+/g, ' ').trim()
        .replace(/\.(xlsx|csv)$/i, '').slice(0, 100).replace(/[. ]+$/, '');
    if (!name) name = 'Lista';
    if (reservedName.test(name)) name = `Lista ${name}`.slice(0, 100);
    return name;
}

module.exports = { validateOutputName, resolveOutputName };
