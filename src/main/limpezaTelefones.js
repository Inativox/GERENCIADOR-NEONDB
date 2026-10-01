const { normalizarDigitos, removerDdi } = require('./regrasLimpezaColunas');
const { cnpjText } = require('./documentos');

function digitsOf(value) {
    if (typeof value === 'number') return Number.isSafeInteger(value) && value >= 0 ? String(value) : '';
    if (typeof value === 'string' && /^\d+$/.test(value)) return value;
    return normalizarDigitos(value);
}

function normalizarDocumento(value, header = 'cnpj') {
    const alphanumeric = cnpjText(value);
    if (alphanumeric && /[A-Z]/.test(alphanumeric)) return alphanumeric;
    if (typeof value === 'string' && /[A-Z]/i.test(value) && !/^\s*\d+(?:[.,]\d+)?E[+-]?\d+\s*$/i.test(value)) return '';
    const digits = digitsOf(value);
    if (!digits) return '';
    const length = String(header).trim().toLowerCase() === 'cpf' && digits.length <= 11 ? 11 : 14;
    return digits.length <= length ? digits.padStart(length, '0') : digits;
}

function normalizarTelefone(value) {
    const digits = digitsOf(value);
    const phone = removerDdi(digits);
    // DDD + fixo/celular brasileiro. Nunca inventa dígitos faltantes.
    const dirty = ![10, 11].includes(phone.length) || /^(\d)\1+$/.test(phone) || /^(\d)\1+$/.test(phone.slice(2));
    return { phone: dirty ? '' : phone, ddiRemoved: digits !== phone };
}

function phoneIndices(header) {
    return header.map((value, index) => ({ index, match: String(value).trim().match(/^fone([1-9]\d*)$/i) }))
        .filter(column => column.match)
        .sort((a, b) => Number(a.match[1]) - Number(b.match[1]))
        .map(column => column.index);
}

function createCrossListContext() { return { cnpjs: new Set(), telefones: new Set() }; }

function cruzarECompactar(header, rows, indices, documentIndex, context) {
    const kept = [header];
    const newDocuments = new Set();
    const newPhones = new Set();
    const stats = { repeatedDocuments: 0, repeatedPhones: 0, withoutPhones: 0 };
    for (const row of rows) {
        const document = normalizarDocumento(row[documentIndex], header[documentIndex]);
        if (document && (context.cnpjs.has(document) || newDocuments.has(document))) {
            stats.repeatedDocuments++;
            continue;
        }
        const phones = [];
        for (const index of indices) {
            const phone = row[index] ? String(row[index]) : '';
            if (!phone) continue;
            if (context.telefones.has(phone) || newPhones.has(phone) || phones.includes(phone)) {
                stats.repeatedPhones++;
            } else {
                phones.push(phone);
            }
        }
        if (indices.length && !phones.length) {
            stats.withoutPhones++;
            continue;
        }
        indices.forEach((index, position) => { row[index] = position < phones.length ? Number(phones[position]) : null; });
        kept.push(row);
        if (document) newDocuments.add(document);
        phones.forEach(phone => newPhones.add(phone));
    }
    return { rows: kept, stats, newDocuments, newPhones };
}

function commitCrossList(context, result) {
    result.newDocuments.forEach(document => context.cnpjs.add(document));
    result.newPhones.forEach(phone => context.telefones.add(phone));
}

module.exports = { normalizarDocumento, normalizarTelefone, phoneIndices, createCrossListContext, cruzarECompactar, commitCrossList };
