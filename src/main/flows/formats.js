'use strict';

const formats = require('./formats.json');
const { normalizarDocumento, normalizarTelefone } = require('../limpezaTelefones');
const { phoneSlot } = require('./layouts');

const SITUACAO_COLUMNS = Object.freeze([
    { header: 'situacao_cadastral_cod', campo: 'situacao_cadastral_cod' },
    { header: 'situacao_cadastral', campo: 'situacao_cadastral' },
    { header: 'situacao_cadastral_data', campo: 'situacao_cadastral_data' },
    { header: 'situacao_motivo', campo: 'situacao_motivo' }
]);
const OPERATION_LABELS = Object.freeze({ c6: 'C6', santander: 'SANTANDER', pagbank: 'PAGBANK', mercadopago: 'MP HUNTER' });
const text = value => value == null ? '' : String(value);
const digits = value => text(value).replace(/\D/g, '');
const clone = value => JSON.parse(JSON.stringify(value));

function listFormats(custom = []) {
    return [...Object.entries(formats).map(([id, format]) => ({ id, nome: format.nome, colunas: clone(format.colunas) })), ...clone(custom)];
}

function getFormat(id = 'padrao', context = {}, custom = []) {
    const source = typeof id === 'string' && Object.hasOwn(formats, id) ? formats[id] : custom.find(item => item.id === id);
    if (!source) throw new Error(`Formato de saída desconhecido: ${text(id)}.`);
    const format = { id, ...clone(source) };
    if (context.fillCpf && !format.colunas.some(column => text(column.header).trim().toLowerCase() === 'livre6')) {
        format.colunas.push({ header: 'livre6', campo: 'cpf_socio' });
    }
    if (context.includeSituacao !== false) {
        for (const column of SITUACAO_COLUMNS) {
            if (!format.colunas.some(existing => existing.campo === column.campo)) {
                let header = column.header, suffix = 2;
                while (format.colunas.some(existing => text(existing.header).trim().toLowerCase() === header.toLowerCase())) header = `${column.header}_${suffix++}`;
                format.colunas.push({ ...column, header });
            }
        }
    }
    return format;
}

function displayDate(value) {
    if (value == null || value === '') return '';
    if (value instanceof Date) return Number.isFinite(value.getTime()) ? displayDate(value.toISOString().slice(0, 10)) : '';
    const string = text(value);
    const iso = string.match(/^(\d{4})-(\d{2})-(\d{2})(?:$|[T\s])/);
    return iso ? `${iso[3]}/${iso[2]}/${iso[1]}` : string;
}

function cleanName(value) {
    return text(value).trim().replace(/\s+(?:\d{11}|\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2})$/, '').trim();
}

function outputPhones(record) {
    // A cleaned/enriched array is authoritative, including an explicitly empty array.
    // Legacy manual slots must never hide contacts added during enrichment.
    if (Array.isArray(record.phones)) return record.phones.map(text);
    return [record.telefone_principal, record.telefone_secundario].map(value => normalizarTelefone(value).phone).filter(Boolean);
}

function resolveField(name, record, context, phones) {
    if (name === 'fixo_vazio') return '';
    if (name === 'fixo_c6') return OPERATION_LABELS[context.operation || 'c6'] || text(context.operation);
    if (name === 'fixo_olos') return 'OLOS';
    if (name === 'fixo_flex') return 'FLEX';
    if (name === 'razao_social') return cleanName(record.razao_social);
    if (name === 'cnpj' || name === 'cnpj_numerico') return normalizarDocumento(record.cnpj, 'cnpj');
    if (name === 'cpf_socio') {
        const value = text(record.livre6 || record.cpf_socio);
        // Receita may return redacted identifiers. Removing mask characters and
        // padding the remaining digits would manufacture a different CPF.
        if (/[^\d.\-\s/]/u.test(value)) return value;
        return value ? normalizarDocumento(value, 'cpf') : '';
    }
    if (name === 'atividade_principal_cod_num') return digits(record.atividade_principal_cod);
    if (name === 'telefone_principal' || name === 'telefone_principal_num') return phones[0] || '';
    if (name === 'telefone_secundario' || name === 'telefone_secundario_num') return phones[1] || '';
    if (name === 'data_abertura' || name === 'situacao_cadastral_data') return displayDate(record[name]);
    if (name === 'ano_abertura') {
        const date = displayDate(record.data_abertura);
        return /^\d{2}\/\d{2}\/\d{4}$/.test(date) ? date.slice(-4) : '';
    }
    if (name === 'situacao_cadastral_cod') return record[name] ? text(record[name]).padStart(2, '0') : '';
    return text(record[name]);
}

function mapOutputRow(record, format, context = {}) {
    if (!record || !format || !Array.isArray(format.colunas)) throw new Error('Registro ou formato de saída inválido.');
    const phones = outputPhones(record);
    return format.colunas.map(column => {
        const slot = phoneSlot(column);
        if (slot) return phones[slot - 1] || '';
        if (text(column.header).trim().toLowerCase() === 'livre5' && context.fillLivre5) {
            const name = text(context.jobName).replace(/\.(?:xlsx|csv|xls)$/i, '');
            return [name, displayDate(context.date)].filter(Boolean).join(' | ');
        }
        if (column.campo === 'manual') return text(column.valor_manual);
        if (column.campo === 'composto') {
            return (column.partes || []).map(name => resolveField(name, record, context, phones).trim()).filter(Boolean).join(column.sep ?? ' - ');
        }
        return resolveField(column.campo, record, context, phones);
    });
}

module.exports = { listFormats, getFormat, mapOutputRow };
