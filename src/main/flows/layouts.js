'use strict';
const { randomUUID } = require('crypto');
const { FIELD_ALIASES } = require('./receita');

const LABELS = {
    razao_social: 'Razão social', cnpj: 'CNPJ', cnpj_numerico: 'CNPJ', nome_fantasia: 'Nome fantasia',
    data_abertura: 'Data de abertura', ano_abertura: 'Ano de abertura', email: 'E-mail',
    atividade_principal_cod: 'Código CNAE', atividade_principal_cod_num: 'Código CNAE', atividade_principal: 'Descrição CNAE',
    estado: 'UF', cidade: 'Cidade', bairro: 'Bairro', logradouro: 'Logradouro', numero: 'Número', complemento: 'Complemento', cep: 'CEP',
    nome_socio: 'Nome do sócio', cpf_socio: 'CPF do sócio', porte: 'Porte', capital_social: 'Capital social',
    natureza_juridica: 'Natureza jurídica', natureza_juridica_cod: 'Código da natureza jurídica',
    situacao_cadastral: 'Situação cadastral', situacao_cadastral_cod: 'Código da situação cadastral', situacao_cadastral_data: 'Data da situação cadastral', situacao_motivo: 'Motivo da situação cadastral',
    situacao_especial: 'Situação especial', opcao_mei: 'Opção MEI', municipio_ibge: 'Código IBGE', tipo: 'Matriz ou filial', pais: 'País', ultima_atualizacao: 'Última atualização',
    fixo_vazio: 'Coluna vazia', fixo_c6: 'Nome da operação do fluxo', fixo_olos: 'Texto OLOS', fixo_flex: 'Texto FLEX',
};
const PHONE_FIELDS = Array.from({ length: 10 }, (_, index) => ({ id: `telefone_${index + 1}`, label: `Telefone ${index + 1}` }));
const RAW_PHONE_FIELDS = ['telefone_principal', 'telefone_secundario', 'telefone_principal_num', 'telefone_secundario_num'];
function listLayoutFields() {
    return [...Object.keys(FIELD_ALIASES).filter(id => !RAW_PHONE_FIELDS.includes(id)).map(id => ({ id, label: LABELS[id] || id })),
        ...Object.keys(LABELS).filter(id => !Object.hasOwn(FIELD_ALIASES, id)).map(id => ({ id, label: LABELS[id] })),
        ...PHONE_FIELDS, { id: 'manual', label: 'Texto fixo' }, { id: 'composto', label: 'Combinar campos' }];
}
const ALLOWED = new Set([...listLayoutFields().map(field => field.id), ...RAW_PHONE_FIELDS]);
function headerPhoneSlot(header) { const match = String(header || '').trim().match(/^(?:fone|telefone\s*)([1-9]\d*)$/i); return match ? Number(match[1]) : 0; }
function fieldPhoneSlot(field) {
    const match = String(field || '').match(/^telefone_([1-9]\d*)$/);
    if (match) return Number(match[1]);
    if (['telefone_principal', 'telefone_principal_num'].includes(field)) return 1;
    if (['telefone_secundario', 'telefone_secundario_num'].includes(field)) return 2;
    return 0;
}
function phoneSlot(column) {
    return headerPhoneSlot(typeof column === 'string' ? column : column.header) || fieldPhoneSlot(column.campo);
}
function checkedText(value, label, max, empty = false) {
    if (typeof value !== 'string' || value.length > max || /[\u0000-\u001f\u007f]/u.test(value) || (!empty && !value.trim())) throw new Error(`${label}: informe um texto ${empty ? 'de até' : 'com 1 a'} ${max} caracteres, sem quebras de linha.`);
    return empty ? value : value.trim();
}
function validateLayout(input, existing) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('Layout inválido.');
    const nome = checkedText(input.nome, 'Nome do layout', 100);
    if (!Array.isArray(input.colunas) || !input.colunas.length || input.colunas.length > 60) throw new Error('O layout precisa de 1 a 60 colunas.');
    const headers = new Set(), slots = new Set();
    const colunas = input.colunas.map((column, index) => {
        if (!column || typeof column !== 'object' || Array.isArray(column)) throw new Error(`Coluna ${index + 1} inválida.`);
        const header = checkedText(column.header, `Nome da coluna ${index + 1}`, 80);
        const canonical = header.normalize('NFC').toLocaleLowerCase('pt-BR');
        if (headers.has(canonical)) throw new Error(`O nome de coluna "${header}" está repetido.`);
        headers.add(canonical);
        if (!ALLOWED.has(column.campo)) throw new Error(`Escolha uma origem válida para "${header}".`);
        const headerSlot = headerPhoneSlot(header), fieldSlot = fieldPhoneSlot(column.campo);
        // Built-in templates identify empty extra slots by header. Custom phone fields allow any header name.
        if (headerSlot && fieldSlot && headerSlot !== fieldSlot) throw new Error(`"${header}" deve usar Telefone ${headerSlot}.`);
        if (headerSlot && !fieldSlot && !['manual', 'fixo_vazio'].includes(column.campo)) throw new Error(`"${header}" é reservado para um telefone. Escolha uma origem de telefone ou renomeie a coluna.`);
        const slot = headerSlot || fieldSlot;
        if (slot) {
            if (slot > 10 || slots.has(slot)) throw new Error('Use cada Telefone de 1 a 10 uma única vez.');
            slots.add(slot);
            return { header, campo: `telefone_${slot}` };
        }
        if (column.campo === 'manual') return { header, campo: 'manual', valor_manual: checkedText(column.valor_manual ?? '', `Texto fixo de "${header}"`, 1000, true) };
        if (column.campo === 'composto') {
            if (!Array.isArray(column.partes) || column.partes.length < 2 || column.partes.length > 8 || column.partes.some(field => !ALLOWED.has(field) || ['manual', 'composto'].includes(field) || fieldPhoneSlot(field))) throw new Error(`"${header}": combine de 2 a 8 campos de dados, sem telefones.`);
            return { header, campo: 'composto', partes: [...column.partes], sep: checkedText(column.sep ?? ' - ', 'Separador', 20, true) };
        }
        return { header, campo: column.campo };
    });
    if (!slots.size || [...slots].some(slot => !slots.has(slot - 1) && slot !== 1)) throw new Error('Inclua Telefone 1 e mantenha a sequência: Telefone 1, Telefone 2, etc., sem lacunas.');
    return { id: existing?.id || `custom-${randomUUID()}`, nome, colunas, custom: true, revision: (existing?.revision || 0) + 1 };
}
module.exports = { listLayoutFields, validateLayout, phoneSlot };
