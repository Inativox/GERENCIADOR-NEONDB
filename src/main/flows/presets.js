'use strict';

// Presets são fluxos fixos do aplicativo: aparecem para todos os usuários, não podem ser
// editados nem excluídos e são montados de novo a cada abertura e a cada execução.
// Origem: fluxos salvos do Davi em 03/11/2026 (C6, Mercado Pago, PagBank e Santander).
const PRESET_PREFIX = 'preset-';
const PROHIBITED_CNAES = Object.freeze([
    '0114800', '0220906', '0724301', '0729404', '0893200', '0899101', '0899102', '0899103', '0899199', '1111902',
    '1210700', '1220401', '1220402', '1220403', '1220499', '2092401', '2442300', '2550101', '2550102', '3211602',
    '3211603', '4520005', '4623104', '4636201', '4681801', '4681802', '4681803', '4681804', '4681805', '4731800',
    '4732600', '4782202', '4783101', '4783102', '4789009', '5590601', '6434400', '6440900', '6491300', '6619399',
    '7911200', '7912100', '8299704', '8299706', '8422100', '8711504', '8720401', '8730101', '8730102', '9002702',
    '9200301', '9200302', '9200399', '9329803', '9329804', '9411100', '9412099', '9420100', '9430800', '9491000',
    '9492800', '9493600', '9499500', '9529106', '9609204',
]);
// CNAEs de varejo, alimentação e serviços usados nas listas de maquininha (Mercado Pago e PagBank).
const MAQUININHA_CNAES = Object.freeze([
    '1091102', '4520001', '4530701', '4530703', '4711302', '4712100', '4713004', '4721102', '4722901', '4722902',
    '4723700', '4729699', '4744005', '4744099', '4751201', '4752100', '4753900', '4754701', '4759801', '4761001',
    '4761003', '4763601', '4763602', '4774100', '4781400', '4782201', '4789004', '5510801', '5590699', '5611201',
    '5611203', '5611204', '5611205', '5620102', '5620104', '7500100', '8592901', '8593700', '8630501', '8630503',
    '8630504', '8650003', '8650004', '8690901', '9313100', '9602501', '9602502', '9609206',
]);

/** Data local (fuso da máquina) no formato AAAA-MM-DD; a abertura vai até o dia corrente. */
function localDate(now = new Date()) {
    const pad = value => String(value).padStart(2, '0');
    return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
}

function preset({ id, name, operation, pipelines, generation, cleaning = {}, fastway, output }) {
    return {
        id, revision: 1, preset: true, name, operation, pipelines,
        generation: { limit: null, uf: [], cidade: [], bairro: [], cnaes: [], machineCnaes: false, naturezas: [], dateFrom: '2000-01-01', dateTo: '',
            mei: 'no', phone: 'all', email: 'all', situacoes: ['02'], availability: 'all', source: 'bq', ...generation },
        enrichment: { enabled: true, strategy: 'append', fillCpf: false },
        api: { enabled: false, keyMode: 'dupla', delayMs: 60000 },
        cleaning: { enabled: true, rootSource: 'bq', rootFile: '', blocklist: true, invalidPhones: true, removeLandlines: true, fillLivre5: true, prohibitedCnaes: [...PROHIBITED_CNAES], ...cleaning },
        fastway: { enabled: true, ...fastway },
        output: { csv: true, includeSituacao: false, ...output },
    };
}

function listPresets(now = new Date()) {
    const today = localDate(now);
    return [
        preset({ id: 'preset-c6-bank', name: 'C6 BANK', operation: 'c6', pipelines: [90],
            generation: { dateTo: today, availability: 'available' },
            fastway: { campaignId: '120' }, output: { fileName: 'RCAN', formatId: 'padrao_8_fones', rowsPerFile: 250000 } }),
        // Mercado Pago gera só empresas abertas até 2023, como no fluxo salvo.
        preset({ id: 'preset-mercado-pago', name: 'MERCADO PAGO', operation: 'mercadopago', pipelines: [58],
            generation: { limit: 150000, cnaes: [...MAQUININHA_CNAES], dateTo: '2023-12-31' },
            fastway: { campaignId: '18' }, output: { fileName: 'MP PADRÃO', formatId: 'padrao_8_fones', rowsPerFile: 150000 } }),
        preset({ id: 'preset-pagbank', name: 'PAGBANK', operation: 'pagbank', pipelines: [34],
            generation: { limit: 150000, cnaes: [...MAQUININHA_CNAES], dateTo: today, mei: 'yes' },
            fastway: { campaignId: '87' }, output: { fileName: 'PAGBANK PADRAO', formatId: 'olos', rowsPerFile: 150000 } }),
        preset({ id: 'preset-santander', name: 'SANTANDER', operation: 'santander', pipelines: [119],
            generation: { limit: 200000, dateTo: today },
            fastway: { campaignId: '63' }, output: { fileName: 'SANTANDER PADRAO', formatId: 'padrao_8_fones', rowsPerFile: 250000 } }),
    ];
}
function isPresetId(id) { return typeof id === 'string' && id.startsWith(PRESET_PREFIX); }
function getPreset(id, now = new Date()) { return listPresets(now).find(item => item.id === id); }

module.exports = { listPresets, getPreset, isPresetId, localDate, PROHIBITED_CNAES, MAQUININHA_CNAES };
