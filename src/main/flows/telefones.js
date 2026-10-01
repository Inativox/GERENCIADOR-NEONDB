'use strict';

const { normalizarTelefone } = require('../limpezaTelefones');

// Match inserir9 in the existing D:/BASES/exportar_leads.py exporter.
// Only complete legacy numbers (DDD + 8 digits, starting 6–9) receive the 9.
function normalizarTelefoneFluxo(value, { ajustarNonoDigito = true } = {}) {
    const normalized = normalizarTelefone(value);
    const usable = normalized.phone.length === 10 && !/^[2-9]$/.test(normalized.phone[2]) ? '' : normalized.phone;
    const ninthDigitAdded = ajustarNonoDigito && usable.length === 10 && /^[6-9]$/.test(usable[2]);
    const phone = ninthDigitAdded ? usable.slice(0, 2) + '9' + usable.slice(2) : usable;
    return { ...normalized, phone, ninthDigitAdded, landline: phone.length === 10 && /^[2-5]$/.test(phone[2]) };
}

function telefoneParaGeracao(value) {
    // Preserve malformed source values for the existing dirty-phone filter.
    // Complete mobiles are normalized as they enter the generation stage.
    return normalizarTelefoneFluxo(value).phone || String(value ?? '');
}

function variantesTelefoneFluxo(phone) {
    const normalized = normalizarTelefoneFluxo(phone).phone;
    if (!normalized) return [];
    const variants = [normalized, '55' + normalized];
    // Check both spellings against blocklist/invalid-phone data. Normalization
    // must never allow an old spelling of a blocked mobile to bypass the filter.
    if (normalized.length === 11 && normalized[2] === '9' && /^[6-9]$/.test(normalized[3])) {
        const legacy = normalized.slice(0, 2) + normalized.slice(3);
        variants.push(legacy, '55' + legacy);
    }
    return variants;
}

module.exports = { normalizarTelefoneFluxo, variantesTelefoneFluxo, telefoneParaGeracao };
