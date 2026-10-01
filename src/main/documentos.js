'use strict';
const CNPJ_FORMAT = /^[A-Z0-9]{12}\d{2}$/;
function cnpjText(value) {
    if (typeof value !== 'string' && typeof value !== 'number') return '';
    const text = String(value).trim().toUpperCase().replace(/[\s./-]/g, '');
    return CNPJ_FORMAT.test(text) ? text : '';
}
function cnpjCheckDigits(value) {
    if (!CNPJ_FORMAT.test(value) || /^(\d)\1+$/.test(value)) return false;
    const check = size => {
        let sum = 0, weight = size - 7;
        for (let index = 0; index < size; index++) { sum += (value.charCodeAt(index) - 48) * weight--; if (weight < 2) weight = 9; }
        const remainder = sum % 11; return remainder < 2 ? 0 : 11 - remainder;
    };
    return check(12) === Number(value[12]) && check(13) === Number(value[13]);
}
module.exports = { CNPJ_FORMAT, cnpjText, cnpjCheckDigits };
