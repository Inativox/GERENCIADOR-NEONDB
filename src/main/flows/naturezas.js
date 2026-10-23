'use strict';
// Only these legal natures may be generated: lists uploaded without this filter reached Fastway with wrong companies.
// An empty selection ("todas") means all of these, never the whole Receita.
const ALLOWED_NATUREZAS = Object.freeze(['2062', '2070', '2089', '2135', '2232', '2240', '2259', '2267', '2305', '2313', '4014']);
const ALLOWED = new Set(ALLOWED_NATUREZAS);
const isAllowedNatureza = code => ALLOWED.has(String(code).trim());

module.exports = { ALLOWED_NATUREZAS, isAllowedNatureza };
