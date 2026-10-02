// Synthetic local benchmark. It never connects to Receita, Neon or BQ.
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { performance } = require('node:perf_hooks');
const { runFlow } = require('../src/main/flows/pipeline');
const { defaults, validateFlow, effectiveFlow } = require('../src/main/flows/config');
async function main() {
    const csv = process.argv.includes('--csv'), count = Number(process.argv.find(value => /^\d+$/.test(value)) || 50000);
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'flows-benchmark-'));
    try {
        const input = defaults(); input.generation.limit = count; input.cleaning.enabled = false; input.cleaning.blocklist = false; input.output.csv = csv;
        const flow = effectiveFlow(validateFlow(input), { username: 'Davi' }); flow.output.directory = directory;
        const start = performance.now();
        const result = await runFlow({ flow, user: { username: 'Davi' }, jobDir: path.join(directory, 'job'),
            providers: { iterateReceita: async function* () { for (let start = 0; start < count; start += 2000) { const rows = Array.from({ length: Math.min(2000, count - start) }, (_, offset) => { const index = start + offset; return { cnpj: String(10000000000000n + BigInt(index)), razao_social: `Empresa sintética ${index}`, telefone_principal: '119' + String(12000000 + index), situacao_cadastral_cod: '02', situacao_cadastral: 'Ativa', atividade_principal_cod: '4711302' }; }); yield { rows }; } } } });
        const ms = Math.round(performance.now() - start), sizes = {};
        for (const file of result.outputs) sizes[file.kind] = (sizes[file.kind] || 0) + (await fs.stat(file.path)).size;
        console.log(JSON.stringify({ mode: csv ? 'xlsx+csv' : 'xlsx', generated: result.counts.generated, exported: result.counts.exported, ms, peakRssMiB: Math.round(process.resourceUsage().maxRSS / 1024), bytes: sizes }));
    } finally { await fs.rm(directory, { recursive: true, force: true }); }
}
main().catch(() => { console.error('Benchmark local falhou.'); process.exitCode = 1; });
