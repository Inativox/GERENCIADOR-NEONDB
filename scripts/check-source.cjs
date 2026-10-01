const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { credentialLocations, localSecrets } = require('./credential-guard.cjs');
const root = path.resolve(__dirname, '..');
const staged = process.argv.includes('--staged');
const revision = staged ? null : process.argv.find((arg, index) => index > 1 && !arg.startsWith('--')) || 'HEAD';
const git = args => execFileSync('git', args, { cwd: root, encoding: 'utf8', maxBuffer: 20 * 1024 * 1024 });
const files = git(staged ? ['diff', '--cached', '--name-only', '--diff-filter=ACMR', '-z'] : ['ls-tree', '-r', '--name-only', '-z', revision]).split('\0').filter(Boolean);
const secrets = localSecrets(root);
const problems = [];
for (const file of files) {
    if (/(^|\/)(\.env(?:\.[^/]*)?|users\.json|access\.json)(\/|$)|\.(mbkey|mbconfig|py)$/.test(file) && !file.endsWith('.env.example')) problems.push(`${file}: private file`);
    if (!/\.(js|mjs|cjs|ts|tsx|json|md|html|css|txt|ya?ml|example)$/i.test(file)) continue;
    const content = git(['show', staged ? `:${file}` : `${revision}:${file}`]);
    for (const line of credentialLocations(content, secrets)) problems.push(`${file}:${line}: possible credential`);
}
if (problems.length) {
    console.error(`Source check blocked (${problems.length} findings):\n${problems.join('\n')}`);
    process.exitCode = 1;
} else console.log(`Source verified: ${files.length} files; no detected Neon literals, private keys or local credentials.`);
