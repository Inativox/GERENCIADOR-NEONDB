// Isolated visual preview. No production preload, credentials, files or databases.
const { app, BrowserWindow } = require('electron');
const fs = require('node:fs/promises');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const { promisify } = require('node:util');
const assert = require('node:assert/strict');
const execFile = promisify(require('node:child_process').execFile);
const root = path.resolve(__dirname, '..');
const directory = path.join(root, 'out', 'flow-scene-preview');
const smoke = process.argv.includes('--smoke');
if (smoke) app.disableHardwareAcceleration();
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
let window;
app.setPath('userData', path.join(directory, 'profile'));
app.on('window-all-closed', () => app.quit());

app.whenReady().then(async () => {
    await fs.mkdir(directory, { recursive: true });
    const entry = path.join(directory, 'preview.tsx');
    const file = name => JSON.stringify(path.join(root, name).replaceAll('\\', '/'));
    await fs.writeFile(entry, `
import React, {useEffect, useState} from ${file('node_modules/react/index.js')};
import {createRoot} from ${file('node_modules/react-dom/client.js')};
import {FlowScene} from ${file('src/renderer/react/FlowScene.tsx')};
import ${file('src/renderer/react/fluxos.css')};
function Preview() {
    const [step,setStep]=useState('cleaning'),[api,setApi]=useState(true),[enrich,setEnrich]=useState(true),[dark,setDark]=useState(false);
    useEffect(()=>{document.body.className=dark?'dark-theme':'light-theme'},[dark]);
    const flow={id:'preview',name:'Receita · comércio SP',operation:'c6',api:{enabled:api},enrichment:{enabled:enrich},cleaning:{enabled:true},output:{csv:true}};
    const status=['completed','empty','interrupted','failed'].includes(step)?step:'running';
    const stage=['completed','empty'].includes(step)?'export':['interrupted','failed','slow'].includes(step)?'cleaning':step;
    const total=stage==='generation'?null:stage==='export'?7814283:7924618;
    const progress={stage,processed:stage==='generation'?9460000:Math.floor(total*.68),total,complete:step==='completed'};
    const job=step==='preview'?undefined:{id:step==='slow'?'demo-slow':'demo',flowName:flow.name,stage,status,progress,updatedAt:new Date(Date.now()-(step==='slow'?180000:0)).toISOString(),counts:{generated:9460000,...(stage!=='generation'?{enriched:8245910}:{}),...(['cleaning','export'].includes(stage)?{apiAvailable:7924618}:{}),...(stage==='export'?{cleaned:7814283}:{}),...(step==='completed'?{exported:7814283}:{}),...(step==='empty'?{cleaned:0,exported:0}:{})}};
    return <main className="demo"><header className="demo-heading"><span>PRÉVIA VISUAL · DADOS DE DEMONSTRAÇÃO</span><h1>Uma nova visão para seus fluxos.</h1><p>Explore as etapas e veja como a cena reage ao processamento.</p></header>
    <div className="demo-controls"><label>Etapa de demonstração<select aria-label="Etapa de demonstração" value={step} onChange={e=>setStep(e.target.value)}>{[['preview','Antes de iniciar'],['generation','Geração Receita'],['enrichment','Enriquecimento'],...(api?[['api','API C6']]:[]),['cleaning','Limpeza'],['slow','Sem avanço'],['export','Exportação'],['completed','Concluído'],['interrupted','Interrompido'],['failed','Falhou'],['empty','Sem resultados']].map(([id,label])=><option key={id} value={id}>{label}</option>)}</select></label><label><input type="checkbox" checked={api} onChange={e=>{setApi(e.target.checked);if(step==='api')setStep('cleaning')}}/>Incluir consulta API</label><label><input type="checkbox" checked={enrich} onChange={e=>setEnrich(e.target.checked)}/>Enriquecimento ativo</label><button onClick={()=>setDark(v=>!v)}>{dark?'Tema claro':'Tema escuro'}</button></div>
    <div className="flows-app"><FlowScene flow={flow} job={job}/></div><p className="demo-footnote">Passe o mouse pela cena para mudar a perspectiva. Selecione uma etapa para ver sua função.</p></main>;
}
createRoot(document.getElementById('root')).render(<Preview/>);
`);
    await execFile('node', ['--input-type=module', '-e', `import {build} from ${JSON.stringify(pathToFileURL(path.join(root, 'node_modules/vite/dist/node/index.js')).href)}; await build({configFile:false,logLevel:'error',root:${JSON.stringify(root)},define:{'process.env.NODE_ENV':'"production"'},build:{outDir:${JSON.stringify(path.join(directory, 'bundle'))},emptyOutDir:true,lib:{entry:${JSON.stringify(entry)},formats:['iife'],name:'FlowPreview',fileName:()=> 'preview.js'}}});`], { windowsHide: true });
    const css = (await fs.readdir(path.join(directory, 'bundle'))).find(name => name.endsWith('.css'));
    const html = path.join(directory, 'index.html');
    await fs.writeFile(html, `<!doctype html><html lang="pt-BR"><head><meta charset="UTF-8"><title>Prévia 3D · Gerenciador de Bases</title><link rel="stylesheet" href="${pathToFileURL(path.join(root, 'src/styles/index.css')).href}"><link rel="stylesheet" href="bundle/${css}"><style>
body{display:block;overflow:auto;background:var(--bg-main);color:var(--text-primary)}.demo{max-width:1180px;margin:auto;padding:32px 36px}.demo-heading span{font:10px var(--font-mono);letter-spacing:.1em;color:var(--text-secondary)}.demo-heading h1{font:500 28px/1.3 var(--font-sans);letter-spacing:-.03em;margin:9px 0;color:var(--text-primary)}.demo-heading p,.demo-footnote{font:12px/1.6 var(--font-sans);color:var(--text-secondary)}.demo-controls{display:flex;align-items:end;flex-wrap:wrap;gap:20px;margin:24px 0 14px;font:11px var(--font-sans)}.demo-controls label:first-child{display:grid;gap:7px;min-width:180px}.demo-controls select,.demo-controls button{background:var(--bg-input);color:var(--text-primary);border:1px solid var(--border-color);border-radius:5px;padding:9px;font:12px var(--font-sans)}.demo-controls input{accent-color:var(--accent-color)}.demo-footnote{text-align:center}
</style></head><body class="light-theme"><div id="root"></div><script src="bundle/preview.js"></script></body></html>`);
    window = new BrowserWindow({ show: false, width: 1280, height: smoke ? 1040 : 860, title: 'Prévia 3D · Dados de demonstração', autoHideMenuBar: true, webPreferences: { contextIsolation: true, nodeIntegration: false, backgroundThrottling: false, offscreen: smoke } });
    window.webContents.session.webRequest.onBeforeRequest({ urls: ['http://*/*', 'https://*/*'] }, (_details, callback) => callback({ cancel: true }));
    await window.loadFile(html);
    const evaluate = code => window.webContents.executeJavaScript(code);
    const waitFor = async expression => { for (let i = 0; i < 150; i++) { if (await evaluate(expression)) return; await delay(20); } throw new Error('Preview state not reached: ' + expression); };
    await waitFor(`document.querySelector('.flow-scene') !== null`);
    // Keep the optional presentation expanded for this isolated preview.
    if (await evaluate(`document.querySelector('.flow-scene.is-compact') !== null`)) await evaluate(`document.querySelector('.flow-scene-toggle').click()`);
    await waitFor(`document.querySelectorAll('.flow-scene-station').length === 5`);
    if (!smoke) { window.show(); console.log('Prévia 3D aberta com dados de demonstração.'); return; }
    await delay(350);
    const selectStage = async stage => {
        await evaluate(`document.querySelector('select').value = ${JSON.stringify(stage)}; document.querySelector('select').dispatchEvent(new Event('change', {bubbles:true}))`);
        await delay(350);
    };
    const assertFraming = async () => {
        const clipped = await evaluate(`(() => {
            const view = document.querySelector('.flow-scene-viewport').getBoundingClientRect();
            return [...document.querySelectorAll('.flow-scene-ground, .flow-volume-top, .flow-scene-station-readout, .flow-scene-floor-number')].filter(node => {
                const rect = node.getBoundingClientRect();
                return rect.top < view.top || rect.bottom > view.bottom || rect.left < view.left || rect.right > view.right;
            }).map(node => ({className:node.className, rect:node.getBoundingClientRect().toJSON(), view:view.toJSON()}));
        })()`);
        assert.deepEqual(clipped, [], `As máquinas e os indicadores devem caber inteiros na cena: ${JSON.stringify(clipped)}`);
    };
    await assertFraming();
    assert.equal(await evaluate(`getComputedStyle(document.querySelector('.flow-scene')).backgroundColor`), 'rgb(255, 255, 255)');
    assert.match(await evaluate(`document.querySelector('.flow-scene-station.state-active .flow-scene-station-readout').textContent`), /68%/);
    assert.notEqual(await evaluate(`getComputedStyle(document.querySelector('.state-complete .flow-scene-station-readout')).color`), await evaluate(`getComputedStyle(document.querySelector('.state-active .flow-scene-station-readout')).color`));
    await fs.writeFile(path.join(root, 'out/flow-scene-preview.png'), (await window.webContents.capturePage()).toPNG());
    assert.equal(await evaluate(`document.querySelector('.flow-scene-station.state-active').dataset.stage`), 'cleaning');
    await evaluate(`document.querySelector('.flow-scene-viewport').dispatchEvent(new PointerEvent('pointermove',{bubbles:true,pointerType:'mouse',clientX:900,clientY:400}))`);
    await delay(250);
    assert.notEqual(await evaluate(`document.querySelector('.flow-scene').style.getPropertyValue('--scene-tilt-y')`), '0deg');
    window.webContents.debugger.attach('1.3');
    await window.webContents.debugger.sendCommand('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-reduced-motion', value: 'reduce' }] });
    await waitFor(`document.querySelector('.flow-scene').dataset.motion === 'off'`);
    assert.equal(await evaluate(`getComputedStyle(document.querySelector('.state-active .flow-scene-beacon')).animationName`), 'none');
    await window.webContents.debugger.sendCommand('Emulation.setEmulatedMedia', { features: [] });
    window.webContents.debugger.detach();
    await evaluate(`document.querySelector('.flow-scene-viewport').dispatchEvent(new PointerEvent('pointerleave', {bubbles:true}))`);
    await selectStage('slow');
    await waitFor(`document.querySelector('.flow-scene').dataset.health === 'slow'`);
    assert.match(await evaluate(`document.querySelector('.flow-scene-warning').textContent`), /Nenhum novo lote confirmado há 3 min/);
    await fs.writeFile(path.join(root, 'out/flow-scene-warning.png'), (await window.webContents.capturePage()).toPNG());
    await selectStage('failed');
    await waitFor(`document.querySelector('.flow-scene').dataset.health === 'failed'`);
    assert.match(await evaluate(`document.querySelector('.flow-scene-station.state-failed').textContent`), /68%/);
    await fs.writeFile(path.join(root, 'out/flow-scene-error.png'), (await window.webContents.capturePage()).toPNG());
    await selectStage('generation');
    assert.equal(await evaluate(`document.querySelector('.flow-scene [role="progressbar"]').hasAttribute('aria-valuenow')`), false);
    assert.match(await evaluate(`document.querySelector('.flow-scene-progress-summary').textContent`), /9.460.000 registros gerados/);
    await selectStage('completed');
    assert.equal(await evaluate(`document.querySelector('.flow-scene [role="progressbar"]').getAttribute('aria-valuenow')`), '100');
    await selectStage('cleaning');
    await waitFor(`document.querySelector('.flow-scene').dataset.health === 'active'`);
    await evaluate(`document.querySelector('.demo-controls button').click()`);
    await waitFor(`document.body.classList.contains('dark-theme')`);
    await delay(200);
    assert.notEqual(await evaluate(`getComputedStyle(document.querySelector('.flow-scene')).backgroundColor`), 'rgb(255, 255, 255)');
    await fs.writeFile(path.join(root, 'out/flow-scene-preview-dark.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.querySelector('.demo-controls button').click()`);
    await waitFor(`document.body.classList.contains('light-theme')`);
    await evaluate(`document.querySelector('.demo-controls input').click()`);
    await waitFor(`document.querySelectorAll('.flow-scene-station').length === 4`);
    await delay(250); await assertFraming();
    await fs.writeFile(path.join(root, 'out/flow-scene-preview-four.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.querySelector('.demo-controls input').click()`);
    window.setSize(780, 1020); await delay(250);
    await assertFraming();
    assert.ok(await evaluate(`document.querySelector('.flow-scene').scrollWidth <= document.querySelector('.flow-scene').clientWidth + 1`));
    await fs.writeFile(path.join(root, 'out/flow-scene-preview-compact.png'), (await window.webContents.capturePage()).toPNG());
    window.setSize(560, 1020); await delay(250); await assertFraming();
    assert.ok(await evaluate(`document.querySelector('.flow-scene').scrollWidth <= document.querySelector('.flow-scene').clientWidth + 1`));
    console.log('Prévia 3D aprovada: percentual, total desconhecido, cores, falta de avanço, recuperação, temas, enquadramento de quatro/cinco etapas, movimento reduzido e telas menores.');
    app.quit();
}).catch(error => { console.error(error); app.exit(1); });
