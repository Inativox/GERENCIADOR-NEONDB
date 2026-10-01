// Execute: npx electron tests/flowUI.smoke.cjs. Only a synthetic API is loaded.
const { app, BrowserWindow } = require('electron');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const fsSync = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const { promisify } = require('node:util');
const execFile = promisify(require('node:child_process').execFile);
const taskDir = fsSync.mkdtempSync(path.join(os.tmpdir(), 'flow-ui-smoke-'));
const root = path.resolve(__dirname, '..');
app.setPath('userData', path.join(taskDir, 'profile'));
app.disableHardwareAcceleration();
app.on('window-all-closed', () => {});
let window;
let exitCode = 0;
const timeout = setTimeout(() => { console.error('Flow UI smoke timed out'); app.exit(1); }, 45000);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const evaluate = source => window.webContents.executeJavaScript(source).catch(error => { console.error('Failed smoke expression:', source); throw error; });
async function waitFor(source) {
    for (let n = 0; n < 150; n++) { if (await evaluate(source)) return; await delay(20); }
    throw new Error(`UI state not reached: ${source}`);
}
const click = text => evaluate(`Array.from(document.querySelectorAll('.flows-app button')).find(item => item.textContent.trim() === ${JSON.stringify(text)}).click()`);
const input = (label, value) => evaluate(`(() => { const element = Array.from(document.querySelectorAll('.flow-field')).find(item => item.firstElementChild.textContent === ${JSON.stringify(label)}).querySelector('input'); Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(element, ${JSON.stringify(value)}); element.dispatchEvent(new Event('input',{bubbles:true})); })()`);
const checkbox = text => evaluate(`Array.from(document.querySelectorAll('.flow-check')).find(item => item.textContent === ${JSON.stringify(text)}).querySelector('input').click()`);
const selectField = (label, value) => evaluate(`(() => { const element=Array.from(document.querySelectorAll('.flow-field')).find(item=>item.firstElementChild.textContent===${JSON.stringify(label)}).querySelector('select'); element.value=${JSON.stringify(value)}; element.dispatchEvent(new Event('change',{bubbles:true})); })()`);
const openDropdown = label => evaluate(`Array.from(document.querySelectorAll('.receita-select')).find(item=>item.querySelector('.receita-select-label').textContent===${JSON.stringify(label)}).querySelector('.receita-select-trigger').click()`);
const chooseOption = value => evaluate(`Array.from(document.querySelectorAll('.receita-select-options label')).find(item=>item.textContent===${JSON.stringify(value)}).querySelector('input').click()`);

app.whenReady().then(async () => {
    const entry = path.join(taskDir, 'entry.tsx');
    await fs.writeFile(entry, `import React from ${JSON.stringify(path.join(root, 'node_modules/react/index.js').replaceAll('\\', '/'))}; import {createRoot} from ${JSON.stringify(path.join(root, 'node_modules/react-dom/client.js').replaceAll('\\', '/'))}; import {Fluxos} from ${JSON.stringify(path.join(root, 'src/renderer/react/Fluxos.tsx').replaceAll('\\', '/'))}; createRoot(document.getElementById('root')).render(React.createElement(Fluxos)); import {ReceitaSituacao} from ${JSON.stringify(path.join(root, 'src/renderer/react/ReceitaSituacao.tsx').replaceAll('\\', '/'))}; createRoot(document.getElementById('receita-root')).render(React.createElement(ReceitaSituacao));`);
    // Electron 28 embeds Node 18; the installed Vite is built with the system Node runtime.
    await execFile('node', ['--input-type=module', '-e', `import {build} from ${JSON.stringify(pathToFileURL(path.join(root, 'node_modules/vite/dist/node/index.js')).href)}; await build({configFile:false,logLevel:'error',define:{'process.env.NODE_ENV':'"production"'},root:${JSON.stringify(root)},build:{outDir:${JSON.stringify(path.join(taskDir,'bundle'))},emptyOutDir:true,lib:{entry:${JSON.stringify(entry)},formats:['iife'],name:'Smoke',fileName:()=> 'ui.js'}}});`]);
    const preload = path.join(taskDir, 'preload.cjs');
    await fs.writeFile(preload, `
const {contextBridge} = require('electron');
let username='Outro', role='admin', flows=[], jobs=[], callbacks=[], authCallbacks=[], calls=[], access={receitaConfigured:true,bqConfigured:true,bqLoginMode:'gcloud',bqAutoLogin:true};
let receitaJob=null, receitaCallbacks=[];
let formats=[{id:'padrao',nome:'Padrão',colunas:[{header:'CNPJ',campo:'cnpj'},{header:'NOME',campo:'razao_social'},{header:'FONE1',campo:'telefone_1'}]}];
const layoutFields=${JSON.stringify(require('../src/main/flows/layouts').listLayoutFields())};
const api={
receitaSituacaoState:async()=>({success:true,configured:true,job:receitaJob}),
receitaSituacaoFile:async()=>({success:true,file:{id:'fixture-list',name:'Lista própria.xlsx'}}),
receitaSituacaoOne:async cnpj=>{calls.push(['situacao-one',cnpj]);return {success:true,found:true,result:{cnpj:'12ABC34501DE35',razao_social:'Empresa fictícia',situacao_cadastral_cod:'04',situacao_cadastral:'Inapta',situacao_cadastral_data:'2026-09-01'}};},
receitaSituacaoStart:async id=>{calls.push(['situacao-start',id]);receitaJob={id:'situacao-job',owner:username,name:'Lista própria.xlsx',status:'running',message:'Consultando o banco da Receita.',output:null,counts:{processed:0,found:0,notFound:0,invalid:0}};return {success:true,job:receitaJob};},
receitaSituacaoOpen:async()=>{calls.push(['situacao-open']);return {success:true};},
onReceitaSituacaoUpdate:callback=>{receitaCallbacks.push(callback);return ()=>{};},
emitReceita:job=>{receitaJob=job;receitaCallbacks.forEach(callback=>callback(job));},

flowsBootstrap:async()=>({success:true,user:{username,role},flows,jobs,formats,layoutFields,access,limits:{maxRows:500000}}),
flowsReceitaOptions:async input=>{calls.push(['options',input]);let options=({uf:[{value:'SP',label:'SP'},{value:'RJ',label:'RJ'}],cidade:[{value:'Campinas',label:'Campinas'},{value:'São Paulo',label:'São Paulo'}],bairro:[{value:'Centro',label:'Centro'},{value:'Cambuí',label:'Cambuí'}],naturezas:[{value:'2062',label:'2062 · Sociedade Empresária Limitada'},{value:'2135',label:'2135 · Empresário Individual'}],cnaes:input.offset?[{value:'4721102',label:'4721102 · Padaria e confeitaria'}]:[{value:'4711302',label:'4711302 · Supermercados'}]})[input.field]||[];if(input.search)options=options.filter(item=>(item.value+' '+item.label).toUpperCase().includes(input.search.toUpperCase()));return {success:true,options,hasMore:input.field==='cnaes'&&!input.offset&&!input.search};},
flowsSaveLayout:async layout=>{calls.push(['save-layout',layout]);const saved={...layout,id:layout.id||'custom-'+formats.length,custom:true,revision:(layout.revision||0)+1};formats=[...formats.filter(item=>item.id!==saved.id),saved];return {success:true,layout:saved};},
flowsDeleteLayout:async id=>{calls.push(['delete-layout',id]);if(flows.some(flow=>flow.output.formatId===id))return {success:false,message:'Layout usado por um fluxo.'};formats=formats.filter(item=>item.id!==id);return {success:true};},
flowsPreviewLayout:async input=>({success:true,preview:{headers:input.layout.colunas.map(column=>column.header),values:input.layout.colunas.map(column=>column.valor_manual||'Exemplo')}}),
flowsSave:async flow=>{calls.push(['save',flow]); const saved={...flow,id:flow.id||'flow-'+(flows.length+1),revision:flow.revision+1}; flows=[saved,...flows.filter(item=>item.id!==saved.id)]; return {success:true,flow:saved};},
flowsDelete:async id=>{flows=flows.filter(flow=>flow.id!==id);calls.push(['delete',id]);return {success:true};},
flowsSelectFolder:async()=>({success:true,path:'C:/synthetic/output'}),
flowsStart:async value=>{calls.push(['start',value]);const flow=flows.find(item=>item.id===value.flowId);const job={id:'job-1',flowId:flow.id,flowName:flow.name,owner:username,status:'running',stage:'generation',createdAt:'2026-09-30T12:00:00Z',updatedAt:'2026-09-30T12:00:00Z',counts:{generated:0},outputs:[],logs:['<img src=x onerror=alert(1)>'],flowSnapshot:flow};jobs=[job];return {success:true,job};},
flowsCancel:async id=>{calls.push(['cancel',id]);jobs=jobs.map(job=>({...job,status:'cancelled'}));return {success:true,job:jobs[0]};},
flowsResume:async id=>{calls.push(['resume',id]);jobs=jobs.map(job=>({...job,status:'running'}));return {success:true,job:jobs[0]};},
flowsOpenOutput:async value=>{calls.push(['open',value]);return {success:true};},
flowsConfigureReceita:async value=>{calls.push(['receita',value]);access.receitaConfigured=true;return {success:true};},
flowsConfigureBq:async()=>{calls.push(['bq']);return {success:true};},
flowsTestBq:async()=>{calls.push(['bq-test']);return {success:true};},
flowsRenewBq:async()=>{calls.push(['bq-login']);access.bqAuth={owner:username,state:'ready',message:'Login Google renovado.'};return {success:true};},
flowsBqAutoLogin:async enabled=>{calls.push(['bq-auto-login',enabled]);access.bqAutoLogin=enabled;return {success:true};},
onFlowBqAuthUpdate:callback=>{authCallbacks.push(callback);return ()=>{authCallbacks=authCallbacks.filter(item=>item!==callback);};},
onFlowUpdate:callback=>{callbacks.push(callback);return ()=>{callbacks=callbacks.filter(item=>item!==callback);};},
selectFile:async()=>['C:/synthetic/root.xlsx'],
inspect:()=>({calls,flows,jobs,formats}),
emitAuth:update=>{access.bqAuth=update;authCallbacks.forEach(callback=>callback(update));},
setUser:user=>{username=user;},
emit:job=>{jobs=[job,...jobs.filter(item=>item.id!==job.id)];callbacks.forEach(callback=>callback(job));}
}; contextBridge.exposeInMainWorld('electronAPI',api);`);
    const css = fsSync.readdirSync(path.join(taskDir, 'bundle')).find(name => name.endsWith('.css'));
    const html = path.join(taskDir, 'smoke.html');
    await fs.writeFile(html, `<!doctype html><html><head><meta charset="utf-8"><link rel="stylesheet" href="${path.join(root, 'src/styles/index.css').replaceAll('\\', '/')}"><link rel="stylesheet" href="bundle/${css}"></head><body class="light-theme"><div id="root"></div><div id="receita-root"></div><script src="${path.join(root, 'src/renderer/flowProgress.js').replaceAll('\\', '/')}"></script><script src="bundle/ui.js"></script></body></html>`);
    window = new BrowserWindow({ show: false, width: 1380, height: 1000, webPreferences: { preload, contextIsolation: true, nodeIntegration: false, backgroundThrottling: false, offscreen: true } });
    const errors = [];
    window.webContents.on('console-message', (_event, level, message) => { if (level === 3) errors.push(message); });
    await window.loadFile(html);
    try { await waitFor(`document.querySelector('.flow-editor') !== null`); }
    catch (error) { console.error('Renderer diagnostics:', errors, await evaluate(`document.body.innerHTML`)); throw error; }
    assert.equal(await evaluate(`Array.from(document.querySelectorAll('.flow-check')).find(item=>item.textContent==='Aplicar blocklist').querySelector('input').disabled`), true);
    const apiLabel = 'Validar na Limpeza API antes de exportar';
    assert.equal(await evaluate(`Array.from(document.querySelectorAll('.flow-check')).find(item=>item.textContent===${JSON.stringify(apiLabel)}).querySelector('input').checked`), true);
    await selectField('Operação', 'santander');
    assert.equal(await evaluate(`document.querySelector('.flow-editor').textContent.includes(${JSON.stringify(apiLabel)})`), false);
    await selectField('Operação', 'c6');
    await checkbox(apiLabel);
    await input('Nome do fluxo', 'C6 comércio');
    await input('Limite de empresas', '');
    await openDropdown('UFs');
    await waitFor(`document.querySelectorAll('.receita-select-options label').length===2`);
    await chooseOption('SP'); await chooseOption('RJ'); await click('Concluir seleção');
    await openDropdown('Cidades');
    await waitFor(`document.querySelectorAll('.receita-select-options label').length===2`);
    await chooseOption('Campinas'); await chooseOption('São Paulo'); await click('Concluir seleção');
    await openDropdown('Bairros');
    await waitFor(`document.querySelectorAll('.receita-select-options label').length===2`);
    await chooseOption('Centro'); await chooseOption('Cambuí'); await click('Concluir seleção');
    await openDropdown('CNAEs');
    await waitFor(`document.querySelector('.receita-select-options label')?.textContent.includes('4711302')`);
    await chooseOption('4711302 · Supermercados'); await click('Carregar mais');
    await waitFor(`document.querySelectorAll('.receita-select-options label').length===2`);
    await chooseOption('4721102 · Padaria e confeitaria'); await click('Concluir seleção');
    await openDropdown('Naturezas jurídicas');
    await waitFor(`document.querySelectorAll('.receita-select-options label').length===2`);
    await evaluate(`(() => { const element=document.querySelector('[aria-label="Buscar em Naturezas jurídicas"]'); Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(element,'Sociedade'); element.dispatchEvent(new Event('input',{bubbles:true})); })()`);
    await waitFor(`document.querySelectorAll('.receita-select-options label').length===1`);
    await chooseOption('2062 · Sociedade Empresária Limitada'); await click('Concluir seleção');
    await checkbox('03 · Suspensa');
    await click('Salvar fluxo');
    await waitFor(`window.electronAPI.inspect().flows.length===1 && !document.querySelector('.flows-app').getAttribute('aria-busy').includes('true')`);
    const first = await evaluate(`window.electronAPI.inspect().flows[0]`);
    assert.deepEqual(first.generation.uf, ['SP', 'RJ']);
    assert.deepEqual(first.generation.cnaes, ['4711302', '4721102']);
    assert.deepEqual(first.generation.cidade, ['Campinas', 'São Paulo']);
    assert.deepEqual(first.generation.bairro, ['Centro', 'Cambuí']);
    assert.deepEqual(first.generation.naturezas, ['2062']);
    assert.equal(first.generation.limit, null);
    const requests = await evaluate(`window.electronAPI.inspect().calls.filter(item=>item[0]==='options').map(item=>item[1])`);
    assert.deepEqual(requests.find(item=>item.field==='cidade').uf,['SP','RJ']);
    assert.deepEqual(requests.find(item=>item.field==='bairro').cidade,['Campinas','São Paulo']);
    assert.ok(requests.some(item=>item.field==='cnaes'&&item.offset===50));
    assert.deepEqual(first.generation.situacoes, ['02', '03']);
    assert.equal(first.output.csv, false);
    assert.equal(first.cleaning.blocklist, true);
    assert.deepEqual(first.api, { enabled: false, keyMode: 'dupla', delayMs: 60000 });
    await openDropdown('UFs');
    await waitFor(`document.querySelectorAll('.receita-select-options label').length===2`);
    await chooseOption('RJ'); await click('Concluir seleção');
    assert.equal(await evaluate(`Array.from(document.querySelectorAll('.receita-select')).filter(item=>['Cidades','Bairros'].includes(item.querySelector('.receita-select-label').textContent)).every(item=>item.querySelector('.receita-select-trigger').textContent.includes('Todos')&&!item.querySelector('.receita-selected'))`),true);
    assert.deepEqual((await evaluate(`window.electronAPI.inspect().flows[0]`)).generation.cidade,['Campinas','São Paulo']);
    await input('Nome do fluxo', 'Rascunho preservado');
    await evaluate(`window.dispatchEvent(new Event('focus'))`);
    await delay(40);
    assert.equal(await evaluate(`document.querySelector('.flow-editor input').value`), 'Rascunho preservado');
    await click('Salvar fluxo');
    await waitFor(`window.electronAPI.inspect().flows[0].name==='Rascunho preservado'`);
    await click('Duplicar');
    await input('Nome do fluxo', 'Cópia C6');
    await click('Salvar fluxo');
    await waitFor(`window.electronAPI.inspect().flows.length===2`);
    await waitFor(`document.querySelectorAll('.flow-presets button').length === 2`);
    assert.equal(await evaluate(`Array.from(document.querySelectorAll('.flow-check')).find(item=>item.textContent===${JSON.stringify(apiLabel)}).querySelector('input').checked`), false);
    await checkbox(apiLabel);
    await click('Salvar fluxo');
    await waitFor(`window.electronAPI.inspect().flows[0].api?.enabled===true && document.querySelector('.flows-app').getAttribute('aria-busy')==='false'`);
    await fs.mkdir(path.join(root, 'out'), { recursive: true });
    await fs.writeFile(path.join(root, 'out', 'flows-ui-smoke.png'), (await window.webContents.capturePage()).toPNG());
    assert.notEqual((await evaluate(`window.electronAPI.inspect().flows`))[0].id, first.id);
    await click('Personalizar layout');
    await waitFor(`document.querySelector('.layout-editor')!==null`);
    await input('Nome do layout', 'C6 personalizado');
    await input('Nome da coluna 3', 'Celular principal');
    await evaluate(`document.querySelector('[aria-label="Subir coluna 3"]').click()`);
    await click('+ Adicionar coluna');
    await input('Nome da coluna 4', 'Campanha');
    await selectField('Origem da coluna 4', 'manual');
    await input('Texto fixo da coluna 4', 'Campanha C6');
    await waitFor(`document.querySelector('.layout-preview table')?.textContent.includes('Campanha C6')`);
    await fs.writeFile(path.join(root,'out','layout-editor-smoke.png'),(await window.webContents.capturePage()).toPNG());
    await click('Salvar novo layout');
    await waitFor(`!document.querySelector('.layout-editor') && window.electronAPI.inspect().formats.length===2`);
    const custom=await evaluate(`window.electronAPI.inspect().formats.find(item=>item.custom)`);
    assert.deepEqual(custom.colunas.map(column=>column.header),['CNPJ','Celular principal','NOME','Campanha']);
    assert.equal(custom.colunas[1].campo,'telefone_1');
    assert.equal(custom.colunas[3].valor_manual,'Campanha C6');
    assert.equal(await evaluate(`Array.from(document.querySelectorAll('.flow-field')).find(item=>item.firstElementChild.textContent==='Layout').querySelector('select').value`),custom.id);
    await click('Editar layout');
    await input('Nome da coluna 1','Rascunho');
    await click('Fechar editor');
    await waitFor(`document.querySelector('.layout-discard')!==null`);
    await click('Descartar e fechar');
    await waitFor(`!document.querySelector('.layout-editor')`);
    assert.equal((await evaluate(`window.electronAPI.inspect().formats.find(item=>item.custom)`)).colunas[0].header,'CNPJ');
    await click('Salvar fluxo');
    await waitFor(`window.electronAPI.inspect().flows[0].output.formatId===${JSON.stringify(custom.id)} && document.querySelector('.flows-app').getAttribute('aria-busy')==='false'`);
    await click('Gerar lista');
    await waitFor(`document.querySelector('.flow-job-detail') !== null`);
    assert.equal(await evaluate(`document.querySelector('.flow-job-counts strong').textContent`), '0');
    assert.equal(await evaluate(`document.querySelectorAll('.flow-progress-stages li').length`), 5);
    await evaluate(`window.electronAPI.emit({...window.electronAPI.inspect().jobs[0],progress:{stage:'generation',processed:25,total:100}})`);
    await waitFor(`document.querySelector('.flow-progress progress').value === 25`);
    assert.equal(await evaluate(`document.querySelectorAll('.flow-logs img').length`), 0);
    assert.equal(await evaluate(`document.querySelectorAll('.flow-job-counts strong')[1].textContent`), '—');
    await click('Cancelar execução');
    await waitFor(`Array.from(document.querySelectorAll('button')).some(item=>item.textContent==='Retomar execução')`);
    await click('Retomar execução');
    await waitFor(`window.electronAPI.inspect().calls.some(item=>item[0]==='resume')`);
    await evaluate(`window.electronAPI.emit({...window.electronAPI.inspect().jobs[0],status:'completed',stage:'output',counts:{generated:0,exported:0},outputs:[{path:'C:/synthetic/output/result.xlsx',kind:'xlsx',rows:0}],logs:Array.from({length:500},(_,n)=>'Log '+n)})`);
    await waitFor(`document.querySelector('.flow-outputs') !== null`);
    assert.equal(await evaluate(`document.querySelector('.flow-logs').textContent.split('\\n').length`), 300);
    await click('Abrir arquivo');
    await waitFor(`window.electronAPI.inspect().calls.some(item=>item[0]==='open')`);
    await evaluate(`window.electronAPI.emit({...window.electronAPI.inspect().jobs[0],id:'foreign-job',owner:'Alguém',flowName:'NÃO EXPOR'})`);
    await delay(40);
    assert.equal(await evaluate(`document.querySelector('.flow-history').textContent.includes('NÃO EXPOR')`), false);
    await click('Fluxos');
    await evaluate(`document.querySelector('.flow-access').open=true`);
    await input('Conexão PostgreSQL da Receita', 'postgresql://fake:synthetic@localhost/fixture');
    await click('Configurar Receita');
    await waitFor(`window.electronAPI.inspect().calls.some(item=>item[0]==='receita')`);
    assert.equal(await evaluate(`document.querySelector('.flow-access input[type=password]').value`), '');
    await click('Importar chave BQ');
    await waitFor(`window.electronAPI.inspect().calls.some(item=>item[0]==='bq')`);
    await click('Testar BQ');
    await waitFor(`window.electronAPI.inspect().calls.some(item=>item[0]==='bq-test')`);
    await click('Renovar login Google');
    await waitFor(`window.electronAPI.inspect().calls.some(item=>item[0]==='bq-login') && document.querySelector('.flows-app').getAttribute('aria-busy')==='false'`);
    await checkbox('Abrir login Google automaticamente quando expirar');
    await waitFor(`window.electronAPI.inspect().calls.some(item=>item[0]==='bq-auto-login' && item[1]===false) && document.querySelector('.flows-app').getAttribute('aria-busy')==='false'`);
    await evaluate(`window.electronAPI.emitAuth({owner:'Outro',state:'renewing',message:'Entre na conta no navegador.'})`);
    await waitFor(`document.querySelector('.flows-app').textContent.includes('Entre na conta no navegador.')`);
    assert.equal(await evaluate(`Array.from(document.querySelectorAll('.flow-access button')).find(item=>item.textContent==='Aguardando login…').disabled`), true);
    await evaluate(`window.electronAPI.emitAuth({owner:'Alguém',state:'ready',message:'NÃO EXPOR AUTH'})`);
    await delay(30);
    assert.equal(await evaluate(`document.querySelector('.flows-app').textContent.includes('NÃO EXPOR AUTH')`), false);
    await evaluate(`window.electronAPI.emitAuth({owner:'Outro',state:'ready',message:'Login validado.'})`);
    await click('Excluir');
    await click('Excluir fluxo');
    await waitFor(`window.electronAPI.inspect().flows.length===1`);
    assert.equal(await evaluate(`typeof window.require`), 'undefined');
    window.setSize(760, 900);
    await delay(50);
    assert.equal(await evaluate(`document.querySelector('.flow-editor').scrollWidth<=document.querySelector('.flow-editor').clientWidth+1`), true);
    await input('CNPJ','12ABC34501DE35'); await click('Consultar situação');
    await waitFor(`document.querySelector('.receita-situacao-result')?.textContent.includes('04 · Inapta')`);
    await click('Selecionar lista'); await click('Consultar lista');
    await waitFor(`window.electronAPI.inspect().calls.some(item=>item[0]==='situacao-start')`);
    await evaluate(`window.electronAPI.emitReceita({id:'situacao-job',owner:'Outro',name:'Lista própria.xlsx',status:'completed',message:'Consulta concluída.',output:'fixture.xlsx',counts:{processed:3,found:1,notFound:1,invalid:1}})`);
    await waitFor(`document.querySelector('.receita-situacao-app .flow-job-counts')?.textContent.includes('Não encontrados')`);
    await click('Abrir resultado');
    await waitFor(`window.electronAPI.inspect().calls.some(item=>item[0]==='situacao-open')`);
    assert.deepEqual(errors, []);
    console.log('Flow UI smoke passed: CRUD, multiple filters/situations, dirty preservation, policy, history, logs, ownership, actions, masked access and compact layout.');
}).catch(error => { console.error(error); exitCode = 1; }).finally(async () => {
    clearTimeout(timeout);
    if (window && !window.isDestroyed()) window.destroy();
    await fs.rm(taskDir, { recursive: true, force: true }).catch(() => {});
    app.exit(exitCode);
});
